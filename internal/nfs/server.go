package nfs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gonfs "github.com/willscott/go-nfs"

	"github.com/otuschhoff/vaultic/internal/snapshotfs"
)

type Addresses struct {
	NFS   string
	Mount string
}

type MountInstructions struct {
	MacOS string
	Linux string
	BSD   string
}

type Readiness struct {
	Ready     bool
	Addresses Addresses
}

type Stats struct {
	ActiveMounts      int
	ActiveConnections int64
	DeniedClients     uint64
	Procedures        ProcedureCounts
}

type ProcedureCounts struct {
	NFS   [22]uint64
	Mount [6]uint64
}

type Server struct {
	cfg        validatedConfig
	filesystem *Filesystem
	handler    *handler

	mu          sync.Mutex
	listeners   []net.Listener
	addresses   Addresses
	ready       bool
	closed      bool
	serveDone   chan struct{}
	serveErr    error
	serveWG     sync.WaitGroup
	serveCancel context.CancelFunc
	rpcServers  []*gonfs.Server
	closeOnce   sync.Once
	closeDone   chan struct{}
	cleanupDone chan struct{}
	closeErr    error

	connections       sync.Map
	activeConnections atomic.Int64
	deniedClients     atomic.Uint64
	lastActivity      atomic.Int64
	nfsProcedures     [22]atomic.Uint64
	mountProcedures   [6]atomic.Uint64
}

func New(fs *snapshotfs.Filesystem, cfg Config) (*Server, error) {
	validated, err := validateConfig(cfg)
	if err != nil {
		return nil, err
	}
	filesystem, err := NewFilesystem(fs, validated.MaxReadSize)
	if err != nil {
		return nil, err
	}
	handler, err := newHandler(filesystem, validated.ExportName, validated.HandleLimit)
	if err != nil {
		return nil, err
	}
	server := &Server{cfg: validated, filesystem: filesystem, handler: handler, closeDone: make(chan struct{}), cleanupDone: make(chan struct{})}
	server.markActivity()
	return server, nil
}

func (server *Server) Serve(ctx context.Context) error {
	server.mu.Lock()
	if server.closed || server.serveDone != nil {
		server.mu.Unlock()
		return errors.New("NFS server is closed or already served")
	}
	server.serveDone = make(chan struct{})
	server.mu.Unlock()

	nfsListener, err := net.Listen("tcp", net.JoinHostPort(server.cfg.Listen, strconv.Itoa(server.cfg.NFSPort)))
	if err != nil {
		_ = server.Close() // Preserve the bind error; server cleanup is best effort.
		server.finishServe(err)
		return err
	}
	mountListener, err := net.Listen("tcp", net.JoinHostPort(server.cfg.Listen, strconv.Itoa(server.cfg.MountPort)))
	if err != nil {
		_ = nfsListener.Close() // Preserve the mount-port bind error; partial listener cleanup is best effort.
		_ = server.Close()      // Preserve the bind error; server cleanup is best effort.
		server.finishServe(err)
		return err
	}

	semaphore := make(chan struct{}, server.cfg.MaxConnections)
	listeners := []net.Listener{
		&guardedListener{Listener: nfsListener, server: server, sem: semaphore},
		&guardedListener{Listener: mountListener, server: server, sem: semaphore},
	}
	server.mu.Lock()
	if server.closed {
		server.mu.Unlock()
		for _, listener := range listeners {
			_ = listener.Close() // Preserve the concurrent-close result; listener cleanup is best effort.
		}
		_ = server.Close() // The server is already closing; repeated cleanup is best effort.
		server.finishServe(net.ErrClosed)
		return net.ErrClosed
	}
	server.serveWG.Add(len(listeners))
	server.listeners = listeners
	server.addresses = Addresses{NFS: nfsListener.Addr().String(), Mount: mountListener.Addr().String()}
	server.ready = true
	server.mu.Unlock()

	serveContext, cancel := context.WithCancel(ctx)
	server.mu.Lock()
	server.serveCancel = cancel
	server.mu.Unlock()
	defer cancel()
	errorsChannel := make(chan error, len(listeners))
	programs := []uint32{gonfs.NFSProgram, gonfs.MountProgram}
	rpcServers := make([]*gonfs.Server, len(listeners))
	for index, program := range programs {
		rpcServers[index] = &gonfs.Server{
			Handler:           server.handler,
			Context:           serveContext,
			AllowedPrograms:   map[uint32]bool{program: true},
			MaxRead:           uint32(server.cfg.MaxReadSize),
			MaxRecordFragment: uint32(server.cfg.MaxRequestSize),
			ObserveProcedure:  server.observeProcedure,
		}
	}
	server.mu.Lock()
	server.rpcServers = rpcServers
	server.mu.Unlock()
	for index, listener := range listeners {
		rpcServer := rpcServers[index]
		go func() {
			defer server.serveWG.Done()
			err := rpcServer.Serve(listener)
			errorsChannel <- err
		}()
	}

	var serveErr error
	if server.cfg.IdleTimeout == 0 {
		select {
		case <-ctx.Done():
			serveErr = ctx.Err()
		case serveErr = <-errorsChannel:
		}
	} else {
		timer := time.NewTimer(server.cfg.IdleTimeout)
		defer timer.Stop()
		for serveErr == nil {
			if server.handler.activeMounts() != 0 {
				server.markActivity()
			}
			remaining := server.cfg.IdleTimeout - time.Since(time.Unix(0, server.lastActivity.Load()))
			if remaining <= 0 {
				serveErr = context.DeadlineExceeded
				break
			}
			timer.Reset(remaining)
			select {
			case <-ctx.Done():
				serveErr = ctx.Err()
			case serveErr = <-errorsChannel:
			case <-timer.C:
			}
		}
	}
	_ = server.Close() // Preserve the serve result; shutdown cleanup is best effort.
	server.finishServe(serveErr)
	if errors.Is(serveErr, context.Canceled) || errors.Is(serveErr, context.DeadlineExceeded) || errors.Is(serveErr, net.ErrClosed) {
		return nil
	}
	return serveErr
}

func (server *Server) Close() error {
	server.closeOnce.Do(func() {
		server.mu.Lock()
		server.closed = true
		server.ready = false
		listeners := append([]net.Listener(nil), server.listeners...)
		cancel := server.serveCancel
		rpcServers := append([]*gonfs.Server(nil), server.rpcServers...)
		server.mu.Unlock()
		server.filesystem.cancel()
		if cancel != nil {
			cancel()
		}
		for _, listener := range listeners {
			_ = listener.Close() // Shutdown proceeds across both listeners despite one close failure.
		}
		server.connections.Range(func(key, _ any) bool {
			_ = key.(net.Conn).Close() // Drain all tracked connections; individual close failures are non-fatal.
			return true
		})
		drained := make(chan struct{})
		go func() {
			server.serveWG.Wait()
			for server.activeConnections.Load() != 0 || activeRPCRequests(rpcServers) != 0 {
				time.Sleep(time.Millisecond)
			}
			close(drained)
		}()
		finishCleanup := func() {
			server.closeErr = server.filesystem.fs.Close()
			close(server.cleanupDone)
		}
		select {
		case <-drained:
			finishCleanup()
		case <-time.After(server.cfg.DrainTimeout):
			server.closeErr = fmt.Errorf("NFS requests did not drain within %s", server.cfg.DrainTimeout)
			go func() {
				<-drained
				_ = server.filesystem.fs.Close() // Late cleanup owns process-local cache release after bounded Close returns.
				close(server.cleanupDone)
			}()
		}
		server.handler.close()
		close(server.closeDone)
	})
	<-server.closeDone
	return server.closeErr
}

// CleanupDone closes after snapshotfs and its process-local plaintext caches are released.
func (server *Server) CleanupDone() <-chan struct{} {
	return server.cleanupDone
}

func activeRPCRequests(servers []*gonfs.Server) int64 {
	var active int64
	for _, server := range servers {
		active += server.ActiveRequests()
	}
	return active
}

func (server *Server) Addresses() Addresses {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.addresses
}

func (server *Server) Readiness() Readiness {
	server.mu.Lock()
	defer server.mu.Unlock()
	return Readiness{Ready: server.ready, Addresses: server.addresses}
}

func (server *Server) MountInstructions(mountpoint string) MountInstructions {
	addresses := server.Addresses()
	host, nfsPort, _ := net.SplitHostPort(addresses.NFS)
	_, mountPort, _ := net.SplitHostPort(addresses.Mount)
	remoteHost := host
	if strings.Contains(host, ":") {
		remoteHost = "[" + host + "]"
	}
	remote := remoteHost + ":" + server.cfg.ExportName
	options := fmt.Sprintf("vers=3,proto=tcp,port=%s,mountport=%s,ro", nfsPort, mountPort)
	return MountInstructions{
		MacOS: fmt.Sprintf("mount_nfs -o %s %s %s", options, remote, mountpoint),
		Linux: fmt.Sprintf("mount -t nfs -o %s %s %s", options, remote, mountpoint),
		BSD:   fmt.Sprintf("mount_nfs -o %s %s %s", options, remote, mountpoint),
	}
}

func (server *Server) Stats() Stats {
	stats := Stats{
		ActiveMounts:      server.handler.activeMounts(),
		ActiveConnections: server.activeConnections.Load(),
		DeniedClients:     server.deniedClients.Load(),
	}
	for index := range stats.Procedures.NFS {
		stats.Procedures.NFS[index] = server.nfsProcedures[index].Load()
	}
	for index := range stats.Procedures.Mount {
		stats.Procedures.Mount[index] = server.mountProcedures[index].Load()
	}
	return stats
}

func (server *Server) markActivity() {
	server.lastActivity.Store(time.Now().UnixNano())
}

func (server *Server) observeProcedure(program, procedure uint32) {
	server.markActivity()
	if program == gonfs.NFSProgram && procedure < uint32(len(server.nfsProcedures)) {
		server.nfsProcedures[procedure].Add(1)
	}
	if program == gonfs.MountProgram && procedure < uint32(len(server.mountProcedures)) {
		server.mountProcedures[procedure].Add(1)
	}
}

func (server *Server) allowed(address net.Addr) bool {
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if len(server.cfg.allowNets) == 0 {
		return ip.IsLoopback()
	}
	for _, network := range server.cfg.allowNets {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func (server *Server) finishServe(err error) {
	server.mu.Lock()
	server.serveErr = err
	if server.serveDone != nil {
		select {
		case <-server.serveDone:
		default:
			close(server.serveDone)
		}
	}
	server.mu.Unlock()
}
