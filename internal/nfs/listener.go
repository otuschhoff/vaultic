package nfs

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	gonfs "github.com/willscott/go-nfs"
)

type guardedListener struct {
	net.Listener
	server *Server
	sem    chan struct{}
}

func (listener *guardedListener) Accept() (net.Conn, error) {
	for {
		conn, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if !listener.server.allowed(conn.RemoteAddr()) {
			listener.server.deniedClients.Add(1)
			_ = conn.Close() // The denied connection is discarded; close failure cannot change admission.
			continue
		}
		select {
		case listener.sem <- struct{}{}:
		default:
			listener.server.deniedClients.Add(1)
			_ = conn.Close() // The over-limit connection is discarded; close failure cannot change admission.
			continue
		}
		wrapped := &guardedConn{
			Conn:        conn,
			server:      listener.server,
			sem:         listener.sem,
			idleTimeout: listener.server.cfg.ConnectionIdleTimeout,
			maxRequests: listener.server.cfg.MaxRequests,
			maxFrame:    uint32(listener.server.cfg.MaxRequestSize),
		}
		listener.server.connections.Store(wrapped, struct{}{})
		listener.server.activeConnections.Add(1)
		return wrapped, nil
	}
}

type guardedConn struct {
	net.Conn
	server        *Server
	sem           chan struct{}
	idleTimeout   time.Duration
	maxRequests   int
	maxFrame      uint32
	closeOnce     sync.Once
	marker        [4]byte
	markerAt      int
	markerReady   bool
	remaining     uint32
	recordSize    uint32
	fragmentCount int
	inRecord      bool
	lastFragment  bool
	requests      int
}

func (conn *guardedConn) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	_ = conn.Conn.SetReadDeadline(time.Now().Add(conn.idleTimeout)) // The subsequent read reports transport failure.
	if !conn.markerReady && conn.remaining == 0 {
		if _, err := io.ReadFull(conn.Conn, conn.marker[:]); err != nil {
			return 0, err
		}
		fragment := binary.BigEndian.Uint32(conn.marker[:])
		if !conn.inRecord {
			conn.requests++
			if conn.requests > conn.maxRequests {
				return 0, io.ErrUnexpectedEOF
			}
			conn.inRecord = true
			conn.recordSize = 0
			conn.fragmentCount = 0
		}
		conn.fragmentCount++
		if conn.fragmentCount > gonfs.MaxRecordFragments {
			return 0, io.ErrUnexpectedEOF
		}
		conn.remaining = fragment &^ (1 << 31)
		if conn.remaining > conn.maxFrame-conn.recordSize {
			return 0, io.ErrUnexpectedEOF
		}
		conn.recordSize += conn.remaining
		conn.lastFragment = fragment&(1<<31) != 0
		conn.markerReady = true
		conn.markerAt = 0
	}
	if conn.markerReady {
		n := copy(dst, conn.marker[conn.markerAt:])
		conn.markerAt += n
		if conn.markerAt == len(conn.marker) {
			conn.markerReady = false
			conn.markerAt = 0
			if conn.remaining == 0 && conn.lastFragment {
				conn.inRecord = false
			}
		}
		return n, nil
	}
	amount := min(uint32(len(dst)), conn.remaining)
	n, err := conn.Conn.Read(dst[:amount])
	conn.remaining -= uint32(n)
	if conn.remaining == 0 && conn.lastFragment {
		conn.inRecord = false
	}
	return n, err
}

func (conn *guardedConn) Write(data []byte) (int, error) {
	_ = conn.Conn.SetWriteDeadline(time.Now().Add(conn.idleTimeout)) // The subsequent write reports transport failure.
	return conn.Conn.Write(data)
}

func (conn *guardedConn) Close() error {
	var err error
	conn.closeOnce.Do(func() {
		err = conn.Conn.Close()
		<-conn.sem
		conn.server.connections.Delete(conn)
		conn.server.activeConnections.Add(-1)
	})
	return err
}

var _ net.Listener = (*guardedListener)(nil)
var _ net.Conn = (*guardedConn)(nil)
