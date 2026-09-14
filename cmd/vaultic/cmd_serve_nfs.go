//go:build darwin || freebsd || linux || windows

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/nfs"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
)

const (
	defaultNFSTreeCacheSize = "16M"
	defaultNFSBlobCacheSize = "64M"
	defaultNFSMaxReadSize   = "1M"
	nfsReadinessSchema      = "vaultic.nfs.readiness"
	nfsReadinessVersion     = 1
)

type serveNFSOptions struct {
	data.SnapshotFilter
	Listen                   string
	NFSPort                  int
	MountPort                int
	ExportName               string
	AllowCIDRs               []string
	AcknowledgeInsecureNFSv3 bool
	Owner                    string
	Permissions              string
	TreeCacheSize            string
	BlobCacheSize            string
	MaxReadSize              string
	IdleTimeout              time.Duration
	GracefulDrainTimeout     time.Duration
	MaxConnections           int
	MaxRequestsPerConnection int
	MaxRequestSize           string
	ConnectionIdleTimeout    time.Duration
	HandleLimit              int
	ReadinessFile            string

	ephemeralPorts bool
	now            func() time.Time
}

type nfsReadinessRecord struct {
	Schema       string    `json:"schema"`
	Version      int       `json:"version"`
	PID          int       `json:"pid"`
	RepositoryID string    `json:"repository_id"`
	SnapshotID   string    `json:"snapshot_id"`
	Subfolder    string    `json:"subfolder"`
	NFSAddress   string    `json:"nfs_address"`
	MountAddress string    `json:"mount_address"`
	ExportName   string    `json:"export_name"`
	StartedAt    time.Time `json:"started_at"`
}

func registerServeCommand(root *cobra.Command, globalOptions *global.Options) {
	serve := &cobra.Command{
		Use:               "serve",
		Short:             "Serve repository data over a network protocol",
		GroupID:           cmdGroupDefault,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
	}
	serve.AddCommand(newServeNFSCommand(globalOptions))
	root.AddCommand(serve)
}

func newServeNFSCommand(globalOptions *global.Options) *cobra.Command {
	options := serveNFSOptions{now: time.Now}
	command := &cobra.Command{
		Use:               "nfs [flags] SNAPSHOT",
		Short:             "Serve one snapshot read-only over NFSv3",
		Args:              cobra.ExactArgs(1),
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			finalizeSnapshotFilter(&options.SnapshotFilter)
			return nil
		},
		RunE: func(command *cobra.Command, args []string) error {
			return runServeNFS(command.Context(), options, *globalOptions, args, globalOptions.Term)
		},
	}
	options.addFlags(command.Flags())
	return command
}

func (options *serveNFSOptions) addFlags(flags *pflag.FlagSet) {
	initSingleSnapshotFilter(flags, &options.SnapshotFilter)
	flags.StringVar(&options.Listen, "listen", "127.0.0.1", "bind NFS services to `address`")
	flags.IntVar(&options.NFSPort, "nfs-port", nfs.DefaultNFSListenPort, "listen on `port` for NFSv3")
	flags.IntVar(&options.MountPort, "mount-port", nfs.DefaultMountListenPort, "listen on `port` for the mount protocol")
	flags.StringVar(&options.ExportName, "export-name", "/snapshot", "publish the snapshot under `name`")
	flags.StringArrayVar(&options.AllowCIDRs, "allow-cidr", nil, "allow clients from `CIDR` (can be specified multiple times)")
	flags.BoolVar(&options.AcknowledgeInsecureNFSv3, "acknowledge-insecure-nfsv3", false, "acknowledge that remote NFSv3 has no encryption or strong authentication")
	flags.StringVar(&options.Owner, "owner", "preserved", "report ownership as preserved, server, or root")
	flags.StringVar(&options.Permissions, "permissions", "preserved", "report permissions as preserved or readable")
	flags.StringVar(&options.TreeCacheSize, "tree-cache-size", defaultNFSTreeCacheSize, "process-local decoded-tree cache `size`")
	flags.StringVar(&options.BlobCacheSize, "blob-cache-size", defaultNFSBlobCacheSize, "process-local plaintext blob cache `size`")
	flags.StringVar(&options.MaxReadSize, "max-read-size", defaultNFSMaxReadSize, "maximum NFS READ response `size`")
	flags.DurationVar(&options.IdleTimeout, "idle-timeout", 0, "stop after no mounts or RPC activity for `duration`")
	flags.DurationVar(&options.GracefulDrainTimeout, "graceful-drain-timeout", 5*time.Second, "wait up to `duration` for requests to drain")
	flags.IntVar(&options.MaxConnections, "max-connections", nfs.DefaultMaxConnections, "maximum simultaneous client connections")
	flags.IntVar(&options.MaxRequestsPerConnection, "max-requests-per-connection", nfs.DefaultMaxRequests, "maximum requests served per connection")
	flags.StringVar(&options.MaxRequestSize, "max-request-size", "20M", "maximum RPC record `size`")
	flags.DurationVar(&options.ConnectionIdleTimeout, "connection-idle-timeout", 2*time.Minute, "close inactive connections after `duration`")
	flags.IntVar(&options.HandleLimit, "handle-limit", nfs.DefaultHandleLimit, "maximum reconstructed file handles")
	flags.StringVar(&options.ReadinessFile, "readiness-file", "", "atomically write readiness JSON to `path`")
}

func parseNFSSize(name, raw string) (int, error) {
	value, err := ui.ParseBytes(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s %q: %w", name, raw, err)
	}
	if value <= 0 || uint64(value) > uint64(math.MaxInt) {
		return 0, fmt.Errorf("--%s must fit in a positive int", name)
	}
	return int(value), nil
}

func nfsSnapshotConfig(options serveNFSOptions) (snapshotfs.Config, error) {
	treeBytes, err := parseNFSSize("tree-cache-size", options.TreeCacheSize)
	if err != nil {
		return snapshotfs.Config{}, err
	}
	blobBytes, err := parseNFSSize("blob-cache-size", options.BlobCacheSize)
	if err != nil {
		return snapshotfs.Config{}, err
	}
	serverUID, serverGID := nfsServerIdentity()
	config := snapshotfs.Config{TreeCacheBytes: treeBytes, BlobCacheBytes: blobBytes, ServerUID: serverUID, ServerGID: serverGID}
	switch options.Owner {
	case "preserved":
		config.Owner = snapshotfs.OwnerPreserved
	case "server":
		config.Owner = snapshotfs.OwnerServer
	case "root":
		config.Owner = snapshotfs.OwnerRoot
	default:
		return snapshotfs.Config{}, fmt.Errorf("invalid --owner %q: expected preserved, server, or root", options.Owner)
	}
	switch options.Permissions {
	case "preserved":
		config.Permissions = snapshotfs.PermissionsPreserved
	case "readable":
		config.Permissions = snapshotfs.PermissionsReadable
	default:
		return snapshotfs.Config{}, fmt.Errorf("invalid --permissions %q: expected preserved or readable", options.Permissions)
	}
	return config, nil
}

func nfsServerConfig(options serveNFSOptions) (nfs.Config, error) {
	maxReadSize, err := parseNFSSize("max-read-size", options.MaxReadSize)
	if err != nil {
		return nfs.Config{}, err
	}
	maxRequestSize, err := parseNFSSize("max-request-size", options.MaxRequestSize)
	if err != nil {
		return nfs.Config{}, err
	}
	config := nfs.Config{
		Listen: options.Listen, NFSPort: options.NFSPort, MountPort: options.MountPort,
		EphemeralPorts: options.ephemeralPorts, ExportName: options.ExportName,
		AllowCIDRs: options.AllowCIDRs, AcknowledgeInsecure: options.AcknowledgeInsecureNFSv3,
		MaxReadSize: maxReadSize, IdleTimeout: options.IdleTimeout,
		ConnectionIdleTimeout: options.ConnectionIdleTimeout, DrainTimeout: options.GracefulDrainTimeout,
		MaxConnections: options.MaxConnections, MaxRequests: options.MaxRequestsPerConnection,
		MaxRequestSize: maxRequestSize, HandleLimit: options.HandleLimit,
	}
	if options.ephemeralPorts {
		config.NFSPort = 0
		config.MountPort = 0
	}
	return config, nil
}

func runServeNFS(ctx context.Context, options serveNFSOptions, globalOptions global.Options, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	filesystemConfig, err := nfsSnapshotConfig(options)
	if err != nil {
		return err
	}
	serverConfig, err := nfsServerConfig(options)
	if err != nil {
		return err
	}

	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()
	if err := repo.LoadIndex(ctx, printer); err != nil {
		return err
	}
	snapshot, subfolder, err := options.SnapshotFilter.FindLatest(ctx, repo, repo, args[0])
	if err != nil {
		return fmt.Errorf("failed to find snapshot: %w", err)
	}
	filesystem, err := snapshotfs.New(ctx, repo, snapshot, subfolder, filesystemConfig)
	if err != nil {
		return err
	}
	server, err := nfs.New(filesystem, serverConfig)
	if err != nil {
		_ = filesystem.Close() // Preserve the server construction error; plaintext cache cleanup is best effort.
		return err
	}

	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()
	addresses, err := waitForNFSServer(ctx, server, serveDone, 5*time.Second)
	if err != nil {
		_ = server.Close() // Preserve the readiness error; server cleanup is best effort.
		waitForNFSCleanup(server.CleanupDone(), serverConfig.DrainTimeout)
		return err
	}

	startedAt := time.Now().UTC()
	if options.now != nil {
		startedAt = options.now().UTC()
	}
	record := nfsReadinessRecord{
		Schema: nfsReadinessSchema, Version: nfsReadinessVersion, PID: os.Getpid(),
		RepositoryID: repo.Config().ID, SnapshotID: snapshot.ID().String(), Subfolder: subfolder,
		NFSAddress: addresses.NFS, MountAddress: addresses.Mount, ExportName: options.ExportName,
		StartedAt: startedAt,
	}
	if options.ReadinessFile != "" {
		if err := writeNFSReadinessFile(options.ReadinessFile, record); err != nil {
			_ = server.Close() // Preserve the readiness-file error; server cleanup is best effort.
			<-serveDone
			waitForNFSCleanup(server.CleanupDone(), serverConfig.DrainTimeout)
			return err
		}
		defer removeNFSReadinessFile(options.ReadinessFile, record)
	}

	cacheStatus := repo.ReadCacheStatus()
	profiles := "none"
	if len(globalOptions.UseProfiles) != 0 {
		profiles = strings.Join(globalOptions.UseProfiles, ",")
	}
	term.Print(fmt.Sprintf("Serving snapshot %s subfolder %q", snapshot.ID().String(), subfolder))
	term.Print(fmt.Sprintf("NFS address: %s", addresses.NFS))
	term.Print(fmt.Sprintf("Mount address: %s", addresses.Mount))
	term.Print(fmt.Sprintf("Shared persistent read-cache: enabled=%v profile=%s revision=%d limit=%d", cacheStatus.Enabled, profiles, cacheStatus.PolicyRevision, cacheStatus.AggregateMaxBytes))
	term.Print(fmt.Sprintf("Process-local caches: tree=%d blob=%d max-read=%d", filesystemConfig.TreeCacheBytes, filesystemConfig.BlobCacheBytes, serverConfig.MaxReadSize))
	if !net.ParseIP(options.Listen).IsLoopback() {
		term.Error("WARNING: NFSv3 traffic is unencrypted and AUTH_SYS identities are client-controlled; use only an authenticated private network.")
	}
	instructions := server.MountInstructions("MOUNTPOINT")
	term.Print("macOS: " + instructions.MacOS)
	term.Print("Linux: " + instructions.Linux)
	term.Print("BSD: " + instructions.BSD)

	err = <-serveDone
	waitForNFSCleanup(server.CleanupDone(), serverConfig.DrainTimeout)
	if ctx.Err() != nil && err == nil {
		return ErrOK
	}
	return err
}

func waitForNFSCleanup(done <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func waitForNFSServer(ctx context.Context, server *nfs.Server, serveDone <-chan error, timeout time.Duration) (nfs.Addresses, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if readiness := server.Readiness(); readiness.Ready {
			return readiness.Addresses, nil
		}
		select {
		case err := <-serveDone:
			if err == nil {
				return nfs.Addresses{}, errors.New("NFS server stopped before becoming ready")
			}
			return nfs.Addresses{}, err
		case <-ctx.Done():
			return nfs.Addresses{}, ctx.Err()
		case <-timer.C:
			return nfs.Addresses{}, errors.New("timed out waiting for NFS server readiness")
		case <-ticker.C:
		}
	}
}

func writeNFSReadinessFile(path string, record nfsReadinessRecord) error {
	directory := filepath.Dir(path)
	if info, err := os.Stat(directory); err != nil {
		return fmt.Errorf("readiness file parent: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("readiness file parent %q is not a directory", directory)
	}
	if existing, err := loadNFSReadinessFile(path); err == nil && validNFSReadinessRecord(existing) && processAlive(existing.PID) {
		return fmt.Errorf("readiness file %q belongs to the live serving process", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing readiness file: %w", err)
	}

	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(directory, ".vaultic-nfs-readiness-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close() // Preserve the permission error; temporary-file cleanup is best effort.
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close() // Preserve the write error; temporary-file cleanup is best effort.
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close() // Preserve the sync error; temporary-file cleanup is best effort.
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := replaceReadinessFile(temporaryPath, path); err != nil {
		return err
	}
	return os.Chmod(path, 0600)
}

func loadNFSReadinessFile(path string) (nfsReadinessRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nfsReadinessRecord{}, err
	}
	defer file.Close()
	var record nfsReadinessRecord
	if err := json.NewDecoder(file).Decode(&record); err != nil {
		return nfsReadinessRecord{}, err
	}
	return record, nil
}

func sameNFSProcess(left, right nfsReadinessRecord) bool {
	return left.Schema == nfsReadinessSchema && left.Version == nfsReadinessVersion &&
		left.PID == right.PID && left.StartedAt.Equal(right.StartedAt)
}

func validNFSReadinessRecord(record nfsReadinessRecord) bool {
	return record.Schema == nfsReadinessSchema && record.Version == nfsReadinessVersion &&
		record.PID > 0 && !record.StartedAt.IsZero()
}

func removeNFSReadinessFile(path string, owner nfsReadinessRecord) {
	existing, err := loadNFSReadinessFile(path)
	if err == nil && sameNFSProcess(existing, owner) {
		_ = os.Remove(path) // Shutdown must not fail because advisory readiness cleanup failed.
	}
}
