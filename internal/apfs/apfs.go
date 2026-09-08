package apfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var ErrUnsupported = errors.New("APFS snapshots are unsupported on this platform")

type ErrorKind string

const (
	ErrorNotAPFS       ErrorKind = "not-apfs"
	ErrorNotAuthorized ErrorKind = "not-authorized"
	ErrorNotAvailable  ErrorKind = "not-available"
	ErrorNameNotFound  ErrorKind = "snapshot-name-not-found"
	ErrorMount         ErrorKind = "mount"
)

type Error struct {
	Kind ErrorKind
	Op   string
	Err  error
}

func (err *Error) Error() string { return fmt.Sprintf("%s: %v", err.Op, err.Err) }
func (err *Error) Unwrap() error { return err.Err }

type Volume struct {
	Root       string
	UUID       string
	Device     uint64
	DeviceNode string
	MountPoint string
	Filesystem string
}

type Runner interface {
	Output(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}

func (execRunner) Output(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

type Manager struct {
	Runner  Runner
	TempDir string
}

type Mount struct {
	Name       string
	MountPoint string
	volume     Volume
	manager    Manager
	keep       bool
	leasePath  string
}

func NewManager() Manager { return Manager{Runner: execRunner{}} }

type lease struct {
	PID        int       `json:"pid"`
	Name       string    `json:"name"`
	MountPoint string    `json:"mount_point,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

func (manager Manager) SweepStale(ctx context.Context) error {
	directory := manager.leaseDirectory()
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read APFS snapshot leases: %w", err)
	}
	var result error
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		leasePath := filepath.Join(directory, entry.Name())
		payload, readErr := os.ReadFile(leasePath)
		if readErr != nil {
			result = errors.Join(result, readErr)
			continue
		}
		var record lease
		if json.Unmarshal(payload, &record) != nil || processAlive(record.PID) {
			continue
		}
		if _, ok := snapshotTimestamp(record.Name); !ok || !manager.ownsMountPoint(record.MountPoint) {
			continue
		}
		if record.MountPoint != "" {
			output, unmountErr := manager.Runner.Output(ctx, "umount", record.MountPoint)
			if unmountErr != nil {
				forced, forceErr := manager.Runner.Output(ctx, "diskutil", "unmount", "force", record.MountPoint)
				if forceErr != nil {
					result = errors.Join(result, classifyCommandError("unmount stale APFS snapshot", output, unmountErr), classifyCommandError("force-unmount stale APFS snapshot", forced, forceErr))
					continue
				}
			}
			if removeErr := os.Remove(record.MountPoint); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				result = errors.Join(result, removeErr)
				continue
			}
		}
		if deleteErr := manager.delete(ctx, record.Name); deleteErr != nil {
			result = errors.Join(result, deleteErr)
			continue
		}
		if removeErr := os.Remove(leasePath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			result = errors.Join(result, removeErr)
		}
	}
	return result
}

func (manager Manager) ResolveVolume(ctx context.Context, sourceRoot string) (Volume, error) {
	if manager.Runner == nil {
		manager.Runner = execRunner{}
	}
	return resolveVolume(ctx, manager.Runner, sourceRoot)
}

func (manager Manager) ResolveVolumes(ctx context.Context, sourceRoot string, oneFileSystem bool) ([]Volume, error) {
	if manager.Runner == nil {
		manager.Runner = execRunner{}
	}
	return resolveVolumes(ctx, manager.Runner, sourceRoot, oneFileSystem)
}

func (manager Manager) CreateAndMount(ctx context.Context, volume Volume, capturedAt time.Time, keep bool) (*Mount, error) {
	if !strings.EqualFold(volume.Filesystem, "apfs") {
		return nil, &Error{Kind: ErrorNotAPFS, Op: "create APFS snapshot", Err: fmt.Errorf("filesystem is %q", volume.Filesystem)}
	}
	if volume.MountPoint != "/" && volume.MountPoint != "/System/Volumes/Data" {
		return nil, &Error{Kind: ErrorNotAvailable, Op: "create APFS snapshot", Err: fmt.Errorf("tmutil cannot create snapshots for volume mounted at %q", volume.MountPoint)}
	}
	if manager.Runner == nil {
		manager.Runner = execRunner{}
	}
	output, err := manager.Runner.Output(ctx, "tmutil", "localsnapshot")
	if err != nil {
		return nil, classifyCommandError("create APFS snapshot", output, err)
	}
	name, err := snapshotNameFromCreateOutput(string(output))
	if err != nil {
		return nil, &Error{Kind: ErrorNameNotFound, Op: "parse APFS snapshot name", Err: err}
	}
	leasePath, err := manager.writeLease(lease{PID: os.Getpid(), Name: name, CreatedAt: time.Now().UTC()})
	if err != nil {
		return nil, &Error{Kind: ErrorMount, Op: "record APFS snapshot lease", Err: errors.Join(err, manager.rollbackCreate(ctx, name, "", ""))}
	}
	listed, listErr := manager.Runner.Output(ctx, "tmutil", "listlocalsnapshots", volume.MountPoint)
	if listErr != nil {
		return nil, errors.Join(classifyCommandError("list APFS snapshots", listed, listErr), manager.rollbackCreate(ctx, name, leasePath, ""))
	}
	if !snapshotListed(string(listed), name) {
		return nil, &Error{Kind: ErrorNameNotFound, Op: "verify APFS snapshot", Err: errors.Join(fmt.Errorf("snapshot %q was not listed", name), manager.rollbackCreate(ctx, name, leasePath, ""))}
	}
	createdAt, err := snapshotTime(name, capturedAt.Location())
	if err != nil || createdAt.Before(capturedAt.Truncate(time.Second)) {
		return nil, &Error{Kind: ErrorNameNotFound, Op: "verify APFS snapshot time", Err: errors.Join(fmt.Errorf("snapshot %q predates anchor capture", name), manager.rollbackCreate(ctx, name, leasePath, ""))}
	}
	mountPoint, err := os.MkdirTemp(manager.TempDir, "vaultic-apfs-")
	if err != nil {
		return nil, &Error{Kind: ErrorMount, Op: "create APFS mount directory", Err: errors.Join(err, manager.rollbackCreate(ctx, name, leasePath, ""))}
	}
	if err := os.Chmod(mountPoint, 0o700); err != nil {
		return nil, &Error{Kind: ErrorMount, Op: "protect APFS mount directory", Err: errors.Join(err, manager.rollbackCreate(ctx, name, leasePath, mountPoint))}
	}
	if err := manager.updateLease(leasePath, lease{PID: os.Getpid(), Name: name, MountPoint: mountPoint, CreatedAt: time.Now().UTC()}); err != nil {
		return nil, &Error{Kind: ErrorMount, Op: "update APFS snapshot lease", Err: errors.Join(err, manager.rollbackCreate(ctx, name, leasePath, mountPoint))}
	}
	mountOutput, err := manager.Runner.Output(ctx, "mount_apfs", "-o", "rdonly,nobrowse", "-s", name, volume.DeviceNode, mountPoint)
	if err != nil {
		return nil, errors.Join(classifyCommandError("mount APFS snapshot", mountOutput, err), manager.rollbackCreate(ctx, name, leasePath, mountPoint))
	}
	return &Mount{Name: name, MountPoint: mountPoint, volume: volume, manager: manager, keep: keep, leasePath: leasePath}, nil
}

func (manager Manager) rollbackCreate(ctx context.Context, name, leasePath, mountPoint string) error {
	var result error
	if mountPoint != "" {
		if err := os.Remove(mountPoint); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	if leasePath != "" {
		if err := os.Remove(leasePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return errors.Join(result, manager.delete(ctx, name))
}

func (mount *Mount) Cleanup(ctx context.Context) error {
	if mount == nil {
		return nil
	}
	var result error
	if output, err := mount.manager.Runner.Output(ctx, "umount", mount.MountPoint); err != nil {
		if forced, forceErr := mount.manager.Runner.Output(ctx, "diskutil", "unmount", "force", mount.MountPoint); forceErr != nil {
			result = errors.Join(classifyCommandError("unmount APFS snapshot", output, err), classifyCommandError("force-unmount APFS snapshot", forced, forceErr))
		}
	}
	if err := os.Remove(mount.MountPoint); err != nil && !errors.Is(err, os.ErrNotExist) {
		result = errors.Join(result, err)
	}
	if !mount.keep {
		result = errors.Join(result, mount.manager.delete(ctx, mount.Name))
	}
	if result == nil || mount.keep {
		if err := os.Remove(mount.leasePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (manager Manager) leaseDirectory() string {
	base := manager.TempDir
	if base == "" {
		base = os.TempDir()
	}
	return filepath.Join(base, "vaultic-apfs-leases")
}

func (manager Manager) writeLease(record lease) (string, error) {
	directory := manager.leaseDirectory()
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", err
	}
	file, err := os.CreateTemp(directory, "lease-*.json")
	if err != nil {
		return "", err
	}
	name := file.Name()
	if err := file.Chmod(0o600); err != nil {
		return "", errors.Join(err, file.Close(), removeFile(name))
	}
	if err := json.NewEncoder(file).Encode(record); err != nil {
		return "", errors.Join(err, file.Close(), removeFile(name))
	}
	if err := file.Close(); err != nil {
		return "", errors.Join(err, removeFile(name))
	}
	return name, nil
}

func removeFile(name string) error {
	err := os.Remove(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (manager Manager) updateLease(name string, record lease) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return os.WriteFile(name, payload, 0o600)
}

func (manager Manager) ownsMountPoint(name string) bool {
	if name == "" {
		return true
	}
	base := manager.TempDir
	if base == "" {
		base = os.TempDir()
	}
	relative, err := filepath.Rel(filepath.Clean(base), filepath.Clean(name))
	return err == nil && !strings.Contains(relative, string(filepath.Separator)) && strings.HasPrefix(relative, "vaultic-apfs-")
}

func (manager Manager) delete(ctx context.Context, name string) error {
	timestamp, ok := snapshotTimestamp(name)
	if !ok {
		return &Error{Kind: ErrorNameNotFound, Op: "delete APFS snapshot", Err: fmt.Errorf("refusing unrecognized snapshot name %q", name)}
	}
	output, err := manager.Runner.Output(ctx, "tmutil", "deletelocalsnapshots", timestamp)
	if err != nil {
		return classifyCommandError("delete APFS snapshot", output, err)
	}
	return nil
}

func snapshotNameFromCreateOutput(output string) (string, error) {
	const marker = "Created local snapshot with date:"
	for _, line := range strings.Split(output, "\n") {
		if index := strings.Index(line, marker); index >= 0 {
			timestamp := strings.TrimSpace(line[index+len(marker):])
			name := "com.apple.TimeMachine." + timestamp + ".local"
			if _, ok := snapshotTimestamp(name); ok {
				return name, nil
			}
		}
	}
	return "", fmt.Errorf("tmutil did not report a snapshot date")
}

func snapshotTimestamp(name string) (string, bool) {
	const prefix, suffix = "com.apple.TimeMachine.", ".local"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return "", false
	}
	value := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if _, err := time.Parse("2006-01-02-150405", value); err != nil {
		return "", false
	}
	return value, true
}

func snapshotListed(output, name string) bool {
	for _, line := range strings.Fields(output) {
		if line == name {
			return true
		}
	}
	return false
}

func snapshotTime(name string, location *time.Location) (time.Time, error) {
	timestamp, ok := snapshotTimestamp(name)
	if !ok {
		return time.Time{}, fmt.Errorf("invalid snapshot name")
	}
	return time.ParseInLocation("2006-01-02-150405", timestamp, location)
}

func classifyCommandError(operation string, output []byte, err error) error {
	message := strings.ToLower(string(output) + " " + err.Error())
	kind := ErrorMount
	switch {
	case errors.Is(err, exec.ErrNotFound), strings.Contains(message, "executable file not found"), strings.Contains(message, "no such file"):
		kind = ErrorNotAvailable
	case strings.Contains(message, "not authorized"), strings.Contains(message, "operation not permitted"), strings.Contains(message, "permission denied"):
		kind = ErrorNotAuthorized
	case strings.Contains(message, "not apfs"):
		kind = ErrorNotAPFS
	}
	return &Error{Kind: kind, Op: operation, Err: fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))}
}

func RemapRoot(liveRoot, snapshotMount, path string) (string, error) {
	relative, err := filepath.Rel(filepath.Clean(liveRoot), filepath.Clean(path))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q is outside source root %q", path, liveRoot)
	}
	return filepath.Join(snapshotMount, relative), nil
}

func SnapshotSourcePath(volume Volume, liveRoot, snapshotMount string) (string, error) {
	relative, err := VolumeRelativePath(volume, liveRoot)
	if err != nil {
		return "", err
	}
	return filepath.Join(snapshotMount, relative), nil
}

func VolumeRelativePath(volume Volume, liveRoot string) (string, error) {
	realRoot := filepath.Clean(liveRoot)
	mountPoint := filepath.Clean(volume.MountPoint)
	if mountPoint == "/System/Volumes/Data" && !pathWithinRoot(mountPoint, realRoot) {
		return strings.TrimPrefix(realRoot, string(filepath.Separator)), nil
	}
	relative, err := filepath.Rel(mountPoint, realRoot)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("source root %q is outside volume mount point %q", liveRoot, volume.MountPoint)
	}
	return relative, nil
}

func pathWithinRoot(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
