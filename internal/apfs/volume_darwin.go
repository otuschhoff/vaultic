//go:build darwin

// Package apfs provides consistent macOS backup sources using APFS snapshots.
package apfs

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func resolveVolume(ctx context.Context, runner Runner, sourceRoot string) (Volume, error) {
	absolute, err := filepath.Abs(sourceRoot)
	if err != nil {
		return Volume{}, fmt.Errorf("resolve source path: %w", err)
	}
	realRoot, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return Volume{}, fmt.Errorf("resolve source links: %w", err)
	}
	info, err := os.Stat(realRoot)
	if err != nil {
		return Volume{}, fmt.Errorf("stat source volume: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return Volume{}, fmt.Errorf("source stat has no Darwin device identity")
	}
	var filesystem unix.Statfs_t
	if err := unix.Statfs(realRoot, &filesystem); err != nil {
		return Volume{}, fmt.Errorf("resolve source mount point: %w", err)
	}
	mountPoint := nullTerminatedString(filesystem.Mntonname[:])
	output, err := runner.Output(ctx, "diskutil", "info", "-plist", mountPoint)
	if err != nil {
		return Volume{}, classifyCommandError("inspect APFS volume", output, err)
	}
	values, err := parseStringPlist(output)
	if err != nil {
		return Volume{}, &Error{Kind: ErrorNotAvailable, Op: "decode APFS volume information", Err: err}
	}
	volume := Volume{
		Root: realRoot, Device: uint64(stat.Dev), UUID: values["VolumeUUID"],
		DeviceNode: values["DeviceNode"], MountPoint: values["MountPoint"],
		Filesystem: values["FilesystemType"],
	}
	if volume.UUID == "" || volume.DeviceNode == "" || volume.MountPoint == "" || volume.Filesystem == "" {
		return Volume{}, &Error{Kind: ErrorNotAvailable, Op: "inspect APFS volume", Err: fmt.Errorf("diskutil omitted required volume identity fields")}
	}
	return volume, nil
}

func resolveVolumes(ctx context.Context, runner Runner, sourceRoot string, oneFileSystem bool) ([]Volume, error) {
	root, err := resolveVolume(ctx, runner, sourceRoot)
	if err != nil || oneFileSystem {
		return []Volume{root}, err
	}
	mountPoints, err := mountedPaths()
	if err != nil {
		return nil, err
	}
	return resolveContributingVolumes(ctx, runner, root, mountPoints)
}

func mountedPaths() ([]string, error) {
	count, err := syscall.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return nil, fmt.Errorf("count mounted filesystems: %w", err)
	}
	filesystems := make([]syscall.Statfs_t, count)
	count, err = syscall.Getfsstat(filesystems, unix.MNT_NOWAIT)
	if err != nil {
		return nil, fmt.Errorf("list mounted filesystems: %w", err)
	}
	mountPoints := make([]string, 0, count)
	for _, filesystem := range filesystems[:count] {
		mountPoints = append(mountPoints, nullTerminatedInt8String(filesystem.Mntonname[:]))
	}
	return mountPoints, nil
}

func resolveContributingVolumes(ctx context.Context, runner Runner, root Volume, mountPoints []string) ([]Volume, error) {
	result := []Volume{root}
	seen := map[string]bool{filepath.Clean(root.Root): true}
	for _, mountPoint := range mountPoints {
		if mountPoint == "" || seen[filepath.Clean(mountPoint)] || !pathWithinRoot(root.Root, mountPoint) {
			continue
		}
		volume, resolveErr := resolveVolume(ctx, runner, mountPoint)
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve nested source volume %q: %w", mountPoint, resolveErr)
		}
		seen[filepath.Clean(volume.Root)] = true
		result = append(result, volume)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Root < result[right].Root })
	return result, nil
}

func nullTerminatedString(value []byte) string {
	for index, character := range value {
		if character == 0 {
			return string(value[:index])
		}
	}
	return string(value)
}

func nullTerminatedInt8String(value []int8) string {
	bytes := make([]byte, 0, len(value))
	for _, character := range value {
		if character == 0 {
			break
		}
		bytes = append(bytes, byte(character))
	}
	return string(bytes)
}

func parseStringPlist(payload []byte) (map[string]string, error) {
	decoder := xml.NewDecoder(strings.NewReader(string(payload)))
	values := make(map[string]string)
	var key string
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "key":
			if err := decoder.DecodeElement(&key, &start); err != nil {
				return nil, err
			}
		case "string":
			var value string
			if err := decoder.DecodeElement(&value, &start); err != nil {
				return nil, err
			}
			if key != "" {
				values[key] = value
				key = ""
			}
		}
	}
	return values, nil
}
