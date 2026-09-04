package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
)

const DeferredObservationKind = "crawl-observation-v1"

func DecodeDeferredObservations(payloads []json.RawMessage) ([]DeferredObservation, error) {
	observations := make([]DeferredObservation, len(payloads))
	for index, payload := range payloads {
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&observations[index]); err != nil {
			return nil, fmt.Errorf("decode deferred crawl observation: %w", err)
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			return nil, fmt.Errorf("decode deferred crawl observation: trailing data")
		}
	}
	return observations, nil
}

type DeferredStat struct {
	Name       string      `json:"name"`
	Mode       os.FileMode `json:"mode"`
	DeviceID   uint64      `json:"device_id"`
	Inode      uint64      `json:"inode"`
	Links      uint64      `json:"links"`
	UID        uint32      `json:"uid"`
	GID        uint32      `json:"gid"`
	Device     uint64      `json:"device"`
	BlockSize  int64       `json:"block_size"`
	Blocks     int64       `json:"blocks"`
	Size       int64       `json:"size"`
	AccessTime time.Time   `json:"access_time"`
	ModTime    time.Time   `json:"mod_time"`
	ChangeTime time.Time   `json:"change_time"`
}

type DeferredObservation struct {
	SnapshotPath string       `json:"snapshot_path"`
	SourcePath   string       `json:"source_path"`
	Node         data.Node    `json:"node"`
	Stat         DeferredStat `json:"stat"`
	ParentPath   string       `json:"parent_path"`
	ParentStat   DeferredStat `json:"parent_stat"`
}

type DeferredCapture struct {
	filesystem statFS
	mu         sync.Mutex
	items      []DeferredObservation
	errors     []error
}

func NewDeferredCapture(filesystem statFS) *DeferredCapture {
	return &DeferredCapture{filesystem: filesystem}
}

func (capture *DeferredCapture) Observe(snapshotPath, sourcePath string, node *data.Node) {
	if node == nil {
		return
	}
	info, err := capture.filesystem.Lstat(sourcePath)
	parentPath := capture.filesystem.Dir(sourcePath)
	var parent *fs.ExtendedFileInfo
	if err == nil {
		parent, err = capture.filesystem.Lstat(parentPath)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if err != nil {
		capture.errors = append(capture.errors, fmt.Errorf("capture deferred crawl evidence: %w", err))
		return
	}
	copyNode := *node
	copyNode.Content = append(copyNode.Content[:0:0], node.Content...)
	if node.Subtree != nil {
		subtree := *node.Subtree
		copyNode.Subtree = &subtree
	}
	capture.items = append(
		capture.items,
		DeferredObservation{
			SnapshotPath: snapshotPath,
			SourcePath:   sourcePath,
			Node:         copyNode,
			Stat:         deferredStat(info),
			ParentPath:   parentPath,
			ParentStat:   deferredStat(parent),
		},
	)
}

func (capture *DeferredCapture) Close() ([]DeferredObservation, error) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	items := append([]DeferredObservation(nil), capture.items...)
	return items, errors.Join(capture.errors...)
}

func ReplayDeferred(ctx context.Context, store Store, observations []DeferredObservation, options Options) ([]byte, error) {
	filesystem := &deferredFilesystem{entries: make(map[string]*fs.ExtendedFileInfo), parents: make(map[string]string)}
	for _, observation := range observations {
		if observation.SourcePath == "" || observation.ParentPath == "" {
			return nil, fmt.Errorf("deferred crawl observation has empty source identity")
		}
		stat, parent := observation.Stat.extended(), observation.ParentStat.extended()
		filesystem.entries[observation.SourcePath] = &stat
		filesystem.entries[observation.ParentPath] = &parent
		filesystem.parents[observation.SourcePath] = observation.ParentPath
	}
	reconciler, err := New(ctx, filesystem, store, options)
	if err != nil {
		return nil, err
	}
	for index := range observations {
		observation := &observations[index]
		reconciler.Observe(observation.SnapshotPath, observation.SourcePath, &observation.Node)
	}
	if err := reconciler.Close(); err != nil {
		return nil, err
	}
	root := reconciler.RootKey()
	if len(root) == 0 {
		return nil, fmt.Errorf("deferred crawl replay produced no snapshot root")
	}
	return root, nil
}

type deferredFilesystem struct {
	entries map[string]*fs.ExtendedFileInfo
	parents map[string]string
}

func (filesystem *deferredFilesystem) Lstat(path string) (*fs.ExtendedFileInfo, error) {
	info, ok := filesystem.entries[path]
	if !ok {
		return nil, fmt.Errorf("deferred stat evidence is missing")
	}
	copyInfo := *info
	return &copyInfo, nil
}

func (filesystem *deferredFilesystem) Dir(path string) string {
	if parent, ok := filesystem.parents[path]; ok {
		return parent
	}
	return path
}

func deferredStat(info *fs.ExtendedFileInfo) DeferredStat {
	return DeferredStat{
		Name:       info.Name,
		Mode:       info.Mode,
		DeviceID:   info.DeviceID,
		Inode:      info.Inode,
		Links:      info.Links,
		UID:        info.UID,
		GID:        info.GID,
		Device:     info.Device,
		BlockSize:  info.BlockSize,
		Blocks:     info.Blocks,
		Size:       info.Size,
		AccessTime: info.AccessTime,
		ModTime:    info.ModTime,
		ChangeTime: info.ChangeTime,
	}
}

func (stat DeferredStat) extended() fs.ExtendedFileInfo {
	return fs.ExtendedFileInfo{
		Name:       stat.Name,
		Mode:       stat.Mode,
		DeviceID:   stat.DeviceID,
		Inode:      stat.Inode,
		Links:      stat.Links,
		UID:        stat.UID,
		GID:        stat.GID,
		Device:     stat.Device,
		BlockSize:  stat.BlockSize,
		Blocks:     stat.Blocks,
		Size:       stat.Size,
		AccessTime: stat.AccessTime,
		ModTime:    stat.ModTime,
		ChangeTime: stat.ChangeTime,
	}
}
