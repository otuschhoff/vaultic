package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
)

// WrapExperimentFS applies a validated source scenario to filesystem I/O.
func WrapExperimentFS(ctx context.Context, filesystem fs.FS, controller *ExperimentController) fs.FS {
	if controller == nil || !controller.Enabled() {
		return filesystem
	}
	return &experimentFS{FS: filesystem, ctx: ctx, controller: controller}
}

type experimentFS struct {
	fs.FS
	ctx        context.Context
	controller *ExperimentController
}

func (filesystem *experimentFS) OpenFile(name string, flag int, metadataOnly bool) (fs.File, error) {
	var file fs.File
	err := filesystem.run("open", sourceIdentity(name), 0, func() error {
		var openErr error
		file, openErr = filesystem.FS.OpenFile(name, flag, metadataOnly)
		return openErr
	})
	if err != nil {
		return nil, err
	}
	return &experimentFile{
		File: file, ctx: filesystem.ctx, controller: filesystem.controller,
		identity: sourceIdentity(name),
	}, nil
}

func (filesystem *experimentFS) Lstat(name string) (*fs.ExtendedFileInfo, error) {
	var info *fs.ExtendedFileInfo
	err := filesystem.run("stat", sourceIdentity(name), 0, func() error {
		var statErr error
		info, statErr = filesystem.FS.Lstat(name)
		return statErr
	})
	return info, err
}

func (filesystem *experimentFS) run(method, identity string, bytes uint64, operation func() error) error {
	if !filesystem.controller.Matches("source", method) {
		return operation()
	}
	return filesystem.controller.Run(filesystem.ctx, ExperimentEvent{Identity: identity, Bytes: bytes}, func(context.Context) error {
		return operation()
	})
}

type experimentFile struct {
	fs.File
	ctx        context.Context
	controller *ExperimentController
	identity   string
}

func (file *experimentFile) Read(buffer []byte) (int, error) {
	var read int
	err := file.run("read", uint64(len(buffer)), func() error {
		var readErr error
		read, readErr = file.File.Read(buffer)
		return readErr
	})
	return read, err
}

func (file *experimentFile) Stat() (*fs.ExtendedFileInfo, error) {
	var info *fs.ExtendedFileInfo
	err := file.run("stat", 0, func() error {
		var statErr error
		info, statErr = file.File.Stat()
		return statErr
	})
	return info, err
}

func (file *experimentFile) Readdirnames(count int) ([]string, error) {
	var names []string
	err := file.run("list", 0, func() error {
		var listErr error
		names, listErr = file.File.Readdirnames(count)
		return listErr
	})
	return names, err
}

func (file *experimentFile) MakeReadable() error {
	return file.run("open", 0, file.File.MakeReadable)
}

func (file *experimentFile) ToNode(ignoreXattrListError bool, warnf func(string, ...any)) (*data.Node, error) {
	var node *data.Node
	err := file.run("stat", 0, func() error {
		var nodeErr error
		node, nodeErr = file.File.ToNode(ignoreXattrListError, warnf)
		return nodeErr
	})
	return node, err
}

func (file *experimentFile) run(method string, bytes uint64, operation func() error) error {
	if !file.controller.Matches("source", method) {
		return operation()
	}
	return file.controller.Run(file.ctx, ExperimentEvent{Identity: file.identity, Bytes: bytes}, func(context.Context) error {
		return operation()
	})
}

func sourceIdentity(name string) string {
	digest := sha256.Sum256([]byte(name))
	return "source-" + hex.EncodeToString(digest[:16])
}

var _ fs.FS = (*experimentFS)(nil)
var _ fs.File = (*experimentFile)(nil)
var _ io.Reader = (*experimentFile)(nil)
