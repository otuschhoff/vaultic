package telemetry

import (
	"context"
	"errors"
	"io"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
)

type productionFS struct {
	fs.FS
	ctx        context.Context
	accounting *ProductionAccounting
}

func WrapProductionFS(ctx context.Context, filesystem fs.FS, accounting *ProductionAccounting) fs.FS {
	if filesystem == nil || accounting == nil || !accounting.enabled {
		return filesystem
	}
	return &productionFS{FS: filesystem, ctx: ctx, accounting: accounting}
}

func (filesystem *productionFS) UnwrapFS() fs.FS { return filesystem.FS }

func (filesystem *productionFS) OpenFile(name string, flag int, metadataOnly bool) (fs.File, error) {
	done := filesystem.accounting.StartBlocking(filesystem.ctx, "source", "source_io")
	defer done.Done()
	dependency := filesystem.accounting.StartDependency(filesystem.ctx, "source")
	file, err := filesystem.FS.OpenFile(name, flag, metadataOnly)
	dependency.Finish(err)
	if err != nil {
		return nil, err
	}
	return &productionFile{File: file, ctx: filesystem.ctx, accounting: filesystem.accounting}, nil
}

func (filesystem *productionFS) Lstat(name string) (*fs.ExtendedFileInfo, error) {
	done := filesystem.accounting.StartBlocking(filesystem.ctx, "source", "source_io")
	defer done.Done()
	dependency := filesystem.accounting.StartDependency(filesystem.ctx, "source")
	info, err := filesystem.FS.Lstat(name)
	dependency.Finish(err)
	return info, err
}

type productionFile struct {
	fs.File
	ctx        context.Context
	accounting *ProductionAccounting
}

func (file *productionFile) Read(buffer []byte) (int, error) {
	done := file.accounting.StartBlocking(file.ctx, "source", "source_io")
	defer done.Done()
	dependency := file.accounting.StartDependency(file.ctx, "source")
	read, err := file.File.Read(buffer)
	dependency.AddBytes(uint64(read))
	if errors.Is(err, io.EOF) {
		dependency.Finish(nil)
	} else {
		dependency.Finish(err)
	}
	file.accounting.AddProcessed(file.ctx, "source", uint64(read))
	return read, err
}

func (file *productionFile) Stat() (*fs.ExtendedFileInfo, error) {
	done := file.accounting.StartBlocking(file.ctx, "source", "source_io")
	defer done.Done()
	dependency := file.accounting.StartDependency(file.ctx, "source")
	info, err := file.File.Stat()
	dependency.Finish(err)
	return info, err
}

func (file *productionFile) Readdirnames(count int) ([]string, error) {
	done := file.accounting.StartBlocking(file.ctx, "source", "source_io")
	defer done.Done()
	dependency := file.accounting.StartDependency(file.ctx, "source")
	names, err := file.File.Readdirnames(count)
	dependency.Finish(err)
	return names, err
}

func (file *productionFile) MakeReadable() error {
	done := file.accounting.StartBlocking(file.ctx, "source", "source_io")
	defer done.Done()
	dependency := file.accounting.StartDependency(file.ctx, "source")
	err := file.File.MakeReadable()
	dependency.Finish(err)
	return err
}

func (file *productionFile) ReaddirEntries(count int) ([]fs.ReadDirEntry, error) {
	provider, ok := file.File.(interface {
		ReaddirEntries(int) ([]fs.ReadDirEntry, error)
	})
	if !ok {
		return nil, errors.ErrUnsupported
	}
	done := file.accounting.StartBlocking(file.ctx, "source", "source_io")
	defer done.Done()
	dependency := file.accounting.StartDependency(file.ctx, "source")
	entries, err := provider.ReaddirEntries(count)
	dependency.Finish(err)
	for index := range entries {
		open := entries[index].OpenMetadata
		entries[index].OpenMetadata = func() (fs.File, error) {
			done := file.accounting.StartBlocking(file.ctx, "source", "source_io")
			defer done.Done()
			dependency := file.accounting.StartDependency(file.ctx, "source")
			metadata, err := open()
			dependency.Finish(err)
			if err != nil {
				return nil, err
			}
			return &productionFile{File: metadata, ctx: file.ctx, accounting: file.accounting}, nil
		}
	}
	return entries, err
}

func (file *productionFile) ToNode(ignoreXattrListError bool, warnf func(string, ...any)) (*data.Node, error) {
	done := file.accounting.StartBlocking(file.ctx, "source", "source_io")
	defer done.Done()
	dependency := file.accounting.StartDependency(file.ctx, "source")
	node, err := file.File.ToNode(ignoreXattrListError, warnf)
	dependency.Finish(err)
	return node, err
}
