package crawl

import (
	"errors"
	"io"
	iofs "io/fs"
	"os"
	"time"

	"github.com/otuschhoff/cwalk"
	"github.com/otuschhoff/vaultic/internal/fs"
)

type walkFilesystem struct {
	filesystem fs.FS
	root       string
}

type walkFileInfo struct {
	info *fs.ExtendedFileInfo
}

func (info walkFileInfo) Name() string       { return info.info.Name }
func (info walkFileInfo) Size() int64        { return info.info.Size }
func (info walkFileInfo) Mode() os.FileMode  { return info.info.Mode }
func (info walkFileInfo) ModTime() time.Time { return info.info.ModTime }
func (info walkFileInfo) IsDir() bool        { return info.info.Mode.IsDir() }
func (info walkFileInfo) Sys() any           { return nil }

func (adapter walkFilesystem) Lstat(name string) (os.FileInfo, error) {
	info, err := adapter.filesystem.Lstat(adapter.filesystem.Join(adapter.root, name))
	if err != nil {
		return nil, err
	}
	return walkFileInfo{info: info}, nil
}

func (adapter walkFilesystem) ReadDir(name string) ([]os.DirEntry, error) {
	entries, err := adapter.ReadDirPlus(name)
	result := make([]os.DirEntry, len(entries))
	for index, entry := range entries {
		result[index] = entry.Entry
	}
	return result, err
}

func (adapter walkFilesystem) ReadDirPlus(name string) ([]cwalk.DirEntryInfo, error) {
	directory, err := adapter.filesystem.OpenFile(adapter.filesystem.Join(adapter.root, name), fs.O_DIRECTORY|fs.O_NOFOLLOW, false)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	var entries []cwalk.DirEntryInfo
	for {
		names, readErr := directory.Readdirnames(128)
		for _, child := range names {
			info, err := adapter.Lstat(adapter.filesystem.Join(name, child))
			if err != nil {
				return nil, err
			}
			entries = append(entries, cwalk.DirEntryInfo{Entry: iofs.FileInfoToDirEntry(info), Info: info})
		}
		if errors.Is(readErr, io.EOF) {
			return entries, nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}
