package restorer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type nfsRestoreSession struct {
	restorer    *Restorer
	destination *fs.NFSRestore
	verify      bool
	count       uint64
	hardlinks   map[[2]uint64]string
}

func (res *Restorer) RestoreToNFS(ctx context.Context, destination *fs.NFSRestore, verify bool) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if res.options.Delete || res.options.Sparse || res.options.OwnershipByName || (res.options.DryRun && verify) {
		return 0, fmt.Errorf("direct NFS restore does not support delete, sparse, ownership-by-name or dry-run verification")
	}
	if res.options.Overwrite < OverwriteAlways || res.options.Overwrite >= OverwriteInvalid {
		return 0, fmt.Errorf("invalid overwrite mode")
	}
	if res.sn.Tree == nil {
		return 0, fmt.Errorf("snapshot has no tree")
	}
	session := &nfsRestoreSession{restorer: res, destination: destination, verify: verify, hardlinks: make(map[[2]uint64]string)}
	err := res.traverseTree(ctx, string(filepath.Separator), *res.sn.Tree, treeVisitor{
		enterDir: func(_ *data.Node, _ string, location string) error {
			name := nfsRestorePath(location)
			if res.options.DryRun {
				info, err := destination.Lstat(name)
				if os.IsNotExist(err) {
					return nil
				}
				if err == nil && !info.Mode.IsDir() {
					err = fmt.Errorf("NFS restore directory %q is not a directory", name)
				}
				return err
			}
			return destination.EnsureDir(name)
		},
		visitNode: func(node *data.Node, _ string, location string) error {
			return session.restoreNode(ctx, node, location)
		},
		leaveDir: func(node *data.Node, _ string, location string, _ []string) error {
			if node == nil {
				return nil
			}
			res.options.Progress.AddProgress(location, ActionDirRestored, 0, 0)
			if res.options.DryRun {
				return nil
			}
			return destination.SetMetadata(nfsRestorePath(location), node)
		},
	})
	return session.count, err
}

func nfsRestorePath(location string) string {
	name := strings.TrimPrefix(filepath.ToSlash(location), "/")
	if name == "" {
		return "."
	}
	return name
}

func (session *nfsRestoreSession) skipNode(node *data.Node, name string) (bool, error) {
	info, err := session.destination.Lstat(name)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	switch session.restorer.options.Overwrite {
	case OverwriteNever:
		return true, nil
	case OverwriteIfNewer:
		return !info.ModTime.Before(node.ModTime), nil
	case OverwriteIfChanged:
		return node.Type == data.NodeTypeFile && info.Mode.IsRegular() && uint64(info.Size) == node.Size && info.ModTime.Equal(node.ModTime), nil
	default:
		return false, nil
	}
}

func (session *nfsRestoreSession) restoreSkipped(ctx context.Context, node *data.Node, name, location string) error {
	session.restorer.options.Progress.AddSkippedFile(location, node.Size)
	if session.restorer.options.Overwrite != OverwriteIfChanged || session.restorer.options.DryRun {
		return nil
	}
	if session.verify && node.Type == data.NodeTypeFile {
		reader, err := session.destination.Open(name)
		if err != nil {
			return err
		}
		if err := session.verifyFile(ctx, node, reader); err != nil {
			return err
		}
	}
	return session.destination.SetMetadata(name, node)
}

func (session *nfsRestoreSession) restoreNode(ctx context.Context, node *data.Node, location string) (resultErr error) {
	if node.Type != data.NodeTypeFile && node.Type != data.NodeTypeSymlink {
		return fmt.Errorf("direct NFS restore does not support node type %s at %s", node.Type, location)
	}
	name := nfsRestorePath(location)
	skipped, err := session.skipNode(node, name)
	if err != nil {
		return err
	}
	progress := session.restorer.options.Progress
	if skipped {
		return session.restoreSkipped(ctx, node, name, location)
	}
	progress.AddFile(node.Size)
	if session.restorer.options.DryRun {
		progress.AddProgress(location, ActionFileRestored, node.Size, node.Size)
		return nil
	}
	identity := [2]uint64{node.DeviceID, node.Inode}
	hardlink := ""
	if node.Type == data.NodeTypeFile && node.Links > 1 {
		hardlink = session.hardlinks[identity]
	}
	file, err := session.destination.Create(name, node, hardlink)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, file.Close())
	}()
	if node.Type == data.NodeTypeFile && hardlink == "" {
		if err := session.writeFile(ctx, node, file); err != nil {
			return err
		}
	}
	if session.verify && node.Type == data.NodeTypeFile {
		reader, err := file.Open()
		if err != nil {
			return err
		}
		if err := session.verifyFile(ctx, node, reader); err != nil {
			return err
		}
	}
	if err := file.Publish(node, session.restorer.options.Overwrite == OverwriteNever); err != nil {
		return err
	}
	if node.Type == data.NodeTypeFile {
		session.count++
		if node.Links > 1 {
			session.hardlinks[identity] = name
		}
	}
	progress.AddProgress(location, ActionFileRestored, node.Size, node.Size)
	return nil
}

func (session *nfsRestoreSession) writeFile(ctx context.Context, node *data.Node, writer io.Writer) error {
	var written uint64
	for _, id := range node.Content {
		blob, err := session.restorer.repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
		if err != nil {
			return err
		}
		if uint64(len(blob)) > node.Size-written {
			return fmt.Errorf("snapshot file content exceeds recorded size")
		}
		count, err := writer.Write(blob)
		written += uint64(count)
		if err != nil {
			return err
		}
		if count != len(blob) {
			return io.ErrShortWrite
		}
	}
	if written != node.Size {
		return fmt.Errorf("snapshot file content size mismatch: %d != %d", written, node.Size)
	}
	return nil
}

func (session *nfsRestoreSession) verifyFile(ctx context.Context, node *data.Node, reader io.ReadCloser) error {
	defer reader.Close()
	for _, id := range node.Content {
		expected, err := session.restorer.repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
		if err != nil {
			return err
		}
		actual := make([]byte, len(expected))
		if _, err := io.ReadFull(reader, actual); err != nil {
			return err
		}
		if !bytes.Equal(actual, expected) {
			return fmt.Errorf("NFS restored content verification failed for %q", node.Name)
		}
	}
	var tail [1]byte
	if count, err := reader.Read(tail[:]); count != 0 || !errors.Is(err, io.EOF) {
		return fmt.Errorf("NFS verification expected EOF for %q: %d bytes: %w", node.Name, count, errors.Join(io.ErrUnexpectedEOF, err))
	}
	return nil
}
