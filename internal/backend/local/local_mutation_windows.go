//go:build windows

package local

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/errors"
	"golang.org/x/sys/windows"
)

func (b *Local) CompareAndSwap(ctx context.Context, h backend.Handle, expected []byte, replacement []byte) ([]byte, bool, error) {
	finalname := b.Filename(h)
	release, err := b.lockMutation(ctx, finalname)
	if err != nil {
		return nil, false, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := os.MkdirAll(filepath.Dir(finalname), b.Modes.Dir); err != nil {
		return nil, false, errors.WithStack(err)
	}
	current, exists, err := readCurrentForCASWindows(finalname)
	if err != nil {
		return nil, false, err
	}
	if expected == nil {
		if exists {
			return current, false, nil
		}
	} else if !exists || !bytes.Equal(current, expected) {
		return current, false, nil
	}
	if err := writeAtomicCASWindows(ctx, filepath.Dir(finalname), finalname, replacement, b.Modes.File); err != nil {
		return nil, false, err
	}
	if expected == nil {
		return nil, true, nil
	}
	return append([]byte{}, replacement...), true, nil
}

func (b *Local) lockMutation(ctx context.Context, _ string) (func(), error) {
	lockFile, err := os.OpenFile(filepath.Join(b.Config.Path, ".vaultic-cas.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	overlapped := &windows.Overlapped{}
	for {
		err = windows.LockFileEx(windows.Handle(lockFile.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
		if err == nil {
			return func() {
				_ = windows.UnlockFileEx(windows.Handle(lockFile.Fd()), 0, 1, 0, overlapped) // Closing the descriptor also releases the advisory lock.
				_ = lockFile.Close()                                                        // The completed mutation remains authoritative.
			}, nil
		}
		if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			_ = lockFile.Close() // Preserve the lock failure; closing an unopened mutation guard is best effort.
			return nil, errors.WithStack(err)
		}
		select {
		case <-ctx.Done():
			_ = lockFile.Close() // Preserve cancellation; closing an unopened mutation guard is best effort.
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func readCurrentForCASWindows(filename string) ([]byte, bool, error) {
	current, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return append([]byte{}, current...), true, nil
}

func writeAtomicCASWindows(ctx context.Context, dir, finalname string, payload []byte, mode os.FileMode) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := tempFile(dir, filepath.Base(finalname)+"-cas-tmp-")
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		if err != nil {
			_ = file.Close()           // Preserve the publication failure during best-effort cleanup.
			_ = os.Remove(file.Name()) // Preserve the publication failure during best-effort cleanup.
		}
	}()
	if _, err = file.Write(payload); err != nil {
		return errors.WithStack(err)
	}
	if err = file.Sync(); err != nil {
		return errors.WithStack(err)
	}
	if err = file.Close(); err != nil {
		return errors.WithStack(err)
	}
	from, err := windows.UTF16PtrFromString(file.Name())
	if err != nil {
		return errors.WithStack(err)
	}
	to, err := windows.UTF16PtrFromString(finalname)
	if err != nil {
		return errors.WithStack(err)
	}
	if err = windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return errors.WithStack(err)
	}
	if err = os.Chmod(finalname, mode); err != nil {
		return errors.WithStack(err)
	}
	return ctx.Err()
}
