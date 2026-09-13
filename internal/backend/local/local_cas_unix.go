//go:build !windows

package local

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/errors"
	"golang.org/x/sys/unix"
)

// CompareAndSwap atomically creates or replaces a file when the expected bytes match.
func (b *Local) CompareAndSwap(ctx context.Context, h backend.Handle, expected []byte, replacement []byte) (current []byte, swapped bool, err error) {
	finalname := b.Filename(h)
	dir := filepath.Dir(finalname)

	defer func() {
		if errors.Is(err, syscall.ENOSPC) || os.IsPermission(err) {
			err = backoff.Permanent(err)
		}
	}()

	release, err := b.lockMutation(ctx, finalname)
	if err != nil {
		return nil, false, err
	}
	defer release()

	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := os.MkdirAll(dir, b.Modes.Dir); err != nil {
		return nil, false, errors.WithStack(err)
	}

	current, mode, exists, err := readCurrentForCAS(finalname)
	if err != nil {
		return nil, false, err
	}

	if expected == nil {
		if exists {
			return current, false, nil
		}
		if err := writeAtomicCAS(ctx, dir, finalname, replacement, b.Modes.File); err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}

	if !exists {
		return nil, false, nil
	}
	if !bytes.Equal(current, expected) {
		return current, false, nil
	}

	if err := writeAtomicCAS(ctx, dir, finalname, replacement, mode); err != nil {
		return nil, false, err
	}
	return append([]byte{}, replacement...), true, nil
}

func (b *Local) lockMutation(ctx context.Context, _ string) (func(), error) {
	lockname := filepath.Join(b.Config.Path, ".vaultic-cas.lock")
	lockFile, err := os.OpenFile(lockname, os.O_CREATE|os.O_RDWR, 0600)
	if b.IsNotExist(err) {
		if mkdirErr := os.MkdirAll(filepath.Dir(lockname), b.Modes.Dir); mkdirErr == nil {
			lockFile, err = os.OpenFile(lockname, os.O_CREATE|os.O_RDWR, 0600)
		}
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := lockFileExclusive(ctx, lockFile); err != nil {
		_ = lockFile.Close() // Preserve the lock failure; closing an unopened mutation guard is best effort.
		return nil, err
	}
	return func() {
		_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN) // Closing the descriptor also releases the advisory lock.
		_ = lockFile.Close()                             // The completed mutation remains authoritative.
	}, nil
}

func lockFileExclusive(ctx context.Context, file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return errors.WithStack(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func readCurrentForCAS(filename string) ([]byte, os.FileMode, bool, error) {
	current, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, errors.WithStack(err)
	}
	info, err := os.Stat(filename)
	if err != nil {
		return nil, 0, false, errors.WithStack(err)
	}
	return append([]byte{}, current...), info.Mode().Perm(), true, nil
}

func writeAtomicCAS(ctx context.Context, dir, finalname string, payload []byte, mode os.FileMode) (err error) {
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
	if err = file.Sync(); err != nil && !errors.Is(err, syscall.ENOTSUP) && !isMacENOTTY(err) {
		return errors.WithStack(err)
	}
	if err = file.Close(); err != nil {
		return errors.WithStack(err)
	}
	if err = os.Rename(file.Name(), finalname); err != nil {
		return errors.WithStack(err)
	}
	if err = os.Chmod(finalname, mode); err != nil {
		return errors.WithStack(fmt.Errorf("chmod replacement %q: %w", finalname, err))
	}
	if err = fsyncDir(dir); err != nil {
		return errors.WithStack(err)
	}
	return ctx.Err()
}
