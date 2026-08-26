package checker

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// CheckHotCold verifies that the hot and cold parts of a hot/cold repository
// agree: every metadata file (keys, snapshots, indexes) present in one part is
// present with identical content in the other. The cold part must be a
// complete repository on its own; the hot part only holds metadata and tree
// packs.
//
// hot and cold are the two backends of a hot/cold repository (see
// internal/backend/hotcold). It returns the list of mismatches found.
func CheckHotCold(ctx context.Context, hot, cold backend.Backend) (errs []error) {
	// keys, snapshots and indexes must be mirrored 1:1 between hot and cold
	for _, t := range []backend.FileType{backend.KeyFile, backend.SnapshotFile, backend.IndexFile} {
		errs = append(errs, compareFileType(ctx, hot, cold, t)...)
	}
	return errs
}

// compareFileType compares all files of type t between hot and cold.
func compareFileType(ctx context.Context, hot, cold backend.Backend, t backend.FileType) (errs []error) {
	// collect file names+sizes from both sides
	type entry struct{ size int64 }
	collect := func(be backend.Backend) (map[string]int64, error) {
		m := map[string]int64{}
		err := be.List(ctx, t, func(fi backend.FileInfo) error {
			m[fi.Name] = fi.Size
			return nil
		})
		return m, err
	}

	hotFiles, err := collect(hot)
	if err != nil {
		return append(errs, fmt.Errorf("unable to list %v in hot part: %w", t, err))
	}
	coldFiles, err := collect(cold)
	if err != nil {
		return append(errs, fmt.Errorf("unable to list %v in cold part: %w", t, err))
	}

	// every hot file must exist in cold (identical size)
	for name, size := range hotFiles {
		csize, ok := coldFiles[name]
		if !ok {
			errs = append(errs, fmt.Errorf("%v/%v: present in hot but missing in cold part", t, name))
			continue
		}
		if csize != size {
			errs = append(errs, fmt.Errorf("%v/%v: size differs (hot %d, cold %d)", t, name, size, csize))
			continue
		}
		// same size: verify identical content
		if err := identical(ctx, hot, cold, t, name); err != nil {
			errs = append(errs, err)
		}
	}
	// every cold metadata file should also be in hot (informational asymmetry is
	// expected only right after a cold-side-only write)
	for name := range coldFiles {
		if _, ok := hotFiles[name]; !ok {
			errs = append(errs, fmt.Errorf("%v/%v: present in cold but missing in hot part", t, name))
		}
	}
	return errs
}

// identical reports whether the file name of type t has identical content in
// both backends.
func identical(ctx context.Context, hot, cold backend.Backend, t backend.FileType, name string) error {
	read := func(be backend.Backend) ([]byte, error) {
		h := backend.Handle{Type: t, Name: name}
		var buf []byte
		err := be.Load(ctx, h, 0, 0, func(rd io.Reader) error {
			var err error
			buf, err = io.ReadAll(rd)
			return err
		})
		return buf, err
	}

	hotData, err := read(hot)
	if err != nil {
		return fmt.Errorf("%v/%v: unable to read from hot part: %w", t, name, err)
	}
	coldData, err := read(cold)
	if err != nil {
		return fmt.Errorf("%v/%v: unable to read from cold part: %w", t, name, err)
	}
	if !bytes.Equal(hotData, coldData) {
		return fmt.Errorf("%v/%v: content differs between hot and cold part", t, name)
	}
	return nil
}

// make sure the package compiles standalone; vaultic import used for doc clarity.
var _ = vaultic.ID{}
