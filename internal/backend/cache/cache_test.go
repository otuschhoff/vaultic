package cache

import (
	"os"
	"path/filepath"
	"testing"

	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestNew(t *testing.T) {
	parent := rtest.TempDir(t)
	basedir := filepath.Join(parent, "cache")
	id := vaultic.NewRandomID().String()
	tagFile := filepath.Join(basedir, "CACHEDIR.TAG")
	versionFile := filepath.Join(basedir, id, "version")

	const (
		stepCreate = iota
		stepComplete
		stepRmTag
		stepRmVersion
		stepEnd
	)

	for step := range stepEnd {
		switch step {
		case stepRmTag:
			rtest.OK(t, os.Remove(tagFile))
		case stepRmVersion:
			rtest.OK(t, os.Remove(versionFile))
		}

		c, err := New(id, basedir)
		rtest.OK(t, err)
		rtest.Equals(t, basedir, c.Base)
		rtest.Equals(t, step == stepCreate, c.Created)

		for _, name := range []string{tagFile, versionFile} {
			info, err := os.Lstat(name)
			rtest.OK(t, err)
			rtest.Assert(t, info.Mode().IsRegular(), "")
		}
	}
}
