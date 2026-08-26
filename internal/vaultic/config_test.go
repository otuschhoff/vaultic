package vaultic_test

import (
	"context"
	"testing"

	rtest "github.com/vaultic/vaultic/internal/test"
	"github.com/vaultic/vaultic/internal/vaultic"
)

type saver struct {
	fn func(vaultic.FileType, []byte) (vaultic.ID, error)
}

func (s saver) SaveUnpacked(_ context.Context, t vaultic.FileType, buf []byte) (vaultic.ID, error) {
	return s.fn(t, buf)
}

func (s saver) Connections() uint {
	return 2
}

type loader struct {
	fn func(vaultic.FileType, vaultic.ID) ([]byte, error)
}

func (l loader) LoadUnpacked(_ context.Context, t vaultic.FileType, id vaultic.ID) (data []byte, err error) {
	return l.fn(t, id)
}

func (l loader) Connections() uint {
	return 2
}

func TestConfig(t *testing.T) {
	var resultBuf []byte
	save := func(tpe vaultic.FileType, buf []byte) (vaultic.ID, error) {
		rtest.Assert(t, tpe == vaultic.ConfigFile,
			"wrong backend type: got %v, wanted %v",
			tpe, vaultic.ConfigFile)

		resultBuf = buf
		return vaultic.ID{}, nil
	}

	cfg1, err := vaultic.CreateConfig(vaultic.MaxRepoVersion, nil)
	rtest.OK(t, err)

	err = vaultic.SaveConfig(context.TODO(), saver{save}, cfg1)
	rtest.OK(t, err)

	load := func(tpe vaultic.FileType, id vaultic.ID) ([]byte, error) {
		rtest.Assert(t, tpe == vaultic.ConfigFile,
			"wrong backend type: got %v, wanted %v",
			tpe, vaultic.ConfigFile)

		return resultBuf, nil
	}

	cfg2, err := vaultic.LoadConfig(context.TODO(), loader{load})
	rtest.OK(t, err)

	rtest.Assert(t, cfg1 == cfg2,
		"configs aren't equal: %v != %v", cfg1, cfg2)
}
