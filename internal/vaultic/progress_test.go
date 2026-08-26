package vaultic_test

import (
	"testing"

	"github.com/vaultic/vaultic/internal/vaultic"
)

func TestNoopCounter(_ *testing.T) {
	c := vaultic.NoopCounter
	c.Add(1)
	c.SetMax(42)
	c.Done()
	v, max := c.Get()
	if v != 0 || max != 0 {
		panic("noop counter must not change")
	}
}
