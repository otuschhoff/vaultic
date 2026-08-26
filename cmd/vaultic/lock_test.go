package main

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/feature"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func TestEffectiveLockPolicy(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.LockFree, false)()

	rtest.Equals(t, LockNone, effectiveLockPolicy(LockShared, lockOpenOptions{DryRun: true}))
	rtest.Equals(t, LockNone, effectiveLockPolicy(LockShared, lockOpenOptions{AllowNoLock: true}))
	rtest.Equals(t, LockExclusive, effectiveLockPolicy(LockExclusive, lockOpenOptions{AllowNoLock: true}))
	rtest.Equals(t, LockShared, effectiveLockPolicy(LockShared, lockOpenOptions{LockFreeRead: true}))

	restore := feature.TestSetFlag(t, feature.Flag, feature.LockFree, true)
	defer restore()
	rtest.Equals(t, LockNone, effectiveLockPolicy(LockShared, lockOpenOptions{LockFreeRead: true}))
	rtest.Equals(t, LockShared, effectiveLockPolicy(LockShared, lockOpenOptions{}))
	rtest.Equals(t, LockExclusive, effectiveLockPolicy(LockExclusive, lockOpenOptions{LockFreeRead: true}))
}

func TestLockPolicyStrings(t *testing.T) {
	rtest.Equals(t, "none", LockNone.String())
	rtest.Equals(t, "shared", LockShared.String())
	rtest.Equals(t, "exclusive", LockExclusive.String())
}
