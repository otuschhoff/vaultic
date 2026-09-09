package cli

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/topology"
)

func TestStorageCredentialAccess(t *testing.T) {
	tests := []struct {
		name     string
		policy   LockPolicy
		options  OpenOptions
		tier     topology.StorageCredentialTier
		usesLock bool
	}{
		{name: "lock-free read", policy: LockNone, tier: topology.StorageRead},
		{name: "locked read", policy: LockShared, options: OpenOptions{LockFreeRead: true}, tier: topology.StorageRead, usesLock: true},
		{name: "backup", policy: LockShared, tier: topology.StorageAppend, usesLock: true},
		{name: "maintenance", policy: LockExclusive, tier: topology.StorageMaintain, usesLock: true},
		{
			name: "coherent read", policy: LockExclusive, options: OpenOptions{ReadOnlyData: true},
			tier: topology.StorageRead, usesLock: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tier, usesLock := storageCredentialAccess(test.policy, test.options)
			if tier != string(test.tier) || usesLock != test.usesLock {
				t.Fatalf("storage access = (%q, %v), want (%q, %v)", tier, usesLock, test.tier, test.usesLock)
			}
		})
	}
}
