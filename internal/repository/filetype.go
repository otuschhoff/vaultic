package repository

import (
	"github.com/vaultic/vaultic/internal/backend"
	"github.com/vaultic/vaultic/internal/vaultic"
)

// Compile-time checks that vaultic and backend FileType constants match. A constant mismatch
// would be an out-of-bounds access that is detected by the compiler.
var (
	_ = [1]struct{}{}[backend.PackFile-backend.FileType(vaultic.PackFile)]
	_ = [1]struct{}{}[backend.KeyFile-backend.FileType(vaultic.KeyFile)]
	_ = [1]struct{}{}[backend.LockFile-backend.FileType(vaultic.LockFile)]
	_ = [1]struct{}{}[backend.SnapshotFile-backend.FileType(vaultic.SnapshotFile)]
	_ = [1]struct{}{}[backend.IndexFile-backend.FileType(vaultic.IndexFile)]
	_ = [1]struct{}{}[backend.ConfigFile-backend.FileType(vaultic.ConfigFile)]
)
