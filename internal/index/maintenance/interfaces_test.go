package maintenance

import "github.com/otuschhoff/vaultic/internal/index/daemon"

var (
	_ Reader            = (*daemon.SchemaStore)(nil)
	_ Writer            = (*daemon.SchemaStore)(nil)
	_ Store             = (*daemon.SchemaStore)(nil)
	_ EncryptionAuditor = (*daemon.SchemaStore)(nil)
)
