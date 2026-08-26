package all

import (
	"github.com/vaultic/vaultic/internal/backend/azure"
	"github.com/vaultic/vaultic/internal/backend/b2"
	"github.com/vaultic/vaultic/internal/backend/gs"
	"github.com/vaultic/vaultic/internal/backend/local"
	"github.com/vaultic/vaultic/internal/backend/location"
	"github.com/vaultic/vaultic/internal/backend/rclone"
	"github.com/vaultic/vaultic/internal/backend/rest"
	"github.com/vaultic/vaultic/internal/backend/s3"
	"github.com/vaultic/vaultic/internal/backend/sftp"
	"github.com/vaultic/vaultic/internal/backend/swift"
)

func Backends() *location.Registry {
	backends := location.NewRegistry()
	backends.Register(azure.NewFactory())
	backends.Register(b2.NewFactory())
	backends.Register(gs.NewFactory())
	backends.Register(local.NewFactory())
	backends.Register(rclone.NewFactory())
	backends.Register(rest.NewFactory())
	backends.Register(s3.NewFactory())
	backends.Register(sftp.NewFactory())
	backends.Register(swift.NewFactory())
	return backends
}
