package all

import (
	"github.com/otuschhoff/vaultic/internal/backend/azure"
	"github.com/otuschhoff/vaultic/internal/backend/b2"
	"github.com/otuschhoff/vaultic/internal/backend/gs"
	"github.com/otuschhoff/vaultic/internal/backend/local"
	"github.com/otuschhoff/vaultic/internal/backend/location"
	"github.com/otuschhoff/vaultic/internal/backend/rclone"
	"github.com/otuschhoff/vaultic/internal/backend/rest"
	"github.com/otuschhoff/vaultic/internal/backend/s3"
	"github.com/otuschhoff/vaultic/internal/backend/sftp"
	"github.com/otuschhoff/vaultic/internal/backend/swift"
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
