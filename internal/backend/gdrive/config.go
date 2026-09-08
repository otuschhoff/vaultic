package gdrive

import (
	"path"
	"strings"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/options"
)

type Config struct {
	DriveID      string
	RootFolderID string
	Prefix       string
	Connections  uint `option:"connections" help:"set a limit for concurrent Google Drive operations (default: 4)"`
	apiEndpoint  string
}

func NewConfig() Config {
	return Config{Connections: 4}
}

func init() {
	options.Register("gdrive", Config{})
}

// ParseConfig accepts gdrive:DRIVE_ID:/path and gdrive:DRIVE_ID/path.
func ParseConfig(value string) (*Config, error) {
	if !strings.HasPrefix(value, "gdrive:") {
		return nil, errors.New("gdrive: invalid format")
	}
	value = strings.TrimPrefix(value, "gdrive:")
	driveID, prefix, found := strings.Cut(value, ":")
	if !found {
		driveID, prefix, found = strings.Cut(value, "/")
	}
	if !found || driveID == "" {
		return nil, errors.New("gdrive: drive ID or path not found")
	}
	config := NewConfig()
	config.DriveID = driveID
	config.Prefix = strings.TrimPrefix(path.Clean("/"+prefix), "/")
	return &config, nil
}
