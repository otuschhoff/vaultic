package nfs

import (
	"fmt"
	"net"
	"strings"
	"time"

	gonfs "github.com/willscott/go-nfs"
)

const (
	DefaultNFSListenPort   = 20490
	DefaultMountListenPort = 20491
	DefaultHandleLimit     = 4096
	DefaultMaxConnections  = 128
	DefaultMaxRequests     = 4096
	DefaultMaxRequestSize  = 20 << 20
)

type Config struct {
	Listen                string
	NFSPort               int
	MountPort             int
	EphemeralPorts        bool
	ExportName            string
	AllowCIDRs            []string
	AcknowledgeInsecure   bool
	MaxReadSize           int
	IdleTimeout           time.Duration
	ConnectionIdleTimeout time.Duration
	DrainTimeout          time.Duration
	MaxConnections        int
	MaxRequests           int
	MaxRequestSize        int
	HandleLimit           int
}

type validatedConfig struct {
	Config
	allowNets []*net.IPNet
}

func applyConfigDefaults(cfg Config) Config {
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1"
	}
	if cfg.NFSPort == 0 && !cfg.EphemeralPorts {
		cfg.NFSPort = DefaultNFSListenPort
	}
	if cfg.MountPort == 0 && !cfg.EphemeralPorts {
		cfg.MountPort = DefaultMountListenPort
	}
	if cfg.ExportName == "" {
		cfg.ExportName = "/snapshot"
	}
	if cfg.MaxReadSize == 0 {
		cfg.MaxReadSize = 1 << 20
	}
	if cfg.ConnectionIdleTimeout == 0 {
		cfg.ConnectionIdleTimeout = 2 * time.Minute
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 5 * time.Second
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = DefaultMaxConnections
	}
	if cfg.MaxRequests == 0 {
		cfg.MaxRequests = DefaultMaxRequests
	}
	if cfg.MaxRequestSize == 0 {
		cfg.MaxRequestSize = DefaultMaxRequestSize
	}
	if cfg.HandleLimit == 0 {
		cfg.HandleLimit = DefaultHandleLimit
	}
	return cfg
}

func validateConfig(cfg Config) (validatedConfig, error) {
	cfg = applyConfigDefaults(cfg)
	if net.ParseIP(cfg.Listen) == nil {
		return validatedConfig{}, fmt.Errorf("listen must be an IP address: %q", cfg.Listen)
	}
	if cfg.NFSPort < 0 || cfg.NFSPort > 65535 || cfg.MountPort < 0 || cfg.MountPort > 65535 {
		return validatedConfig{}, fmt.Errorf("ports must be between 0 and 65535")
	}
	if cfg.EphemeralPorts && (cfg.NFSPort != 0 || cfg.MountPort != 0) {
		return validatedConfig{}, fmt.Errorf("ephemeral ports require both ports to be zero")
	}
	if cfg.NFSPort != 0 && cfg.NFSPort == cfg.MountPort {
		return validatedConfig{}, fmt.Errorf("NFS and mount ports must differ")
	}
	if !strings.HasPrefix(cfg.ExportName, "/") || cfg.ExportName == "/" ||
		strings.Contains(cfg.ExportName[1:], "/") || strings.IndexByte(cfg.ExportName, 0) >= 0 {
		return validatedConfig{}, fmt.Errorf("export name must be one absolute path component")
	}
	if cfg.MaxReadSize <= 0 || cfg.MaxReadSize > gonfs.MaxRead {
		return validatedConfig{}, fmt.Errorf("max read size must be between 1 and %d", gonfs.MaxRead)
	}
	if cfg.IdleTimeout < 0 || cfg.ConnectionIdleTimeout <= 0 || cfg.DrainTimeout <= 0 ||
		cfg.MaxConnections <= 0 || cfg.MaxRequests <= 0 || cfg.MaxRequestSize < 40 ||
		cfg.MaxRequestSize > int(^uint32(0)>>1) || cfg.HandleLimit <= 0 {
		return validatedConfig{}, fmt.Errorf("timeouts and limits must be positive and request size at least 40 bytes")
	}

	validated := validatedConfig{Config: cfg}
	for _, raw := range cfg.AllowCIDRs {
		_, network, err := net.ParseCIDR(raw)
		if err != nil {
			return validatedConfig{}, fmt.Errorf("invalid allow CIDR %q: %w", raw, err)
		}
		validated.allowNets = append(validated.allowNets, network)
	}
	if !net.ParseIP(cfg.Listen).IsLoopback() && (len(validated.allowNets) == 0 || !cfg.AcknowledgeInsecure) {
		return validatedConfig{}, fmt.Errorf("non-loopback listen requires allow CIDRs and acknowledge insecure")
	}
	return validated, nil
}
