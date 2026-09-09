// Package rados implements native Ceph RADOS repository storage.
package rados

import (
	"fmt"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/options"
)

const defaultConnections = 1

type Config struct {
	Monitors     string
	ClusterFSID  string
	Pool         string
	Namespace    string
	Prefix       string
	Client       string
	Key          options.SecretString
	Connections  uint
	OperationTTL time.Duration
}

func (config Config) validate() error {
	if config.Monitors == "" || config.ClusterFSID == "" || config.Pool == "" || config.Namespace == "" || config.Prefix == "" {
		return fmt.Errorf("native RADOS configuration is incomplete")
	}
	if !strings.HasPrefix(config.Client, "client.") || config.Key.Unwrap() == "" {
		return fmt.Errorf("native RADOS requires a CephX client and key")
	}
	if config.Connections == 0 {
		return fmt.Errorf("native RADOS connections must be positive")
	}
	if config.OperationTTL <= 0 {
		return fmt.Errorf("native RADOS operation timeout must be positive")
	}
	return nil
}
