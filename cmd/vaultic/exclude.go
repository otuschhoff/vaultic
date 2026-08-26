package main

import (
	"github.com/vaultic/vaultic/internal/archiver"
	"github.com/vaultic/vaultic/internal/debug"
	"github.com/vaultic/vaultic/internal/errors"
	"github.com/vaultic/vaultic/internal/fs"
	"github.com/vaultic/vaultic/internal/repository"
)

// rejectResticCache returns a RejectByNameFunc that rejects the vaultic cache
// directory (if set).
func rejectResticCache(repo *repository.Repository) (archiver.RejectByNameFunc, error) {
	if repo.Cache() == nil {
		return func(string) bool {
			return false
		}, nil
	}
	cacheBase := repo.Cache().BaseDir()

	if cacheBase == "" {
		return nil, errors.New("cacheBase is empty string")
	}

	return func(item string) bool {
		if fs.HasPathPrefix(cacheBase, item) {
			debug.Log("rejecting vaultic cache directory %v", item)
			return true
		}

		return false
	}, nil
}
