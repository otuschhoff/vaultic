package main

import (
	"fmt"

	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/repository"
)

func requireLegacyMetadataMutation(repo *repository.Repository, operation string) error {
	if repo.Engine().Mode() == enginepkg.ModeSlateDB {
		return fmt.Errorf("%s is disabled for SlateDB-authoritative repositories until its revalidation path is implemented", operation)
	}
	return nil
}
