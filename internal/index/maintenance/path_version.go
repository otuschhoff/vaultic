package maintenance

import (
	"context"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/pathindex"
)

func checkPathVersionIndex(ctx context.Context, store Store, paths []string, result *CheckResult, maxFindings uint) error {
	if len(paths) == 0 {
		return nil
	}
	_, err := pathindex.Check(ctx, pathIndexStore{Store: store}, paths, func(difference pathindex.Difference) error {
		result.PathVersionMismatch++
		kind := "path_version_drift"
		if difference.Expected == nil {
			kind = "stale_path_version"
		}
		addFinding(result, maxFindings, Finding{Kind: kind, Key: fmt.Sprintf("%x", difference.Key)})
		return nil
	})
	return err
}

func RebuildPathVersionIndex(ctx context.Context, store Store, paths []string, dryRun bool) (pathindex.BuildResult, error) {
	if len(paths) == 0 {
		return pathindex.BuildResult{}, nil
	}
	return pathindex.Rebuild(ctx, pathIndexStore{Store: store}, paths, dryRun)
}

func PrunePathVersionIndex(ctx context.Context, store Store, beforeCommit uint64, dryRun bool) (uint64, error) {
	return pathindex.PruneBefore(ctx, pathIndexStore{Store: store}, beforeCommit, dryRun)
}

type pathIndexStore struct{ Store }
