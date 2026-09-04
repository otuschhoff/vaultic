package indexcmd

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/repository"
)

func TestSessionCloseUsesReverseOrderAndJoinsErrors(t *testing.T) {
	firstErr := errors.New("first")
	secondErr := errors.New("second")
	var order []string
	session := &Session{close: []func() error{
		func() error {
			order = append(order, "repository")
			return firstErr
		},
		func() error {
			order = append(order, "daemon")
			return secondErr
		},
	}}

	err := session.Close()
	if !reflect.DeepEqual(order, []string{"daemon", "repository"}) {
		t.Fatalf("unexpected close order %v", order)
	}
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("Close() error %v does not retain both causes", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("second Close() returned %v", err)
	}
}

func TestRunWithSessionReportsActionAndCleanupFailures(t *testing.T) {
	actionErr := errors.New("action failed")
	cleanupErr := errors.New("cleanup failed")
	session := &Session{close: []func() error{
		func() error { return cleanupErr },
	}}

	_, err := runWithSession(session, func() (struct{}, error) {
		return struct{}{}, actionErr
	})
	if !errors.Is(err, actionErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("runWithSession() error %v does not retain action and cleanup failures", err)
	}
}

func TestRunWithSessionReportsCleanupFailureAfterSuccessfulAction(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	session := &Session{close: []func() error{
		func() error { return cleanupErr },
	}}

	result, err := runWithSession(session, func() (string, error) {
		return "complete", nil
	})
	if result != "complete" {
		t.Fatalf("runWithSession() result %q, want complete", result)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("runWithSession() error %v, want cleanup failure", err)
	}
}

func TestOpenSessionReportsRepositoryFailure(t *testing.T) {
	wantErr := errors.New("repository unavailable")
	_, err := OpenSession(context.Background(), func(ctx context.Context) (context.Context, *repository.Repository, func() error, error) {
		return ctx, nil, nil, wantErr
	}, nil, "")
	if !errors.Is(err, wantErr) {
		t.Fatalf("OpenSession() error %v, want %v", err, wantErr)
	}
}

func TestOpenSessionReleasesRepositoryLockAfterDaemonFailure(t *testing.T) {
	repo := repository.TestRepository(t)
	wantErr := errors.New("daemon unavailable")
	unlocked := false
	_, err := OpenSession(
		context.Background(),
		func(ctx context.Context) (context.Context, *repository.Repository, func() error, error) {
			return ctx, repo, func() error { unlocked = true; return nil }, nil
		},
		func(context.Context, string) (*daemon.Client, error) { return nil, wantErr },
		"",
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("OpenSession() error %v, want %v", err, wantErr)
	}
	if !unlocked {
		t.Fatal("OpenSession() did not release the repository lock after daemon failure")
	}
}
