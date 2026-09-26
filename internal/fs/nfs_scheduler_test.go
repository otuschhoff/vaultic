package fs

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	client "github.com/willscott/go-nfs-client/nfs"
)

func TestNFSPool(t *testing.T) {
	for _, connections := range []int{1, 4, 8, 16} {
		t.Run(fmt.Sprint(connections), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			server := newNFSServer(connections)
			targets := make([]*client.Target, connections)
			for index := range targets {
				targets[index] = new(client.Target)
			}
			pool := newNFSPool(targets, server)
			var releases []func()
			for index := 0; index < max(1, connections-1); index++ {
				_, release, err := pool.acquire(ctx, true)
				if err != nil {
					t.Fatal(err)
				}
				releases = append(releases, release)
			}
			if connections > 1 {
				target, release, err := pool.acquire(ctx, false)
				if err != nil || target != targets[0] {
					t.Fatalf("metadata reservation: %p %v", target, err)
				}
				releases = append(releases, release)
			}
			blocked, stop := context.WithCancel(ctx)
			result := make(chan error, 1)
			go func() {
				_, release, err := pool.acquire(blocked, false)
				if err == nil {
					release()
				}
				result <- err
			}()
			stop()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("queued cancellation: %v", err)
			}
			other := newNFSPool([]*client.Target{new(client.Target)}, server)
			limited, stopLimit := context.WithTimeout(ctx, time.Millisecond)
			defer stopLimit()
			if _, release, err := other.acquire(limited, false); !errors.Is(err, context.DeadlineExceeded) {
				if err == nil {
					release()
				}
				t.Fatalf("cross-export server limit: %v", err)
			}
			releases[0]()
			target, release, err := pool.acquire(ctx, false)
			if err != nil || target != targets[min(1, connections-1)] {
				t.Fatalf("did not select the available connection: %p %v", target, err)
			}
			releases[0] = release
			for _, release := range releases {
				release()
			}
			var counters nfsOperationCounters
			if err := pool.call(ctx, true, &counters, func(*client.Target) error { return nil }); err != nil {
				t.Fatal(err)
			}
			stats := counters.stats()
			if stats.Attempts != 1 || stats.Active != 0 || stats.MaxActive != 1 || stats.Errors != 0 ||
				stats.QueueNanoseconds == 0 || stats.ServiceNanoseconds == 0 {
				t.Fatalf("invalid call accounting: %+v", stats)
			}
			if err := pool.call(blocked, false, &counters, func(*client.Target) error {
				t.Error("called target after cancellation")
				return nil
			}); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			stats = counters.stats()
			if stats.Attempts != 2 || stats.Calls != 1 || stats.Errors != 1 || stats.Cancellations != 1 || stats.Active != 0 {
				t.Fatalf("invalid cancellation accounting: %+v", stats)
			}
			if len(server.slots) != 0 || len(server.reads) != 0 || len(pool.available)+len(pool.metadata) != connections || len(other.available) != 1 {
				t.Fatal("scheduler leaked admission or connections")
			}
		})
	}
}
