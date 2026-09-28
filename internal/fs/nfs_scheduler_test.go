package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	client "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
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
				if err != nil || target != targets[1] {
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
			if err != nil || target != targets[0] {
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

func TestNFSOperationErrorClasses(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		err   error
		class string
	}{
		{"success", nil, ""},
		{"eof", io.EOF, ""},
		{"cancelled", context.Canceled, "cancelled"},
		{"deadline", context.DeadlineExceeded, "deadline_exceeded"},
		{"missing", client.NFS3Error(client.NFS3ErrNoEnt), "not_found"},
		{"typed-missing", &client.Error{ErrorNum: client.NFS3ErrNoEnt}, "not_found"},
		{"permission", client.NFS3Error(client.NFS3ErrPerm), "permission"},
		{"access", client.NFS3Error(client.NFS3ErrAcces), "permission"},
		{"stale", client.NFS3Error(client.NFS3ErrStale), "stale_handle"},
		{"bad-handle", client.NFS3Error(client.NFS3ErrBadHandle), "stale_handle"},
		{"not-directory", client.NFS3Error(client.NFS3ErrNotDir), "not_directory"},
		{"server-io", client.NFS3Error(client.NFS3ErrIO), "nfs_other"},
		{"closed", net.ErrClosed, "transport"},
		{"short-response", io.ErrUnexpectedEOF, "transport"},
		{"network-timeout", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, "transport"},
		{"unknown", errors.New("unclassified"), "other"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			for _, wrapped := range []bool{false, true} {
				returned := testCase.err
				if wrapped && returned != nil {
					returned = fmt.Errorf("private path: %w", returned)
				}
				pool := newNFSPool([]*client.Target{new(client.Target)}, newNFSServer(1))
				var counters nfsOperationCounters
				if err := pool.call(t.Context(), false, &counters, func(*client.Target) error { return returned }); !errors.Is(err, returned) {
					t.Fatalf("returned error changed: got=%v want=%v", err, returned)
				}
				stats := counters.stats()
				var total uint64
				for class, count := range stats.ErrorClasses {
					var want uint64
					if class == testCase.class {
						want = 1
					}
					if count != want {
						t.Fatalf("class=%s count=%d want=%d", class, count, want)
					}
					total += count
				}
				var cancelled uint64
				if testCase.class == "cancelled" || testCase.class == "deadline_exceeded" {
					cancelled = 1
				}
				if len(stats.ErrorClasses) != int(nfsErrorClassCount) || stats.Errors != total || stats.Cancellations != cancelled ||
					stats.Attempts != 1 || stats.Calls != 1 || stats.Active != 0 {
					t.Fatalf("unexpected outcome accounting: %+v", stats)
				}
			}
		})
	}
}

func TestNFSPoolQueuedErrorClasses(t *testing.T) {
	for _, expired := range []bool{false, true} {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		want := "deadline_exceeded"
		if !expired {
			cancel()
			ctx, cancel = context.WithCancel(t.Context())
			cancel()
			want = "cancelled"
		}
		pool := newNFSPool(nil, newNFSServer(1))
		var counters nfsOperationCounters
		err := pool.call(ctx, false, &counters, func(*client.Target) error {
			t.Fatal("queued failure invoked RPC")
			return nil
		})
		cancel()
		stats := counters.stats()
		if !errors.Is(err, ctx.Err()) || stats.Attempts != 1 || stats.Calls != 0 || stats.Errors != 1 ||
			stats.Cancellations != 1 || stats.ErrorClasses[want] != 1 || stats.Active != 0 {
			t.Fatalf("queued outcome: error=%v stats=%+v", err, stats)
		}
	}
}

func TestNFSPoolRegrowth(t *testing.T) {
	newTarget := func() *client.Target {
		local, remote := net.Pipe()
		connection := rpc.NewClient(t.Context(), local)
		t.Cleanup(func() { connection.Close(); _ = remote.Close() })
		return &client.Target{Client: connection}
	}
	pool := newNFSPool([]*client.Target{newTarget()}, newNFSServer(4))
	pool.add(newTarget())
	pool.add(newTarget())
	pool.add(newTarget())
	_, release, err := pool.acquire(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	if removed := pool.trimIdle(time.Now()); removed != 0 {
		t.Fatal("trimmed a recently used pool")
	}
	if removed := pool.trimIdle(time.Now().Add(time.Minute)); removed != 2 || pool.size.Load() != 2 {
		t.Fatalf("retired=%d size=%d", removed, pool.size.Load())
	}
	release()
	if removed := pool.trimIdle(time.Now().Add(time.Minute)); removed != 1 || pool.size.Load() != 1 {
		t.Fatalf("retired=%d size=%d", removed, pool.size.Load())
	}
	pool.add(newTarget())
	if pool.size.Load() != 2 || len(pool.metadata) != 1 || len(pool.available) != 1 {
		t.Fatal("regrowth did not restore metadata reservation")
	}
}
