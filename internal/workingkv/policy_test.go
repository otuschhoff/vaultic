package workingkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestM4AtomicMigration(t *testing.T) {
	root := t.TempDir()
	policy, err := NewPolicy(ModeAuto, 1<<20, root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithPolicy(t.Context(), policy)
	store, err := OpenWorking(ctx, root, "migration-")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for number := 0; number < 32; number++ {
		key := []byte(fmt.Sprintf("%04d", number))
		value := bytes.Repeat([]byte{byte(number)}, 8<<10)
		if err := store.Put(ctx, []Entry{{key, value}}); err != nil {
			t.Fatal(err)
		}
		for previous := 0; previous <= number; previous++ {
			actual, found, err := store.Get(ctx, []byte(fmt.Sprintf("%04d", previous)))
			if err != nil || !found || !bytes.Equal(actual, bytes.Repeat([]byte{byte(previous)}, 8<<10)) {
				t.Fatal("partial handoff", previous, err)
			}
		}
	}
	if store.Backend() != "bbolt" {
		t.Fatal("no migration")
	}
	rows, err := store.Scan(ctx, nil, nil, 1024)
	if err != nil || len(rows) != 32 {
		t.Fatal("migration lost records", len(rows), err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if policy.Budget().Used != 0 {
		t.Fatal("migration reservation leak", policy.Budget())
	}
}

func TestM4MigrationFailurePreservesRAM(t *testing.T) {
	for _, stage := range []string{"open", "copy", "verify", "handoff"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			policy, _ := NewPolicy(ModeAuto, 512<<10, root)
			ctx := WithPolicy(t.Context(), policy)
			store, err := OpenWorking(ctx, root, "failure-")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.Put(ctx, []Entry{{[]byte("key"), []byte("original")}}); err != nil {
				t.Fatal(err)
			}
			cause := errors.New("injected " + stage)
			store.fault = func(current string) error {
				if current == stage {
					return cause
				}
				return nil
			}
			store.mutex.Lock()
			err = store.migrate(ctx)
			store.mutex.Unlock()
			if !errors.Is(err, cause) || !errors.Is(err, ErrWorkingMigrationFailed) || store.Backend() != "ram" {
				t.Fatal("invalid failed handoff", err)
			}
			value, found, err := store.Get(ctx, []byte("key"))
			if err != nil || !found || string(value) != "original" {
				t.Fatal("lost valid original", err)
			}
			files, _ := os.ReadDir(root)
			if len(files) != 0 {
				t.Fatal("partial store leaked")
			}
		})
	}
}

func TestM4SharedMigrationHeadroom(t *testing.T) {
	root := t.TempDir()
	policy, _ := NewPolicy(ModeAuto, 1<<20, root)
	ctx := WithPolicy(t.Context(), policy)
	left, err := OpenWorking(ctx, root, "left-")
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := OpenWorking(ctx, root, "right-")
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	if err := right.Put(ctx, []Entry{{[]byte("key"), []byte("right")}}); err != nil {
		t.Fatal(err)
	}
	left.mutex.Lock()
	err = left.migrate(ctx)
	left.mutex.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	newcomer, err := OpenWorking(ctx, root, "new-")
	if err != nil {
		t.Fatal(err)
	}
	if newcomer.Backend() != "bbolt" {
		t.Fatal("one-way fallback not inherited")
	}
	if err := newcomer.Close(); err != nil {
		t.Fatal(err)
	}
	if policy.headroom == 0 {
		t.Fatal("sibling close released active migration reserve")
	}
	value, found, err := right.Get(ctx, []byte("key"))
	if err != nil || !found || string(value) != "right" || right.Backend() != "bbolt" {
		t.Fatal("sibling handoff", err)
	}
	left.Close()
	right.Close()
	if policy.Budget().Used != 0 {
		t.Fatal("shared reservation leak", policy.Budget())
	}
}

func TestM4ByteGrammarAndResources(t *testing.T) {
	for _, text := range []string{"0", "-1", "+1", "1G", "1.5GiB", "1gb", " 1B", "1B ", "18446744073709551615TB"} {
		if _, err := ParseBytes(text); !errors.Is(err, ErrWorkingPolicyConflict) {
			t.Fatal("accepted invalid quantity", text, err)
		}
	}
	for text, want := range map[string]uint64{"1": 1, "12B": 12, "1KB": 1000, "1KiB": 1024, "2GiB": 2 << 30} {
		got, err := ParseBytes(text)
		if err != nil || got != want {
			t.Fatal(text, got, err)
		}
	}
	resources := Resources{HostLimit: 4 << 30, HostAvailable: 3 << 30, CgroupLimit: 2 << 30, CgroupUsed: 512 << 20, RSS: 64 << 20, RSSKnown: true, CgroupKnown: true}
	policy, err := Resolve(Configuration{Mode: "ram", RAMLimit: "4GiB"}, resources)
	if err != nil {
		t.Fatal(err)
	}
	event := policy.Snapshot("test", "test")
	if event.Resolution.Effective >= event.Resolution.Configured || event.Resolution.Headroom < 128<<20 {
		t.Fatal("unsafe startup", event)
	}
	initial := policy.Budget().Limit
	resources.CgroupLimit = 1 << 30
	if err := policy.Observe(resources); err != nil {
		t.Fatal(err)
	}
	if policy.Budget().Limit >= initial {
		t.Fatal("lowered cgroup not enforced")
	}
	resources.CgroupLimit = 4 << 30
	if err := policy.Observe(resources); err != nil || policy.Budget().Limit >= initial {
		t.Fatal("limit increased automatically", err)
	}
	resources.CgroupLimit = 128 << 20
	if _, err := Resolve(Configuration{Mode: "ram", RAMLimit: "1GiB"}, resources); !errors.Is(err, ErrWorkingMemoryLimitExceeded) {
		t.Fatal("unsafe headroom accepted", err)
	}
}

func TestM4BoundedPressureAndStop(t *testing.T) {
	policy, _ := NewPolicy(ModeRAM, 1<<20, "")
	policy.configured = 1 << 20
	resources := Resources{HostLimit: 1 << 30, HostAvailable: 1 << 30, RSSKnown: true}
	if err := policy.Reserve(900 << 10); err != nil {
		t.Fatal(err)
	}
	evicted := 0
	remove := policy.RegisterEvictor(func() { evicted++ })
	defer remove()
	for range 100 {
		if err := policy.Observe(resources); err != nil {
			t.Fatal(err)
		}
	}
	if evicted == 0 || policy.Snapshot("test", "test").Events > 4 {
		t.Fatal("missing/unbounded pressure handling")
	}
	resources.HostAvailable = 1
	if err := policy.Observe(resources); err == nil {
		t.Fatal("unsafe pressure did not stop")
	}
	if err := policy.Reserve(1); !errors.Is(err, ErrWorkingMemoryLimitExceeded) {
		t.Fatal("admission continued", err)
	}
	policy.Release(900 << 10)
}

func TestM4AggregateDiskBudget(t *testing.T) {
	root := t.TempDir()
	policy, _ := NewPolicy(ModeKV, 2<<20, root)
	policy.disk, _ = NewBudget(96 << 10)
	ctx := WithPolicy(t.Context(), policy)
	left, err := OpenWorking(ctx, root, "left-")
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	_, err = OpenWorking(ctx, root, "right-")
	if !errors.Is(err, ErrWorkingScratchLimitExceeded) || errors.Is(err, ErrWorkingMemoryLimitExceeded) {
		t.Fatal("wrong disk admission classification", err)
	}
	if policy.disk.Snapshot().Peak > policy.disk.Snapshot().Limit {
		t.Fatal("aggregate disk overspent")
	}
	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	if policy.Budget().Used != 0 || policy.disk.Snapshot().Used != 0 {
		t.Fatal("failed constructor leaked reservations")
	}
}

func TestM4ConcurrentMigration(t *testing.T) {
	root := t.TempDir()
	policy, _ := NewPolicy(ModeAuto, 8<<20, root)
	ctx := WithPolicy(t.Context(), policy)
	store, err := OpenWorking(ctx, root, "concurrent-")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var group sync.WaitGroup
	failures := make(chan error, 8)
	for lane := 0; lane < 4; lane++ {
		group.Go(func() {
			for number := 0; number < 160; number++ {
				key := []byte(fmt.Sprintf("%02d/%04d", lane, number))
				value := bytes.Repeat([]byte{byte(lane), byte(number)}, 1024)
				if err := store.Put(ctx, []Entry{{key, value}}); err != nil {
					failures <- err
					return
				}
				got, found, err := store.Get(ctx, key)
				if err != nil || !found || !bytes.Equal(got, value) {
					failures <- fmt.Errorf("concurrent read parity: %w", err)
					return
				}
			}
		})
	}
	for range 4 {
		group.Go(func() {
			for range 160 {
				_, err := store.Scan(ctx, nil, nil, 8)
				if err != nil {
					failures <- err
					return
				}
			}
		})
	}
	group.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	rows, err := store.Scan(ctx, nil, nil, 1024)
	if err != nil || len(rows) != 640 {
		t.Fatal("lost/duplicated concurrent entries", len(rows), err)
	}
	if store.Backend() != "bbolt" {
		t.Fatal("concurrent migration was not exercised")
	}
	if policy.Budget().Peak > policy.Budget().Limit {
		t.Fatal("shared memory overspent")
	}
	store.Close()
	if policy.Budget().Used != 0 || policy.disk.Snapshot().Used != 0 {
		t.Fatal("concurrent handoff leaked")
	}
}

func TestM4AuthenticatedMigrationRejectsCorruption(t *testing.T) {
	root := t.TempDir()
	policy, _ := NewPolicy(ModeAuto, 4<<20, root)
	ctx := WithPolicy(t.Context(), policy)
	store, err := OpenSecureMap(ctx, root, "authenticated-")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key := []byte("protected")
	if err := store.PutValue(ctx, key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	encoded, _, err := store.WorkingStore.Get(ctx, store.token(key))
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 1
	if err := store.WorkingStore.Put(ctx, []Entry{{store.token(key), encoded}}); err != nil {
		t.Fatal(err)
	}
	store.WorkingStore.mutex.Lock()
	err = store.WorkingStore.migrate(ctx)
	store.WorkingStore.mutex.Unlock()
	if !errors.Is(err, ErrWorkingStateCorrupt) || !errors.Is(err, ErrWorkingMigrationFailed) || store.Backend() != "ram" {
		t.Fatal("corrupt handoff accepted", err)
	}
	files, _ := os.ReadDir(root)
	if len(files) != 0 {
		t.Fatal("corrupt target survived")
	}
}

func TestM4FaultCausesAndCleanup(t *testing.T) {
	for _, cause := range []error{syscall.ENOSPC, context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			root := t.TempDir()
			policy, _ := NewPolicy(ModeAuto, 1<<20, root)
			ctx := WithPolicy(t.Context(), policy)
			store, err := OpenWorking(ctx, root, "fault-")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.Put(ctx, []Entry{{[]byte("key"), []byte("original")}}); err != nil {
				t.Fatal(err)
			}
			store.fault = func(stage string) error {
				if stage == "copy" {
					return cause
				}
				return nil
			}
			store.mutex.Lock()
			err = store.migrate(ctx)
			store.mutex.Unlock()
			if !errors.Is(err, cause) || store.Backend() != "ram" {
				t.Fatal("lost native cause/original backend", err)
			}
			store.fault = nil
			value, found, err := store.Get(ctx, []byte("key"))
			if err != nil || !found || string(value) != "original" {
				t.Fatal("original invalid", err)
			}
		})
	}
	t.Run("cleanup", func(t *testing.T) {
		root := t.TempDir()
		policy, _ := NewPolicy(ModeAuto, 1<<20, root)
		ctx := WithPolicy(t.Context(), policy)
		store, _ := OpenWorking(ctx, root, "cleanup-")
		defer store.Close()
		_ = store.Put(ctx, []Entry{{[]byte("key"), []byte("value")}})
		cleanupCause := errors.New("injected cleanup failure")
		store.fault = func(stage string) error {
			if stage == "copy" {
				return syscall.ENOSPC
			}
			if stage == "cleanup" {
				return cleanupCause
			}
			return nil
		}
		store.mutex.Lock()
		err := store.migrate(ctx)
		store.mutex.Unlock()
		if !errors.Is(err, syscall.ENOSPC) || !errors.Is(err, cleanupCause) || store.Backend() != "ram" {
			t.Fatal("cleanup cause hidden", err)
		}
		if policy.disk.Snapshot().Used == 0 {
			t.Fatal("failed removal released live scratch reservation")
		}
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
		policy.ReleaseDisk(policy.disk.Snapshot().Used)
	})
}

func TestM4CancelledHandoffWait(t *testing.T) {
	root := t.TempDir()
	policy, _ := NewPolicy(ModeAuto, 1<<20, root)
	ctx := WithPolicy(t.Context(), policy)
	store, _ := OpenWorking(ctx, root, "wait-")
	defer store.Close()
	_ = store.Put(ctx, []Entry{{[]byte("key"), []byte("value")}})
	store.mutex.Lock()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	finished := make(chan error, 1)
	go func() { _, _, err := store.Get(cancelled, []byte("key")); finished <- err }()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		store.mutex.Unlock()
		t.Fatal("cancelled waiter joined handoff", err)
	}
	store.mutex.Unlock()
}

func TestM4InvalidRequestsNeverFallback(t *testing.T) {
	root := t.TempDir()
	policy, _ := NewPolicy(ModeAuto, 1<<20, root)
	ctx := WithPolicy(t.Context(), policy)
	store, _ := OpenWorking(ctx, root, "invalid-")
	defer store.Close()
	policy.pressure = true
	if _, _, err := store.Get(ctx, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.Scan(ctx, nil, nil, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := store.Put(ctx, []Entry{{nil, []byte("invalid")}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(root)
	if len(files) != 0 || store.Backend() != "ram" {
		t.Fatal("invalid request migrated")
	}
}

func TestM4PressureHysteresis(t *testing.T) {
	policy, _ := NewPolicy(ModeRAM, 1000, "")
	resources := Resources{HostLimit: 2 << 30, HostAvailable: 1 << 30, RSSKnown: true}
	if err := policy.Reserve(900); err != nil {
		t.Fatal(err)
	}
	if err := policy.Observe(resources); err != nil {
		t.Fatal(err)
	}
	policy.Release(100)
	if err := policy.Observe(resources); err != nil || !policy.pressure {
		t.Fatal("pressure cleared before hysteresis", err)
	}
	policy.Release(200)
	if err := policy.Observe(resources); err != nil || policy.pressure {
		t.Fatal("pressure did not clear below low watermark", err)
	}
	policy.Release(600)
}

func TestM4IsolatedCgroupPressure(t *testing.T) {
	if os.Getenv("VAULTIC_PHASE35_M4_CGROUP") != "1" {
		t.Skip("isolated cgroup harness only")
	}
	resources, err := ReadResources()
	if err != nil {
		t.Fatal(err)
	}
	if !resources.CgroupKnown || resources.CgroupLimit > 512<<20 {
		t.Fatal("missing isolated cap", resources)
	}
	policy, err := Resolve(Configuration{Mode: "ram", RAMLimit: "2GiB", Reserve: "64MiB"}, resources)
	if err != nil {
		t.Fatal(err)
	}
	initial := policy.Budget().Limit
	if initial >= 2<<30 {
		t.Fatal("unsafe request not lowered")
	}
	if err := policy.Reserve(32 << 20); err != nil {
		t.Fatal(err)
	}
	managed := make([]byte, 32<<20)
	unrelated := make([]byte, 64<<20)
	for offset := 0; offset < len(managed); offset += 4096 {
		managed[offset] = 1
	}
	for offset := 0; offset < len(unrelated); offset += 4096 {
		unrelated[offset] = 1
	}
	resources, err = ReadResources()
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Observe(resources); err != nil {
		t.Fatal(err)
	}
	if policy.Budget().Limit >= initial {
		t.Fatal("external pressure failed to lower allowance", initial, policy.Budget())
	}
	if err := policy.Reserve(policy.Budget().Limit); !errors.Is(err, ErrWorkingMemoryLimitExceeded) {
		t.Fatal("over-limit admission succeeded", err)
	}
	t.Logf("M4_CGROUP limit=%d initial_effective=%d lower_effective=%d rss=%d runtime=%d", resources.CgroupLimit, initial, policy.Budget().Limit, resources.RSS, resources.RuntimeBytes)
	runtime.KeepAlive(managed)
	runtime.KeepAlive(unrelated)
	policy.Release(32 << 20)
	if policy.Budget().Used != 0 {
		t.Fatal("managed allocation leaked")
	}
}

func TestM4CancelledAggregateMigrationWait(t *testing.T) {
	policy, _ := NewPolicy(ModeAuto, 1<<20, t.TempDir())
	ctx := WithPolicy(t.Context(), policy)
	store, err := OpenWorking(ctx, "", "wait-")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	policy.migration.Lock()
	defer policy.migration.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	finished := make(chan error, 1)
	go func() { finished <- store.migrate(ctx) }()
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled aggregate migration wait blocked")
	}
}

func TestWorkingPolicyStore(t *testing.T) {
	for _, mode := range []Mode{ModeRAM, ModeKV} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			policy, err := NewPolicy(mode, 2<<20, root)
			if err != nil {
				t.Fatal(err)
			}
			ctx := WithPolicy(context.Background(), policy)
			store, err := OpenWorking(ctx, filepath.Join(root, "absent"), "fixture-")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, []Entry{{[]byte("key"), []byte("value")}}); err != nil {
				t.Fatal(err)
			}
			if mode == ModeRAM {
				files, err := os.ReadDir(root)
				if err != nil || len(files) != 0 {
					t.Fatal("RAM created files", err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if policy.Budget().Used != 0 {
				t.Fatal("policy leaked capacity")
			}
			files, err := os.ReadDir(root)
			if err != nil || len(files) != 0 {
				t.Fatal("working files survived close", err)
			}
		})
	}
}

func TestM3SecureMapParity(t *testing.T) {
	for _, mode := range []Mode{ModeRAM, ModeKV, ModeAuto} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			policy, _ := NewPolicy(mode, 16<<20, root)
			ctx := WithPolicy(t.Context(), policy)
			store, err := OpenSecureMap(ctx, root, "vaultic-secure-")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			key := []byte("sensitive-directory\x00\xff")
			for _, value := range [][]byte{nil, []byte("sensitive-child-name"), bytes.Repeat([]byte("sensitive-child-name"), 100000)} {
				if err := store.PutValue(ctx, key, value); err != nil {
					t.Fatal(err)
				}
				got, found, err := store.GetValue(ctx, key)
				if err != nil || !found || !bytes.Equal(got, value) {
					t.Fatal("secure value mismatch", err)
				}
			}
			if mode == ModeRAM {
				entries, _ := os.ReadDir(root)
				if len(entries) != 0 {
					t.Fatal("RAM used scratch")
				}
			} else {
				err := filepath.WalkDir(store.Path(), func(path string, entry os.DirEntry, err error) error {
					if err != nil || entry.IsDir() {
						return err
					}
					raw, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					if bytes.Contains(raw, []byte("sensitive-directory")) || bytes.Contains(raw, []byte("sensitive-child-name")) {
						t.Error("plaintext working state on disk")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			rows, err := store.WorkingStore.Scan(ctx, nil, nil, 1024)
			if err != nil {
				t.Fatal(err)
			}
			rootKey := store.token(key)
			domain := store.domain
			store.domain = []byte("vaultic-working/v2/other-kind")
			if value, _, err := store.GetValue(ctx, key); err == nil || value != nil {
				t.Fatal("wrong kind/version accepted")
			}
			store.domain = domain
			for _, row := range rows {
				if bytes.Equal(row.Key, rootKey) {
					row.Value[len(row.Value)-1] ^= 1
					if err := store.WorkingStore.Put(ctx, []Entry{row}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if value, _, err := store.GetValue(ctx, key); err == nil || value != nil {
				t.Fatal("tampered root accepted")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if policy.Budget().Used != 0 {
				t.Fatal("secure map leaked")
			}
		})
	}
}
