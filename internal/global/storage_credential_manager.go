package global

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/topology"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var errStorageCredentialExpired = errors.New("storage credential expired")

type renewableBackend struct {
	mu            sync.RWMutex
	current       backend.Backend
	validUntil    time.Time
	writeDeadline time.Time
	closed        bool
}

func newRenewableBackend(current backend.Backend, validUntil time.Time, ttl time.Duration) *renewableBackend {
	return &renewableBackend{
		current: current, validUntil: validUntil,
		writeDeadline: validUntil.Add(-storageOperationSafetyMargin(ttl)),
	}
}

func storageOperationSafetyMargin(ttl time.Duration) time.Duration {
	return min(5*time.Minute, ttl/10)
}

func (r *renewableBackend) begin(write bool) (backend.Backend, error) {
	r.mu.RLock()
	deadline := r.validUntil
	if write {
		deadline = r.writeDeadline
	}
	if r.closed || !time.Now().Before(deadline) {
		r.mu.RUnlock()
		return nil, errStorageCredentialExpired
	}
	return r.current, nil
}

func (r *renewableBackend) end() { r.mu.RUnlock() }

func (r *renewableBackend) swap(next backend.Backend, validUntil time.Time, ttl time.Duration) (backend.Backend, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return next, errors.New("storage backend is closed")
	}
	previous := r.current
	r.current = next
	r.validUntil = validUntil
	r.writeDeadline = validUntil.Add(-storageOperationSafetyMargin(ttl))
	return previous, nil
}

func (r *renewableBackend) Properties() backend.Properties {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.current.Properties()
}

func (r *renewableBackend) Hasher() hash.Hash {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.current.Hasher()
}

func (r *renewableBackend) Remove(ctx context.Context, handle backend.Handle) error {
	current, err := r.begin(true)
	if err != nil {
		return err
	}
	defer r.end()
	return current.Remove(ctx, handle)
}

func (r *renewableBackend) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.current.Close()
}

func (r *renewableBackend) Save(ctx context.Context, handle backend.Handle, reader backend.RewindReader) error {
	current, err := r.begin(true)
	if err != nil {
		return err
	}
	defer r.end()
	return current.Save(ctx, handle, reader)
}

func (r *renewableBackend) Load(ctx context.Context, handle backend.Handle, length int, offset int64, fn func(io.Reader) error) error {
	current, err := r.begin(false)
	if err != nil {
		return err
	}
	defer r.end()
	return current.Load(ctx, handle, length, offset, fn)
}

func (r *renewableBackend) Stat(ctx context.Context, handle backend.Handle) (backend.FileInfo, error) {
	current, err := r.begin(false)
	if err != nil {
		return backend.FileInfo{}, err
	}
	defer r.end()
	return current.Stat(ctx, handle)
}

func (r *renewableBackend) List(ctx context.Context, fileType backend.FileType, fn func(backend.FileInfo) error) error {
	current, err := r.begin(false)
	if err != nil {
		return err
	}
	defer r.end()
	return current.List(ctx, fileType, fn)
}

func (r *renewableBackend) IsNotExist(err error) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.current.IsNotExist(err)
}

func (r *renewableBackend) IsPermanentError(err error) bool {
	if errors.Is(err, errStorageCredentialExpired) {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.current.IsPermanentError(err)
}

func (r *renewableBackend) Delete(ctx context.Context) error {
	current, err := r.begin(true)
	if err != nil {
		return err
	}
	defer r.end()
	return current.Delete(ctx)
}

func (r *renewableBackend) Warmup(ctx context.Context, handles []backend.Handle) ([]backend.Handle, error) {
	current, err := r.begin(false)
	if err != nil {
		return nil, err
	}
	defer r.end()
	return current.Warmup(ctx, handles)
}

func (r *renewableBackend) WarmupWait(ctx context.Context, handles []backend.Handle) error {
	current, err := r.begin(false)
	if err != nil {
		return err
	}
	defer r.end()
	return current.WarmupWait(ctx, handles)
}

type storageCredentialSlot struct {
	backend     *renewableBackend
	declared    topology.PackBackend
	target      string
	tier        string
	leaseID     string
	expiresAt   time.Time
	nextAttempt time.Time
	failures    uint
}

type storageCredentialManager struct {
	client      *indexbroker.Client
	options     Options
	printer     vaultic.Printer
	renewMargin time.Duration
	outageGrace time.Duration
	slots       []*storageCredentialSlot
	cancel      context.CancelFunc
	done        chan struct{}
	closeOnce   sync.Once
}

func newStorageCredentialManager(client *indexbroker.Client, options Options, printer vaultic.Printer) (*storageCredentialManager, error) {
	ttl, margin, grace, err := storageCredentialLifetime(options)
	if err != nil {
		return nil, err
	}
	options.StorageTokenTTL = ttl
	return &storageCredentialManager{
		client: client, options: options, printer: printer, renewMargin: margin,
		outageGrace: grace, done: make(chan struct{}),
	}, nil
}

func (m *storageCredentialManager) add(slot *storageCredentialSlot) {
	slot.nextAttempt = m.nextRenewal(slot.expiresAt, time.Now())
	m.slots = append(m.slots, slot)
}

func (m *storageCredentialManager) nextRenewal(expiresAt, contactedAt time.Time) time.Time {
	expiryRenewal := expiresAt.Add(-m.renewMargin)
	graceRenewal := contactedAt.Add(m.outageGrace / 2)
	if expiryRenewal.Before(graceRenewal) {
		return expiryRenewal
	}
	return graceRenewal
}

func (m *storageCredentialManager) start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go m.run(ctx)
}

func (m *storageCredentialManager) run(ctx context.Context) {
	defer close(m.done)
	for {
		next := time.Now().Add(time.Hour)
		for _, slot := range m.slots {
			if slot.nextAttempt.Before(next) {
				next = slot.nextAttempt
			}
		}
		timer := time.NewTimer(max(time.Until(next), 0))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		now := time.Now()
		for _, slot := range m.slots {
			if !slot.nextAttempt.After(now) {
				m.renew(ctx, slot)
			}
		}
	}
}

func (m *storageCredentialManager) renew(ctx context.Context, slot *storageCredentialSlot) {
	observability.EmitBestEffort(ctx, storageCredentialEvent(observability.Info, "storage credential renewal attempted", slot, nil))
	leased, err := leaseTopologyStorageCredential(ctx, m.client, m.options, slot.target, slot.tier)
	if err != nil {
		m.reconnect(ctx)
	}
	if err == nil {
		var next backend.Backend
		next, _, err = openStructuredBackend(ctx, m.options, m.printer, slot.declared, leased.credential)
		if err == nil {
			validUntil := leased.expiresAt
			if graceUntil := time.Now().Add(m.outageGrace); graceUntil.Before(validUntil) {
				validUntil = graceUntil
			}
			var previous backend.Backend
			previous, err = slot.backend.swap(next, validUntil, m.options.StorageTokenTTL)
			if err == nil {
				oldLeaseID := slot.leaseID
				slot.leaseID = leased.leaseID
				slot.expiresAt = leased.expiresAt
				slot.nextAttempt = m.nextRenewal(leased.expiresAt, time.Now())
				slot.failures = 0
				_ = previous.Close()                       // The replacement is active; stale client cleanup is best effort.
				_ = m.client.ReleaseLease(ctx, oldLeaseID) // The replacement lease is active; old lease cleanup is best effort.
				observability.EmitBestEffort(ctx, storageCredentialEvent(observability.Notice, "storage credential renewed", slot, &leased))
				return
			}
			_ = next.Close() // Preserve the swap error; unused replacement cleanup is best effort.
		}
		_ = m.client.ReleaseLease(ctx, leased.leaseID) // Preserve the renewal error; rejected lease cleanup is best effort.
	}
	slot.failures++
	backoff := min(time.Second<<min(slot.failures, 6), time.Minute)
	jitter := time.Duration(rand.Int64N(int64(max(backoff/5, time.Millisecond))))
	slot.nextAttempt = time.Now().Add(backoff - backoff/10 + jitter)
	message := "storage credential renewal failed"
	fields := map[string]any{"storage_target": slot.target, "storage_tier": slot.tier}
	var brokerError *indexbroker.RequestError
	if errors.As(err, &brokerError) && brokerError.Code == "locked" {
		message = "storage credential renewal locked"
		fields["remaining_validity_seconds"] = max(int64(time.Until(slot.expiresAt).Seconds()), 0)
	}
	observability.EmitBestEffort(ctx, observability.Event{
		Severity: observability.Warning, Category: observability.CategoryAuth,
		Component: "storage-credential-manager", Message: message, Fields: fields,
	})
}

func (m *storageCredentialManager) reconnect(ctx context.Context) {
	replacement, err := indexbroker.Dial(ctx, m.options.KeyBrokerSocket)
	if err != nil {
		return
	}
	previous := m.client
	m.client = replacement
	_ = previous.Close() // The replacement connection is active; stale transport cleanup is best effort.
}

func storageCredentialEvent(severity observability.Severity, message string, slot *storageCredentialSlot, leased *leasedStorageCredential) observability.Event {
	fields := map[string]any{"storage_target": slot.target, "storage_tier": slot.tier}
	if leased != nil {
		fields["lease_id"] = leased.leaseID
		fields["expires_at"] = leased.expiresAt.UTC().Format(time.RFC3339)
		fields["credential_source"] = leased.source
	}
	return observability.Event{Severity: severity, Category: observability.CategoryAuth, Component: "storage-credential-manager", Message: message, Fields: fields}
}

func (m *storageCredentialManager) Close() error {
	var result error
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
			<-m.done
		}
		for _, slot := range m.slots {
			if err := m.client.ReleaseLease(context.Background(), slot.leaseID); err != nil {
				result = errors.Join(result, fmt.Errorf("release storage credential lease: %w", err))
			}
		}
		result = errors.Join(result, m.client.Close())
	})
	return result
}
