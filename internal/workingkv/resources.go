package workingkv

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Configuration struct {
	Mode       string
	RAMLimit   string
	Scratch    string
	DiskBudget string
	Reserve    string
}

type Resources struct {
	HostLimit      uint64 `json:"host_limit"`
	HostAvailable  uint64 `json:"host_available"`
	CgroupLimit    uint64 `json:"cgroup_limit"`
	CgroupUsed     uint64 `json:"cgroup_used"`
	RSS            uint64 `json:"rss"`
	OwnedFileCache uint64 `json:"owned_file_cache_bytes"`
	RuntimeBytes   uint64 `json:"runtime_bytes"`
	RSSKnown       bool   `json:"rss_known"`
	CgroupKnown    bool   `json:"cgroup_known"`
}

type Resolution struct {
	Configured uint64    `json:"configured_limit"`
	Effective  uint64    `json:"effective_limit"`
	Envelope   uint64    `json:"envelope"`
	Reserve    uint64    `json:"co_resident_reserve"`
	Headroom   uint64    `json:"safety_headroom"`
	Resources  Resources `json:"resources"`
	Unmanaged  uint64    `json:"unmanaged_observed"`
}

type PolicyEvent struct {
	MessageType       string         `json:"message_type"`
	Action            string         `json:"action"`
	Cause             string         `json:"cause"`
	Requested         Mode           `json:"requested_mode"`
	Effective         Mode           `json:"effective_mode"`
	Resolution        Resolution     `json:"resolution"`
	Memory            BudgetSnapshot `json:"memory"`
	Disk              BudgetSnapshot `json:"disk"`
	MigrationReserved uint64         `json:"migration_reserved"`
	Events            uint64         `json:"events"`
}

func ParseBytes(text string) (uint64, error) {
	if text == "" {
		return 0, ErrWorkingPolicyConflict
	}
	end := 0
	for end < len(text) && text[end] >= '0' && text[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, ErrWorkingPolicyConflict
	}
	multipliers := map[string]uint64{"": 1, "B": 1, "KB": 1000, "MB": 1000000, "GB": 1000000000, "TB": 1000000000000, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40}
	multiplier, ok := multipliers[text[end:]]
	if !ok {
		return 0, ErrWorkingPolicyConflict
	}
	value, err := strconv.ParseUint(text[:end], 10, 64)
	if err != nil || value == 0 || value > ^uint64(0)/multiplier {
		return 0, ErrWorkingPolicyConflict
	}
	return value * multiplier, nil
}

func envelope(resources Resources, reserve uint64) (uint64, uint64, error) {
	if resources.HostLimit == 0 || resources.HostAvailable == 0 || !resources.RSSKnown {
		return 0, 0, errors.Join(ErrWorkingPolicyConflict, errors.New("working resource envelope unavailable"))
	}
	available := resources.HostAvailable
	if resources.RSS > ^uint64(0)-available {
		return 0, 0, ErrWorkingPolicyConflict
	}
	available = min(resources.HostLimit, available+resources.RSS)
	if resources.CgroupKnown {
		resident := max(resources.RSS, resources.OwnedFileCache)
		free := uint64(0)
		if resources.CgroupUsed < resources.CgroupLimit {
			free = resources.CgroupLimit - resources.CgroupUsed
		}
		if resident > ^uint64(0)-free {
			return 0, 0, ErrWorkingPolicyConflict
		}
		available = min(available, resources.CgroupLimit, free+resident)
	}
	if reserve >= available {
		return 0, 0, ErrWorkingMemoryLimitExceeded
	}
	available -= reserve
	headroom := max(uint64(128<<20), available/5)
	if headroom >= available {
		return 0, headroom, ErrWorkingMemoryLimitExceeded
	}
	return available, headroom, nil
}

func Resolve(configuration Configuration, resources Resources) (*Policy, error) {
	if configuration.Mode == "" {
		if configuration.RAMLimit != "" || configuration.Scratch != "" || configuration.DiskBudget != "" || configuration.Reserve != "" {
			return nil, ErrWorkingPolicyConflict
		}
		return nil, nil
	}
	mode := Mode(configuration.Mode)
	if mode != ModeRAM && mode != ModeKV && mode != ModeAuto {
		return nil, ErrWorkingPolicyConflict
	}
	if mode != ModeRAM && configuration.Scratch == "" {
		return nil, errors.Join(ErrWorkingPolicyConflict, errors.New("KV/auto requires --working-memory-scratch"))
	}
	parse := func(text string, fallback uint64) (uint64, error) {
		if text == "" {
			return fallback, nil
		}
		return ParseBytes(text)
	}
	requested, err := parse(configuration.RAMLimit, 256<<20)
	if err != nil {
		return nil, err
	}
	reserve, err := parse(configuration.Reserve, 128<<20)
	if err != nil {
		return nil, err
	}
	disk, err := parse(configuration.DiskBudget, 8<<30)
	if err != nil {
		return nil, err
	}
	unmanaged := max(resources.RSS, resources.RuntimeBytes)
	if unmanaged > ^uint64(0)-reserve {
		return nil, ErrWorkingMemoryLimitExceeded
	}
	capacity, headroom, err := envelope(resources, reserve+unmanaged)
	if err != nil {
		return nil, err
	}
	effective := min(requested, capacity-headroom)
	if effective < 8<<20 {
		return nil, &MemoryLimitError{effective, 0, 8 << 20}
	}
	policy, err := NewPolicy(mode, effective, configuration.Scratch)
	if err != nil {
		return nil, err
	}
	policy.disk, _ = NewBudget(disk)
	policy.configured = requested
	policy.resolution = Resolution{Configured: requested, Effective: effective, Envelope: capacity, Reserve: reserve, Headroom: headroom, Resources: resources, Unmanaged: unmanaged}
	return policy, nil
}

func (policy *Policy) Snapshot(action, cause string) PolicyEvent {
	policy.mutex.Lock()
	defer policy.mutex.Unlock()
	return policy.snapshotLocked(action, cause)
}

func (policy *Policy) snapshotLocked(action, cause string) PolicyEvent {
	effective := policy.mode
	if policy.mode == ModeAuto {
		effective = ModeRAM
		if policy.fallback {
			effective = ModeKV
			if policy.ramStores > 0 {
				effective = Mode("mixed")
			}
		}
	}
	resolution := policy.resolution
	resolution.Configured = policy.configured
	resolution.Effective = policy.Budget().Limit
	return PolicyEvent{"working_memory", action, cause, policy.mode, effective, resolution, policy.Budget(), policy.disk.Snapshot(), policy.headroom, policy.events}
}

func (policy *Policy) emit(action, cause string, bit uint32) {
	policy.mutex.Lock()
	if policy.actions&bit != 0 || policy.events >= 32 {
		policy.mutex.Unlock()
		return
	}
	policy.actions |= bit
	policy.events++
	event := policy.snapshotLocked(action, cause)
	report := policy.report
	policy.mutex.Unlock()
	if report != nil {
		report(event)
	}
}

func (policy *Policy) RegisterEvictor(evict func()) func() {
	policy.mutex.Lock()
	defer policy.mutex.Unlock()
	for index := range policy.evictors {
		if policy.evictors[index] == nil {
			policy.evictors[index] = evict
			return func() { policy.mutex.Lock(); policy.evictors[index] = nil; policy.mutex.Unlock() }
		}
	}
	return func() {}
}

func (policy *Policy) applyObservedLimit(limit uint64) bool {
	policy.mutex.Lock()
	defer policy.mutex.Unlock()
	limit = min(limit, policy.ceiling, policy.configured)
	if limit <= policy.Budget().Limit {
		policy.budget.lower(limit)
		policy.recoverySamples, policy.recoveryLimit = 0, 0
		return false
	}
	if policy.recoverySamples == 0 {
		policy.recoveryLimit = limit
	} else {
		policy.recoveryLimit = min(policy.recoveryLimit, limit)
	}
	policy.recoverySamples++
	if policy.recoverySamples < 3 {
		return false
	}
	policy.budget.setLimit(policy.recoveryLimit)
	policy.recoverySamples, policy.recoveryLimit = 0, 0
	return true
}

func (policy *Policy) Observe(resources Resources) error {
	policy.mutex.Lock()
	reserve := policy.resolution.Reserve
	state := policy.Budget()
	managed := state.Used - min(state.Used, policy.headroom)
	policy.mutex.Unlock()
	unmanaged := max(resources.RSS, resources.RuntimeBytes)
	unmanaged -= min(unmanaged, managed)
	if unmanaged > ^uint64(0)-reserve {
		policy.applyObservedLimit(0)
		policy.emit("stop", "resource_unavailable", 1<<0)
		return ErrWorkingMemoryLimitExceeded
	}
	capacity, headroom, err := envelope(resources, reserve+unmanaged)
	if err != nil {
		policy.applyObservedLimit(0)
		policy.emit("stop", "resource_unavailable", 1<<0)
		return err
	}
	recovered := policy.applyObservedLimit(capacity - headroom)
	policy.mutex.Lock()
	policy.resolution.Resources = resources
	policy.resolution.Envelope = capacity
	policy.resolution.Headroom = headroom
	policy.resolution.Unmanaged = unmanaged
	state = policy.Budget()
	warning := state.Used >= state.Limit-state.Limit/4
	var evictors [4]func()
	if warning {
		evictors = policy.evictors
	}
	if state.Used > state.Limit || state.Used >= state.Limit-state.Limit*15/100 {
		policy.pressure = true
	}
	policy.mutex.Unlock()
	if recovered {
		policy.emit("recover", "sustained_resource_recovery", 1<<7)
	}
	if warning {
		policy.emit("warn", "managed_pressure", 1<<1)
		for _, evict := range evictors {
			if evict != nil {
				evict()
			}
		}
		policy.emit("backpressure", "managed_pressure", 1<<2)
	}
	state = policy.Budget()
	policy.mutex.Lock()
	if state.Used >= state.Limit-state.Limit*15/100 {
		policy.pressure = true
	} else if state.Used < state.Limit-state.Limit*30/100 {
		policy.pressure = false
	}
	policy.mutex.Unlock()
	if state.Used > state.Limit {
		policy.emit("stop", "lowered_limit", 1<<3)
		return &MemoryLimitError{state.Limit, state.Used, 0}
	}
	if policy.mode == ModeAuto && warning {
		policy.emit("fallback_pending", "managed_pressure", 1<<4)
	}
	return nil
}

func (policy *Policy) Start(ctx context.Context, probe func() (Resources, error), report func(PolicyEvent)) func() {
	ctx, cancel := context.WithCancel(ctx)
	policy.mutex.Lock()
	policy.report = report
	policy.mutex.Unlock()
	policy.emit("start", "resolved", 1<<5)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				resources, err := probe()
				if err != nil {
					policy.applyObservedLimit(0)
					policy.emit("stop", "resource_unavailable", 1<<0)
					continue
				}
				resources.OwnedFileCache = policy.ownedFileCacheBytes()
				_ = policy.Observe(resources)
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { cancel(); <-done; policy.emit("complete", "closed", 1<<6) }) }
}

func (policy *Policy) trackCacheFile(file *os.File) {
	policy.cacheMutex.Lock()
	defer policy.cacheMutex.Unlock()
	if policy.cacheFiles == nil {
		policy.cacheFiles = make(map[*os.File]struct{})
	}
	policy.cacheFiles[file] = struct{}{}
}

func (policy *Policy) untrackCacheFile(file *os.File) {
	policy.cacheMutex.Lock()
	defer policy.cacheMutex.Unlock()
	delete(policy.cacheFiles, file)
}

func (policy *Policy) ownedFileCacheBytes() uint64 {
	policy.cacheMutex.Lock()
	defer policy.cacheMutex.Unlock()
	var total uint64
	for file := range policy.cacheFiles {
		cached, err := cachedFileBytes(file)
		if err == nil && cached <= ^uint64(0)-total {
			total += cached
		}
	}
	return total
}

func ReadResources() (Resources, error) {
	if runtime.GOOS != "linux" {
		return readPortableResources()
	}
	var result Resources
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return result, errors.Join(ErrWorkingPolicyConflict, errors.New("host resource sampling unavailable"))
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 || fields[2] != "kB" {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || value > ^uint64(0)/1024 {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			result.HostLimit = value * 1024
		case "MemAvailable:":
			result.HostAvailable = value * 1024
		}
	}
	err = errors.Join(scanner.Err(), file.Close())
	if err != nil {
		return result, err
	}
	stat, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return result, err
	}
	fields := strings.Fields(string(stat))
	if len(fields) < 2 {
		return result, ErrWorkingPolicyConflict
	}
	resident, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil || resident > ^uint64(0)/uint64(os.Getpagesize()) {
		return result, ErrWorkingPolicyConflict
	}
	result.RSS = resident * uint64(os.Getpagesize())
	result.RSSKnown = true
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	result.RuntimeBytes = memory.Sys - memory.HeapReleased
	cgroups, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return result, err
	}
	for _, line := range strings.Split(string(cgroups), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		base, limitName, usedName := "/sys/fs/cgroup", "memory.max", "memory.current"
		if parts[1] != "" {
			if !strings.Contains(","+parts[1]+",", ",memory,") {
				continue
			}
			base = "/sys/fs/cgroup/memory"
			limitName = "memory.limit_in_bytes"
			usedName = "memory.usage_in_bytes"
		}
		path := filepath.Join(base, filepath.Clean("/"+parts[2]))
		for {
			limitRaw, readErr := os.ReadFile(filepath.Join(path, limitName))
			if readErr != nil && !os.IsNotExist(readErr) {
				return result, errors.New("cgroup resource sampling unavailable")
			}
			if readErr == nil && strings.TrimSpace(string(limitRaw)) != "max" {
				limit, parseErr := strconv.ParseUint(strings.TrimSpace(string(limitRaw)), 10, 64)
				usedRaw, usedErr := os.ReadFile(filepath.Join(path, usedName))
				used, usedParseErr := strconv.ParseUint(strings.TrimSpace(string(usedRaw)), 10, 64)
				if parseErr != nil || usedErr != nil || usedParseErr != nil {
					return result, errors.New("cgroup resource sampling unavailable")
				}
				free := limit - min(limit, used)
				if result.CgroupKnown {
					free = min(free, result.CgroupLimit-min(result.CgroupLimit, result.CgroupUsed))
					limit = min(limit, result.CgroupLimit)
				}
				result.CgroupKnown = true
				result.CgroupLimit = limit
				result.CgroupUsed = limit - min(limit, free)
			}
			if path == base {
				break
			}
			parent := filepath.Dir(path)
			if parent == path || !strings.HasPrefix(parent, base) {
				break
			}
			path = parent
		}
	}
	if result.HostLimit == 0 || result.HostAvailable == 0 {
		return result, fmt.Errorf("%w: missing host memory observations", ErrWorkingPolicyConflict)
	}
	return result, nil
}

func ValidateScratchParent(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrWorkingPolicyConflict
	}
	return nil
}

type ScratchLimitError struct{ Limit, Used, Requested uint64 }

func (err *ScratchLimitError) Error() string {
	return fmt.Sprintf("working scratch limit exceeded: limit=%d used=%d requested=%d", err.Limit, err.Used, err.Requested)
}
func (err *ScratchLimitError) Is(target error) bool { return target == ErrWorkingScratchLimitExceeded }

func (policy *Policy) ReserveDisk(size uint64) error {
	if err := policy.disk.reserve(size); err != nil {
		var detail *MemoryLimitError
		if errors.As(err, &detail) {
			return &ScratchLimitError{detail.Limit, detail.Used, detail.Requested}
		}
		return err
	}
	return nil
}
func (policy *Policy) ReleaseDisk(size uint64) { policy.disk.release(size) }

func (policy *Policy) Available() uint64 {
	state := policy.Budget()
	if state.Used >= state.Limit {
		return 0
	}
	return state.Limit - state.Used
}

func (policy *Policy) MarkFallback() {
	if policy.mode != ModeAuto {
		return
	}
	policy.mutex.Lock()
	policy.fallback = true
	policy.mutex.Unlock()
	policy.emit("fallback", "scratch_handoff", 1<<7)
}
