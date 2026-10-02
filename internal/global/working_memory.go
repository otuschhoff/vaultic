package global

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/otuschhoff/vaultic/internal/env"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/workingkv"
	"github.com/spf13/pflag"
)

func (options *Options) AddWorkingMemoryFlags(flags *pflag.FlagSet) {
	configuration := &options.WorkingMemory
	for _, field := range []struct {
		target *string
		name   string
	}{{&configuration.Mode, "WORKING_MEMORY_MODE"}, {&configuration.RAMLimit, "WORKING_MEMORY_RAM_LIMIT"}, {&configuration.Scratch, "WORKING_MEMORY_SCRATCH"}, {&configuration.DiskBudget, "WORKING_MEMORY_DISK_BUDGET"}, {&configuration.Reserve, "WORKING_MEMORY_RESERVE"}} {
		if *field.target == "" {
			*field.target = env.Get(field.name)
		}
	}
	flags.StringVar(&configuration.Mode, "working-memory-mode", configuration.Mode, "working state mode: ram, kv, or auto (omission preserves existing behavior)")
	flags.StringVar(&configuration.RAMLimit, "working-memory-ram-limit", configuration.RAMLimit, "aggregate managed memory limit in bytes, decimal KB/MB/GB/TB or binary KiB/MiB/GiB/TiB")
	flags.StringVar(&configuration.Scratch, "working-memory-scratch", configuration.Scratch, "scratch parent required for kv/auto; never authoritative metadata")
	flags.StringVar(&configuration.DiskBudget, "working-memory-disk-budget", configuration.DiskBudget, "aggregate scratch limit including copy and merge overlap")
	flags.StringVar(&configuration.Reserve, "working-memory-reserve", configuration.Reserve, "co-resident/runtime reserve in bytes (default 128MiB, in addition to safety headroom)")
}

func ValidateWorkingMemoryFlags(flags *pflag.FlagSet, policy *workingkv.Policy) error {
	if policy == nil || flags == nil {
		return nil
	}
	explicit := func(flag *pflag.Flag) bool {
		return flag != nil && (flag.Changed || len(flag.Annotations["profile-explicit"]) != 0 || env.Get(strings.ToUpper(strings.ReplaceAll(flag.Name, "-", "_"))) != "")
	}
	for _, name := range []string{"metadata-scratch", "check-temp-dir"} {
		flag := flags.Lookup(name)
		if explicit(flag) && (policy.Mode() == workingkv.ModeRAM || flag.Value.String() != policy.ScratchDirectory()) {
			return fmt.Errorf("%w: --%s conflicts with shared working-memory policy", workingkv.ErrWorkingPolicyConflict, name)
		}
	}
	for _, name := range []string{"metadata-cache-mib", "check-memory", "check-temp-max-bytes"} {
		flag := flags.Lookup(name)
		if !explicit(flag) {
			continue
		}
		value := flag.Value.String()
		if name == "check-memory" && value == "auto" {
			continue
		}
		limit := policy.Budget().Limit
		var requested uint64
		var err error
		if name == "metadata-cache-mib" {
			requested, err = strconv.ParseUint(value, 10, 64)
			if requested > ^uint64(0)/(1<<20) {
				err = workingkv.ErrWorkingPolicyConflict
			}
			requested *= 1 << 20
			limit /= 8
		} else if name == "check-memory" || name == "check-temp-max-bytes" {
			var bytes int64
			bytes, err = ui.ParseBytes(value)
			if bytes <= 0 {
				err = workingkv.ErrWorkingPolicyConflict
			}
			requested = uint64(bytes)
		} else {
			requested, err = workingkv.ParseBytes(value)
		}
		if name == "check-temp-max-bytes" {
			limit = policy.Snapshot("validate", "policy").Disk.Limit
		}
		if err != nil || requested == 0 || requested > limit {
			return fmt.Errorf("%w: --%s exceeds or conflicts with shared allowance", workingkv.ErrWorkingPolicyConflict, name)
		}
	}
	return nil
}

func (options Options) PrepareWorkingMemory(ctx context.Context, flags *pflag.FlagSet, term ui.Terminal) (context.Context, func(), error) {
	noop := func() {}
	if options.WorkingMemory.Mode == "" {
		_, err := workingkv.Resolve(options.WorkingMemory, workingkv.Resources{})
		return ctx, noop, err
	}
	resources, err := workingkv.ReadResources()
	if err != nil {
		return ctx, noop, err
	}
	policy, err := workingkv.Resolve(options.WorkingMemory, resources)
	if err != nil {
		return ctx, noop, err
	}
	if err := ValidateWorkingMemoryFlags(flags, policy); err != nil {
		return ctx, noop, err
	}
	if policy.Mode() != workingkv.ModeRAM {
		if err := workingkv.ValidateScratchParent(policy.ScratchDirectory()); err != nil {
			return ctx, noop, err
		}
	}
	ctx, cancel := context.WithCancelCause(ctx)
	report := func(event workingkv.PolicyEvent) {
		if event.Action == "stop" {
			cancel(&workingkv.MemoryLimitError{Limit: event.Memory.Limit, Used: event.Memory.Used})
		}
		if term == nil {
			return
		}
		if options.JSON {
			term.Print(ui.ToJSONString(event))
			return
		}
		if options.Quiet {
			return
		}
		term.Print(fmt.Sprintf("working memory: mode=%s effective=%s configured=%d effective_limit=%d reserved=%d disk_reserved=%d action=%s cause=%s", event.Requested, event.Effective, event.Resolution.Configured, event.Memory.Limit, event.Memory.Used, event.Disk.Used, event.Action, event.Cause))
	}
	stop := policy.Start(ctx, workingkv.ReadResources, report)
	return workingkv.WithPolicy(ctx, policy), func() { stop(); cancel(nil) }, nil
}

func WorkingMemoryError(err error, contexts ...context.Context) error {
	for _, ctx := range contexts {
		if cause := context.Cause(ctx); cause != nil {
			err = errors.Join(err, cause)
		}
	}
	if err == nil {
		return nil
	}
	for _, ctx := range contexts {
		if policy := workingkv.PolicyFrom(ctx); policy != nil && (errors.Is(err, workingkv.ErrWorkingMemoryLimitExceeded) || errors.Is(err, workingkv.ErrWorkingScratchLimitExceeded) || errors.Is(err, workingkv.ErrWorkingMigrationFailed) || errors.Is(err, workingkv.ErrWorkingStateCorrupt)) {
			event := policy.Snapshot("error", "operation")
			err = fmt.Errorf("%w; working_state=run requested_mode=%s effective_mode=%s configured_limit=%d effective_limit=%d required_capacity_reported_in_cause", err, event.Requested, event.Effective, event.Resolution.Configured, event.Memory.Limit)
			break
		}
	}
	switch {
	case errors.Is(err, workingkv.ErrWorkingScratchLimitExceeded):
		return fmt.Errorf("%w; increase --working-memory-disk-budget or choose a scratch parent with sufficient capacity", err)
	case errors.Is(err, workingkv.ErrWorkingStateCorrupt):
		return fmt.Errorf("%w; abort this run and inspect the failure; corruption never permits backend fallback or publication retry", err)
	case errors.Is(err, workingkv.ErrWorkingMemoryLimitExceeded):
		return fmt.Errorf("%w; reduce concurrency/working state, increase --working-memory-ram-limit within reported headroom, or select kv with adequate scratch", err)
	case errors.Is(err, workingkv.ErrWorkingMigrationFailed):
		return fmt.Errorf("%w; abort this run; the prior backend was retained and no publication was retried", err)
	default:
		return err
	}
}
