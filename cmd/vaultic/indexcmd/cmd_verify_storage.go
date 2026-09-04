package indexcmd

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	indexverification "github.com/otuschhoff/vaultic/internal/index/verification"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
)

type verifyStorageOptions struct {
	Daemon           indexDaemonOptions
	Tiers            []string
	Backends         []string
	StorageClasses   []string
	PackTypes        []string
	CreatedAfter     string
	CreatedBefore    string
	MinSize          uint64
	MaxSize          uint64
	RetentionStatus  string
	NotVerifiedSince string
	Level            string
	All              bool
	SampleCount      int
	SamplePercent    float64
	Seed             uint64
	ErrorsOnly       bool
	ErrorHistory     bool
	HistoryLimit     int
	CurrentStatus    bool
	Concurrency      int
}

type repositoryWarmer struct{ repository *repository.Repository }
type repositoryVerifier struct{ repository *repository.Repository }

var packTiers = map[string]schema.PackTier{
	"hot":      schema.TierHot,
	"cold":     schema.TierCold,
	"mirrored": schema.TierMirrored,
	"single":   schema.TierSingle,
	"unknown":  schema.TierUnknown,
}

var packTypes = map[string]schema.PackType{
	"data":    schema.PackData,
	"tree":    schema.PackTree,
	"mixed":   schema.PackMixed,
	"unknown": schema.PackUnknown,
}

func (verifier repositoryVerifier) VerifyPackPlacement(ctx context.Context, packID schema.ID, backend uint64, level schema.VerificationLevel) error {
	return verifier.repository.VerifyPackPlacement(ctx, vaultic.ID(packID), backend, level)
}

func (warmer repositoryWarmer) WarmupPlacements(ctx context.Context, candidates []indexverification.Candidate) error {
	targets := make([]repository.PlacementTarget, len(candidates))
	for index, candidate := range candidates {
		targets[index] = repository.PlacementTarget{PackID: vaultic.ID(candidate.PackID), Backend: candidate.Backend}
	}
	return warmer.repository.WarmupPackPlacements(ctx, targets)
}

func newIndexVerifyStorageCommand(globalOptions *global.Options) *cobra.Command {
	return newVerifyStorageCommand(globalOptions, "verify-storage", "Verify selected physical pack placements", "")
}

func NewVerifyPacksCommand(globalOptions *global.Options) *cobra.Command {
	return newVerifyStorageCommand(globalOptions, "verify-packs", "Verify selected physical pack placements", "advanced")
}

func newVerifyStorageCommand(globalOptions *global.Options, use, short, group string) *cobra.Command {
	var options verifyStorageOptions
	command := &cobra.Command{
		Use:               use,
		Short:             short,
		GroupID:           group,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			return runVerifyStorage(command.Context(), options, *globalOptions)
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringSliceVar(&options.Tiers, "tier", nil, "include hot, cold, mirrored, single, or unknown tier")
	command.Flags().StringSliceVar(&options.Backends, "backend", nil, "include backend ID hash (decimal or 0xhex)")
	command.Flags().StringSliceVar(&options.StorageClasses, "storage-class", nil, "include storage class")
	command.Flags().StringSliceVar(&options.PackTypes, "pack-type", nil, "include data, tree, mixed, or unknown packs")
	command.Flags().StringVar(&options.CreatedAfter, "created-after", "", "include packs created at or after RFC3339 time")
	command.Flags().StringVar(&options.CreatedBefore, "created-before", "", "include packs created before RFC3339 time")
	command.Flags().Uint64Var(&options.MinSize, "min-size", 0, "include packs at least this many physical bytes")
	command.Flags().Uint64Var(&options.MaxSize, "max-size", 0, "include packs at most this many physical bytes")
	command.Flags().StringVar(&options.RetentionStatus, "retention-status", "", "include active or expired retained packs")
	command.Flags().StringVar(&options.NotVerifiedSince, "not-verified-since", "", "include placements not verified at the requested level since RFC3339 time")
	command.Flags().StringVar(&options.Level, "level", "header", "verification level: header, checksum, or full")
	command.Flags().StringVar(&options.Level, "verification-level", "header", "alias for --level")
	command.Flags().BoolVar(&options.All, "all", false, "verify all matching placements")
	command.Flags().IntVar(&options.SampleCount, "sample-count", 0, "deterministically sample exactly N matching placements")
	command.Flags().Float64Var(&options.SamplePercent, "sample-percent", 0, "deterministically sample a percentage of matching placements")
	command.Flags().Uint64Var(&options.Seed, "seed", 0, "sampling seed")
	command.Flags().BoolVar(&options.ErrorsOnly, "errors-only", false, "select only placements with current findings")
	command.Flags().BoolVar(&options.ErrorHistory, "error-history", false, "report append-only verification finding history without verifying")
	command.Flags().IntVar(&options.HistoryLimit, "history-limit", 10000, "maximum verification history events to return")
	command.Flags().BoolVar(&options.CurrentStatus, "current-status", false, "report current placement verification state without verifying")
	command.Flags().IntVar(&options.Concurrency, "concurrency", 1, "number of concurrent placement checks")
	return command
}

func runVerifyStorage(ctx context.Context, options verifyStorageOptions, globalOptions global.Options) error {
	if options.ErrorHistory && options.CurrentStatus {
		return fmt.Errorf("--error-history and --current-status are mutually exclusive")
	}
	if options.ErrorHistory && hasVerificationCandidateSelection(options) {
		return fmt.Errorf("--error-history cannot be combined with candidate selection or sampling flags")
	}
	if options.ErrorHistory && options.HistoryLimit <= 0 {
		return fmt.Errorf("--history-limit must be positive")
	}
	config, err := options.Daemon.config("")
	if err != nil {
		return err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()
	if err := requireSlateDBRepository(repo); err != nil {
		return err
	}
	store, _, closeStore, err := openIndexStore(ctx, repo, options.Daemon)
	if err != nil {
		return err
	}
	defer closeStore()
	if options.ErrorHistory {
		history, err := verificationHistory(ctx, store, options.HistoryLimit)
		globalOptions.Term.Print(ui.ToJSONString(history))
		return err
	}
	selection, level, err := parseVerificationOptions(options)
	if err != nil {
		return err
	}
	candidates, err := indexverification.Plan(ctx, store, selection, time.Now())
	if err != nil {
		return err
	}
	if options.CurrentStatus {
		globalOptions.Term.Print(ui.ToJSONString(verificationStatus(candidates)))
		return nil
	}
	result, runErr := indexverification.Run(ctx, store, repositoryVerifier{repo}, repositoryWarmer{repo}, candidates, level, options.Concurrency, time.Now)
	globalOptions.Term.Print(ui.ToJSONString(result))
	severity := observability.Info
	if result.IntegrityErrors > 0 {
		severity = observability.Error
	} else if result.OperationalErrors > 0 {
		severity = observability.Warning
	}
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  severity,
			Category:  observability.CategoryIntegrity,
			Component: "verify-storage",
			Message:   "storage verification completed",
			Fields: map[string]any{
				"candidates":         result.Candidates,
				"verified":           result.Verified,
				"integrity_errors":   result.IntegrityErrors,
				"operational_errors": result.OperationalErrors,
			},
		},
	)
	return runErr
}

func hasVerificationCandidateSelection(options verifyStorageOptions) bool {
	return options.All || options.SampleCount != 0 || options.SamplePercent != 0 || options.ErrorsOnly ||
		len(options.Tiers) != 0 || len(options.Backends) != 0 || len(options.StorageClasses) != 0 || len(options.PackTypes) != 0 ||
		options.CreatedAfter != "" || options.CreatedBefore != "" || options.MinSize != 0 || options.MaxSize != 0 ||
		options.RetentionStatus != "" || options.NotVerifiedSince != ""
}

func parseVerificationOptions(options verifyStorageOptions) (indexverification.Options, schema.VerificationLevel, error) {
	selection := indexverification.Options{
		Tiers:           map[schema.PackTier]bool{},
		Backends:        map[uint64]bool{},
		StorageClasses:  map[string]bool{},
		PackTypes:       map[schema.PackType]bool{},
		RetentionStatus: options.RetentionStatus,
		SampleCount:     options.SampleCount,
		SamplePercent:   options.SamplePercent,
		Seed:            options.Seed,
		ErrorsOnly:      options.ErrorsOnly,
	}
	levels := map[string]schema.VerificationLevel{
		"header":   schema.VerificationHeader,
		"checksum": schema.VerificationChecksum,
		"full":     schema.VerificationFull,
		"unpack":   schema.VerificationFull,
	}
	level, ok := levels[options.Level]
	if !ok {
		return selection, 0, fmt.Errorf("invalid --level %q", options.Level)
	}
	selection.Level = level
	if err := validateVerificationMode(options); err != nil {
		return selection, 0, err
	}
	if err := parseVerificationSets(options, &selection); err != nil {
		return selection, 0, err
	}
	if err := parseVerificationTimes(options, &selection); err != nil {
		return selection, 0, err
	}
	if err := validateVerificationRanges(options, &selection); err != nil {
		return selection, 0, err
	}
	return selection, level, nil
}

func validateVerificationMode(options verifyStorageOptions) error {
	modes := 0
	if options.All {
		modes++
	}
	if options.SampleCount > 0 {
		modes++
	}
	if options.SamplePercent > 0 {
		modes++
	}
	if options.ErrorHistory && options.CurrentStatus {
		return fmt.Errorf("--error-history and --current-status are mutually exclusive")
	}
	if (!options.CurrentStatus && modes != 1) || (options.CurrentStatus && modes != 0) {
		if options.CurrentStatus {
			return fmt.Errorf("--current-status cannot be combined with a sampling mode")
		}
		return fmt.Errorf("exactly one of --all, --sample-count, or --sample-percent is required")
	}
	return nil
}

func parseVerificationSets(options verifyStorageOptions, selection *indexverification.Options) error {
	for _, value := range options.Tiers {
		tier, ok := packTiers[value]
		if !ok {
			return fmt.Errorf("invalid --tier %q", value)
		}
		selection.Tiers[tier] = true
	}
	for _, value := range options.PackTypes {
		packType, ok := packTypes[value]
		if !ok {
			return fmt.Errorf("invalid --pack-type %q", value)
		}
		selection.PackTypes[packType] = true
	}
	for _, value := range options.Backends {
		backend, err := strconv.ParseUint(value, 0, 64)
		if err != nil || backend == 0 {
			return fmt.Errorf("invalid --backend %q", value)
		}
		selection.Backends[backend] = true
	}
	for _, value := range options.StorageClasses {
		selection.StorageClasses[value] = true
	}
	return nil
}

func parseVerificationTime(name, value string) (*int64, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("invalid --%s: %w", name, err)
	}
	unix := parsed.Unix()
	return &unix, nil
}

func parseVerificationTimes(options verifyStorageOptions, selection *indexverification.Options) error {
	var err error
	if selection.CreatedAfter, err = parseVerificationTime("created-after", options.CreatedAfter); err != nil {
		return err
	}
	if selection.CreatedBefore, err = parseVerificationTime("created-before", options.CreatedBefore); err != nil {
		return err
	}
	if selection.NotVerifiedSince, err = parseVerificationTime("not-verified-since", options.NotVerifiedSince); err != nil {
		return err
	}
	if selection.CreatedAfter != nil && selection.CreatedBefore != nil && *selection.CreatedAfter >= *selection.CreatedBefore {
		return fmt.Errorf("--created-after must be earlier than --created-before")
	}
	if selection.NotVerifiedSince != nil && *selection.NotVerifiedSince > time.Now().Unix() {
		return fmt.Errorf("--not-verified-since cannot be in the future")
	}
	return nil
}

func validateVerificationRanges(options verifyStorageOptions, selection *indexverification.Options) error {
	if options.MinSize > 0 {
		selection.MinSize = &options.MinSize
	}
	if options.MaxSize > 0 {
		selection.MaxSize = &options.MaxSize
	}
	if options.MinSize > 0 && options.MaxSize > 0 && options.MinSize > options.MaxSize {
		return fmt.Errorf("--min-size must not exceed --max-size")
	}
	if options.RetentionStatus != "" && options.RetentionStatus != "active" && options.RetentionStatus != "expired" {
		return fmt.Errorf("invalid --retention-status %q", options.RetentionStatus)
	}
	if options.Concurrency <= 0 || options.SampleCount < 0 || options.SamplePercent < 0 || options.SamplePercent > 100 {
		return fmt.Errorf("invalid sampling or concurrency value")
	}
	return nil
}

type verificationHistoryEntry struct {
	Time     uint64                         `json:"time"`
	Sequence uint64                         `json:"sequence"`
	PackID   string                         `json:"pack_id"`
	Backend  uint64                         `json:"backend"`
	Type     string                         `json:"type"`
	Level    string                         `json:"level"`
	Class    string                         `json:"classification"`
	Event    schema.VerificationEventRecord `json:"event"`
}

type verificationStatusEntry struct {
	PackID       string                         `json:"pack_id"`
	Backend      uint64                         `json:"backend"`
	Tier         string                         `json:"tier"`
	StorageClass string                         `json:"storage_class,omitempty"`
	Result       string                         `json:"result"`
	Level        string                         `json:"last_attempt_level,omitempty"`
	Class        string                         `json:"classification,omitempty"`
	State        schema.VerificationStateRecord `json:"state"`
	HasState     bool                           `json:"has_state"`
}

func verificationStatus(candidates []indexverification.Candidate) []verificationStatusEntry {
	result := make([]verificationStatusEntry, len(candidates))
	for index, candidate := range candidates {
		result[index] = verificationStatusEntry{
			PackID:       hex.EncodeToString(candidate.PackID[:]),
			Backend:      candidate.Backend,
			Tier:         candidate.Pack.Tier.String(),
			StorageClass: candidate.Placement.StorageClass,
			Result:       verificationResultName(candidate.State.Result),
			Level:        verificationLevelName(candidate.State.LastAttemptLevel),
			Class:        verificationClassificationName(candidate.State.Classification),
			State:        candidate.State,
			HasState:     candidate.HasState,
		}
	}
	return result
}

func verificationHistory(ctx context.Context, store *daemon.SchemaStore, limit int) ([]verificationHistoryEntry, error) {
	var result []verificationHistoryEntry
	var cursor []byte
	for {
		items, done, err := store.ScanPrefix(ctx, schema.VerificationEventPrefix(), cursor, 1_000)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			key, err := schema.ParseKey(item.Key)
			if err != nil {
				return nil, err
			}
			event, err := schema.UnmarshalVerificationEventRecord(item.Value)
			if err != nil {
				return nil, err
			}
			result = append(
				result,
				verificationHistoryEntry{
					Time:     key.EventTime,
					Sequence: key.Revision,
					PackID:   hex.EncodeToString(key.ID[:]),
					Backend:  key.Backend,
					Type:     verificationEventTypeName(event.Type),
					Level:    verificationLevelName(event.Level),
					Class:    verificationClassificationName(event.Classification),
					Event:    event,
				},
			)
			cursor = append(cursor[:0], item.Key...)
			if len(result) >= limit {
				return result, nil
			}
		}
		if done {
			return result, nil
		}
		if len(items) == 0 {
			return nil, fmt.Errorf("verification history scan made no progress")
		}
	}
}

func verificationLevelName(value schema.VerificationLevel) string {
	return map[schema.VerificationLevel]string{
		schema.VerificationHeader:   "header",
		schema.VerificationChecksum: "checksum",
		schema.VerificationFull:     "full",
	}[value]
}

func verificationResultName(value schema.VerificationResult) string {
	return map[schema.VerificationResult]string{
		schema.VerificationUnknown:          "unknown",
		schema.VerificationHealthy:          "healthy",
		schema.VerificationOperationalError: "operational-error",
		schema.VerificationIntegrityError:   "integrity-error",
	}[value]
}

func verificationClassificationName(value schema.VerificationClassification) string {
	return map[schema.VerificationClassification]string{
		schema.VerificationNoError:              "none",
		schema.VerificationMissing:              "missing",
		schema.VerificationSizeMismatch:         "size-mismatch",
		schema.VerificationChecksumMismatch:     "checksum-mismatch",
		schema.VerificationHeaderAuthentication: "header-authentication",
		schema.VerificationPayloadDecrypt:       "payload-decrypt",
		schema.VerificationDecompression:        "decompression",
		schema.VerificationWarmupTimeout:        "warm-up-timeout",
		schema.VerificationTransport:            "transport",
		schema.VerificationCancelled:            "cancelled",
	}[value]
}

func verificationEventTypeName(value schema.VerificationEventType) string {
	return map[schema.VerificationEventType]string{
		schema.VerificationDetected: "detected",
		schema.VerificationChanged:  "changed",
		schema.VerificationResolved: "resolved",
	}[value]
}
