package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/global"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/healing"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/staging"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
)

type indexHealOptions struct {
	Daemon      indexDaemonOptions
	ArtifactDir string
}

type indexHealPlanOptions struct {
	indexHealOptions
	CandidateNamespace string
	FreshDEK           bool
}

type indexHealExecuteOptions struct {
	indexHealOptions
	PlanID    string
	Candidate indexDaemonOptions
}

type indexHealVerifyOptions struct {
	indexHealOptions
	PlanID    string
	Candidate indexDaemonOptions
}

type indexHealDecisionOptions struct {
	indexHealOptions
	Candidate        indexDaemonOptions
	PlanID           string
	ReportID         string
	ExpectedDecision uint64
	Generation       uint64
	Observation      time.Duration
	Acknowledge      bool
}

func newIndexHealCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "heal", Short: "Quarantine, rebuild, validate, and replace suspect metadata", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(
		newIndexHealStatusCommand(globalOptions),
		newIndexHealPlanCommand(globalOptions),
		newIndexHealExecuteCommand(globalOptions),
		newIndexHealVerifyCommand(globalOptions),
		newIndexHealActivateCommand(globalOptions),
		newIndexHealRollbackCommand(globalOptions),
		newIndexHealRetireCommand(globalOptions),
	)
	return command
}

func addHealFlags(command *cobra.Command, options *indexHealOptions) {
	options.Daemon.AddFlags(command.Flags())
	addHealArtifactFlag(command, options)
}

func addHealArtifactFlag(command *cobra.Command, options *indexHealOptions) {
	command.Flags().StringVar(&options.ArtifactDir, "artifact-dir", "", "protected directory containing signed healing plans, checkpoints, and reports")
}

func newIndexHealStatusCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHealOptions
	command := &cobra.Command{Use: "status", Short: "Show metadata generation authority and recovery interlocks", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		return withHealing(command.Context(), *globalOptions, options, func(_ *repository.Repository, client *daemon.Client, _ healing.Store, _ []byte) error {
			status, err := client.GenerationStatus(command.Context())
			if err != nil {
				return err
			}
			if status.State != "healing-required" {
				return fmt.Errorf("Plan B requires a generation quarantined by a proven Plan A healing-required result")
			}
			printHealResult(globalOptions, status)
			return nil
		})
	}}
	addHealFlags(command, &options)
	return command
}

func newIndexHealPlanCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHealPlanOptions
	command := &cobra.Command{Use: "plan", Short: "Inventory authenticated reconstruction sources and quarantine suspect metadata", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		return withHealing(command.Context(), *globalOptions, options.indexHealOptions, func(repo *repository.Repository, client *daemon.Client, artifactStore healing.Store, key []byte) error {
			status, err := client.GenerationStatus(command.Context())
			if err != nil {
				return err
			}
			inventory, err := healingInventory(command.Context(), repo)
			if err != nil {
				return err
			}
			policy := "reuse-recovered"
			if options.FreshDEK {
				policy = "fresh-capsule-before-write"
			}
			plan, err := healing.NewPlan(repo.Config().ID, options.CandidateNamespace, policy, status, inventory, key, time.Now())
			if err != nil {
				return err
			}
			if err := artifactStore.SavePlan(plan); err != nil {
				return err
			}
			_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Critical, Category: observability.CategoryIntegrity, Component: "index-heal", Message: "metadata healing plan created for quarantined generation", Fields: map[string]any{"plan_id": plan.ID, "generation": plan.SuspectGeneration}})
			printHealResult(globalOptions, plan)
			return nil
		})
	}}
	addHealFlags(command, &options.indexHealOptions)
	command.Flags().StringVar(&options.CandidateNamespace, "candidate-namespace", "", "dedicated immutable candidate metadata namespace")
	command.Flags().BoolVar(&options.FreshDEK, "fresh-metadata-dek", false, "require a fresh capsule generation before candidate writes")
	_ = command.MarkFlagRequired("candidate-namespace")
	return command
}

func newIndexHealExecuteCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHealExecuteOptions
	command := &cobra.Command{Use: "execute", Short: "Build or resume an isolated metadata candidate", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		return executeHealingPlan(command.Context(), *globalOptions, options, globalOptions)
	}}
	addHealArtifactFlag(command, &options.indexHealOptions)
	options.Candidate.AddFlags(command.Flags())
	command.Flags().StringVar(&options.PlanID, "plan", "", "signed healing plan ID")
	_ = command.MarkFlagRequired("plan")
	return command
}

func newIndexHealVerifyCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHealVerifyOptions
	command := &cobra.Command{Use: "verify", Short: "Run all activation gates and sign a healing report", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		return verifyHealingCandidate(command.Context(), *globalOptions, options, globalOptions)
	}}
	addHealArtifactFlag(command, &options.indexHealOptions)
	options.Candidate.AddFlags(command.Flags())
	command.Flags().StringVar(&options.PlanID, "plan", "", "signed healing plan ID")
	_ = command.MarkFlagRequired("plan")
	return command
}

func newIndexHealActivateCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHealDecisionOptions
	command := &cobra.Command{Use: "activate", Short: "Atomically activate a verified candidate generation", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !options.Acknowledge {
			return fmt.Errorf("activation requires --approve")
		}
		return withHealing(command.Context(), *globalOptions, options.indexHealOptions, func(repo *repository.Repository, client *daemon.Client, store healing.Store, key []byte) error {
			plan, report, err := loadHealingAuthorization(store, options.PlanID, options.ReportID, key)
			if err != nil {
				return err
			}
			if rebuildCandidateName(options.Candidate) != plan.CandidateNamespace {
				return fmt.Errorf("candidate daemon namespace does not match signed healing plan")
			}
			candidateOptions := options.Candidate
			candidateOptions.RebuildInitialize, candidateOptions.Start = false, false
			candidateClient, err := candidateOptions.connect(command.Context(), repo.Config().ID)
			if err != nil {
				return fmt.Errorf("re-resolve candidate generation: %w", err)
			}
			defer candidateClient.Close(context.Background())
			candidateStatus, err := candidateClient.WriterStatus(command.Context())
			if err != nil {
				return err
			}
			if candidateStatus.Role != "read-only" && candidateStatus.Role != "read-write" {
				return fmt.Errorf("candidate daemon is not ready for fenced activation: %s", candidateStatus.Role)
			}
			status, err := client.GenerationStatus(command.Context())
			if err != nil {
				return err
			}
			alreadyActivated := status.ActiveGeneration == plan.CandidateGeneration && status.Namespace == plan.CandidateNamespace && status.State == "post-activation"
			if !alreadyActivated && (status.Decision != plan.AuthorityDecision+1 || status.ActiveGeneration != plan.SuspectGeneration || status.State != "healing-required") {
				return fmt.Errorf("generation authority changed since plan creation")
			}
			activated := status
			if !alreadyActivated {
				activated, err = client.ActivateGeneration(command.Context(), plan.SuspectGeneration, plan.CandidateGeneration, plan.CandidateNamespace, report.ID, options.Observation)
				if err != nil {
					return err
				}
			}
			if candidateStatus.Role == "read-only" {
				candidateStatus, err = candidateClient.PromoteWriter(command.Context(), "activated metadata generation")
				if err != nil {
					return fmt.Errorf("acquire fresh candidate writer fence after authority activation: %w", err)
				}
			}
			_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "index-heal", Message: "candidate metadata generation activated", Fields: map[string]any{"plan_id": plan.ID, "generation": activated.ActiveGeneration, "decision": activated.Decision, "writer_epoch": candidateStatus.CurrentEpoch}})
			printHealResult(globalOptions, activated)
			return nil
		})
	}}
	addHealArtifactFlag(command, &options.indexHealOptions)
	options.Candidate.AddFlags(command.Flags())
	command.Flags().StringVar(&options.PlanID, "plan", "", "signed healing plan ID")
	command.Flags().StringVar(&options.ReportID, "report", "", "signed clean healing report ID")
	command.Flags().DurationVar(&options.Observation, "observation-window", 24*time.Hour, "post-activation observation interval")
	command.Flags().BoolVar(&options.Acknowledge, "approve", false, "explicitly approve metadata authority activation")
	return command
}

func newIndexHealRollbackCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHealDecisionOptions
	command := &cobra.Command{Use: "rollback", Short: "Publish a newer authority decision restoring the quarantined generation", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !options.Acknowledge {
			return fmt.Errorf("rollback requires --acknowledge-rollback")
		}
		return withHealing(command.Context(), *globalOptions, options.indexHealOptions, func(repo *repository.Repository, client *daemon.Client, store healing.Store, key []byte) error {
			plan, _, err := loadHealingAuthorization(store, options.PlanID, options.ReportID, key)
			if err != nil {
				return err
			}
			if rebuildCandidateName(options.Candidate) != plan.CandidateNamespace {
				return fmt.Errorf("candidate daemon namespace does not match signed healing plan")
			}
			candidateOptions := options.Candidate
			candidateOptions.RebuildInitialize, candidateOptions.Start = false, false
			candidateClient, err := candidateOptions.connect(command.Context(), repo.Config().ID)
			if err != nil {
				return fmt.Errorf("connect active candidate for rollback: %w", err)
			}
			defer candidateClient.Close(context.Background())
			candidateStatus, err := candidateClient.WriterStatus(command.Context())
			if err != nil {
				return err
			}
			if candidateStatus.Role == "read-write" {
				if _, err := candidateClient.DemoteWriter(command.Context(), "metadata generation rollback", false, time.Minute); err != nil {
					return fmt.Errorf("demote active candidate before rollback: %w", err)
				}
			} else if candidateStatus.Role != "read-only" {
				return fmt.Errorf("candidate daemon cannot be safely rolled back from role %s", candidateStatus.Role)
			}
			status, err := client.RollbackGeneration(command.Context(), options.ExpectedDecision, options.ReportID, options.Observation)
			if err != nil {
				return err
			}
			_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "index-heal", Message: "metadata generation rolled back", Fields: map[string]any{"plan_id": plan.ID, "generation": status.ActiveGeneration, "decision": status.Decision}})
			printHealResult(globalOptions, status)
			return nil
		})
	}}
	addHealArtifactFlag(command, &options.indexHealOptions)
	options.Candidate.AddFlags(command.Flags())
	command.Flags().StringVar(&options.PlanID, "plan", "", "signed healing plan ID")
	command.Flags().StringVar(&options.ReportID, "report", "", "signed clean healing report ID")
	command.Flags().Uint64Var(&options.ExpectedDecision, "expected-decision", 0, "authority decision compare-and-swap predicate")
	command.Flags().DurationVar(&options.Observation, "observation-window", 24*time.Hour, "post-rollback observation interval")
	command.Flags().BoolVar(&options.Acknowledge, "acknowledge-rollback", false, "acknowledge high-severity rollback before maintenance resumes")
	return command
}

func newIndexHealRetireCommand(globalOptions *global.Options) *cobra.Command {
	var options indexHealDecisionOptions
	command := &cobra.Command{Use: "retire", Short: "Retire a retained forensic generation after post-activation verification", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !options.Acknowledge || options.Generation == 0 {
			return fmt.Errorf("retirement requires --generation and --acknowledge-retirement")
		}
		return withHealing(command.Context(), *globalOptions, options.indexHealOptions, func(_ *repository.Repository, client *daemon.Client, _ healing.Store, _ []byte) error {
			status, err := client.RetireGeneration(command.Context(), options.ExpectedDecision, options.Generation, options.ReportID)
			if err == nil {
				printHealResult(globalOptions, status)
			}
			return err
		})
	}}
	addDecisionFlags(command, &options)
	command.Flags().Uint64Var(&options.Generation, "generation", 0, "retained generation to retire")
	command.Flags().BoolVar(&options.Acknowledge, "acknowledge-retirement", false, "acknowledge irreversible forensic namespace retirement eligibility")
	return command
}

func addDecisionFlags(command *cobra.Command, options *indexHealDecisionOptions) {
	addHealFlags(command, &options.indexHealOptions)
	command.Flags().StringVar(&options.PlanID, "plan", "", "signed healing plan ID")
	command.Flags().StringVar(&options.ReportID, "report", "", "signed healing report ID or SHA-256 audit digest")
	command.Flags().Uint64Var(&options.ExpectedDecision, "expected-decision", 0, "authority decision compare-and-swap predicate")
	command.Flags().DurationVar(&options.Observation, "observation-window", 24*time.Hour, "post-activation observation interval")
}

func withHealing(ctx context.Context, globalOptions global.Options, options indexHealOptions, action func(*repository.Repository, *daemon.Client, healing.Store, []byte) error) error {
	config, err := options.Daemon.config("")
	if err != nil {
		return err
	}
	ctx = repository.WithDaemonOptions(ctx, config)
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, globalOptions.Term)
	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	client, closeClient, err := healingClient(ctx, repo, options.Daemon)
	if err != nil {
		return err
	}
	defer closeClient()
	key, err := healing.DeriveKey(repo.Key().EncryptionKey[:], repo.Config().ID)
	if err != nil {
		return err
	}
	directory := options.ArtifactDir
	if directory == "" {
		directory = filepath.Join(globalOptions.CacheDir, "healing", repo.Config().ID)
	}
	return action(repo, client, healing.Store{Directory: directory}, key)
}

func healingClient(ctx context.Context, repo *repository.Repository, options indexDaemonOptions) (*daemon.Client, func(), error) {
	if engine, ok := repo.Engine().(*enginepkg.DaemonEngine); ok {
		return engine.Client(), func() {}, nil
	}
	client, err := options.connect(ctx, repo.Config().ID)
	if err != nil {
		return nil, func() {}, err
	}
	return client, func() { _ = client.Close(context.Background()) }, nil
}

func healingInventory(ctx context.Context, repo *repository.Repository) (healing.Inventory, error) {
	inventory := healing.Inventory{}
	for _, placement := range repo.Config().PlacementBackends {
		inventory.PackBackends = append(inventory.PackBackends, placement.ID)
		placementBackend, ok := repo.PlacementBackend(repository.PlacementBackendHash(placement.ID))
		if !ok {
			return inventory, fmt.Errorf("placement backend %q is not open", placement.ID)
		}
		source := healing.Source{Kind: "pack-backend", ID: placement.ID, Authority: 3, Authenticated: true}
		if err := placementBackend.List(ctx, backend.PackFile, func(info backend.FileInfo) error {
			if info.Size < 0 {
				return fmt.Errorf("pack %s reports a negative size", info.Name)
			}
			source.Objects++
			source.Bytes += uint64(info.Size)
			inventory.EstimatedObjects++
			inventory.EstimatedBytes += uint64(info.Size)
			return nil
		}); err != nil {
			return inventory, fmt.Errorf("inventory placement backend %q: %w", placement.ID, err)
		}
		inventory.Sources = append(inventory.Sources, source)
	}
	sort.Strings(inventory.PackBackends)
	if err := repo.Backend().List(ctx, backend.IndexFile, func(info backend.FileInfo) error {
		inventory.LegacyIndexes++
		inventory.EstimatedObjects++
		inventory.EstimatedBytes += uint64(info.Size)
		return nil
	}); err != nil {
		return inventory, err
	}
	if err := repo.Backend().List(ctx, backend.SnapshotFile, func(info backend.FileInfo) error {
		inventory.LegacySnapshots++
		inventory.EstimatedObjects++
		inventory.EstimatedBytes += uint64(info.Size)
		return nil
	}); err != nil {
		return inventory, err
	}
	journalStore, err := stagingStore(repo)
	if err != nil {
		return inventory, err
	}
	jobs, err := journalStore.Discover(ctx, repo.Config().ID)
	if err != nil {
		return inventory, err
	}
	for _, job := range jobs {
		if job.State != staging.StateSealedPending && job.State != staging.StateCommitted {
			continue
		}
		inventory.Sources = append(inventory.Sources, healing.Source{Kind: "ingest-journal", ID: job.Header.JobID, Authority: 1, Authenticated: true, State: string(job.State)})
		if job.Header.CapsuleGeneration > inventory.CapsuleGeneration {
			inventory.CapsuleGeneration = job.Header.CapsuleGeneration
		}
	}
	if inventory.LegacyIndexes > 0 {
		inventory.Sources = append(inventory.Sources, healing.Source{Kind: "legacy-indexes", ID: "repository-index-set", Authority: 2, Authenticated: true, Objects: inventory.LegacyIndexes})
	}
	if inventory.LegacySnapshots > 0 {
		inventory.Sources = append(inventory.Sources, healing.Source{Kind: "legacy-snapshots", ID: "repository-snapshot-set", Authority: 2, Authenticated: true, Objects: inventory.LegacySnapshots})
	}
	return inventory, nil
}

func executeHealingPlan(ctx context.Context, globalOptions global.Options, options indexHealExecuteOptions, output *global.Options) error {
	var plan healing.Plan
	var key []byte
	var store healing.Store
	err := withHealing(ctx, globalOptions, options.indexHealOptions, func(_ *repository.Repository, client *daemon.Client, artifactStore healing.Store, artifactKey []byte) error {
		var err error
		plan, err = artifactStore.LoadPlan(options.PlanID)
		if err != nil {
			return err
		}
		if err = healing.VerifyPlan(plan, artifactKey); err != nil {
			return err
		}
		status, err := client.GenerationStatus(ctx)
		if err != nil {
			return err
		}
		if status.State != "healing-required" || status.ActiveGeneration != plan.SuspectGeneration {
			return fmt.Errorf("suspect generation is not quarantined")
		}
		store, key = artifactStore, artifactKey
		return nil
	})
	if err != nil {
		return err
	}
	if !options.Candidate.RebuildInitialize || !options.Candidate.Start || !options.Candidate.Persistent {
		return fmt.Errorf("execute requires candidate --metadata-rebuild-initialize, --start-daemon, and --persistent-daemon")
	}
	if rebuildCandidateName(options.Candidate) != plan.CandidateNamespace {
		return fmt.Errorf("candidate daemon namespace does not match signed healing plan")
	}
	if plan.TargetDEKPolicy == "fresh-capsule-before-write" {
		if options.Candidate.BrokerSocket == "" || options.Candidate.EncryptionMode != "required" {
			return fmt.Errorf("fresh-DEK plan requires brokered rebuild initialization with --metadata-encryption=required")
		}
	}
	importOptions := indexImportOptions{Daemon: options.Candidate, Resume: true, FromLegacy: true, SnapshotDepth: ^uint(0), ConfirmMetadataLossRebuild: true}
	recoveryOptions := globalOptions
	recoveryOptions.MetadataLossRecovery = true
	result, err := runIndexImport(ctx, importOptions, recoveryOptions, globalOptions.Term)
	if err != nil {
		return err
	}
	var replayed []string
	err = withHealing(ctx, globalOptions, options.indexHealOptions, func(repo *repository.Repository, _ *daemon.Client, _ healing.Store, _ []byte) error {
		candidateOptions := options.Candidate
		candidateOptions.RebuildInitialize, candidateOptions.Start = false, false
		candidateClient, err := candidateOptions.connect(ctx, repo.Config().ID)
		if err != nil {
			return err
		}
		defer candidateClient.Close(context.Background())
		if plan.TargetDEKPolicy == "fresh-capsule-before-write" {
			encryption := candidateClient.Encryption()
			if !encryption.Enabled || encryption.EnvelopeGeneration == 0 || encryption.ActiveDEKVersion == 0 {
				return fmt.Errorf("candidate did not publish a fresh brokered metadata envelope before rebuild")
			}
		}
		candidateStore := daemon.NewSchemaStore(candidateClient)
		journalStore, err := stagingStore(repo)
		if err != nil {
			return err
		}
		jobs, err := journalStore.Discover(ctx, repo.Config().ID)
		if err != nil {
			return err
		}
		authority := &staging.DaemonAuthority{
			Client: candidateClient, Store: candidateStore,
			Preflight: func(ctx context.Context, header staging.Header) error {
				if header.RepositoryID != repo.Config().ID {
					return fmt.Errorf("journal repository identity mismatch")
				}
				_, err := candidateStore.MetadataHead(ctx)
				return err
			},
			SnapshotPublisher: func(context.Context, string, []byte) error { return nil },
			ReplayObservations: func(ctx context.Context, payloads []json.RawMessage) ([]byte, error) {
				observations, err := reconcile.DecodeDeferredObservations(payloads)
				if err != nil {
					return nil, err
				}
				return reconcile.ReplayDeferred(ctx, candidateStore, observations, reconcile.Options{PathIndexPaths: repo.Config().PathIndexPaths})
			},
		}
		for _, job := range jobs {
			if job.State != staging.StateSealedPending && job.State != staging.StateCommitted {
				continue
			}
			reconciliation := staging.ReconcileCandidate(ctx, journalStore, authority, staging.BackendPackVerifier{Backends: journalStore.Mirrors, Policy: journalStore.Policy}, job)
			if reconciliation.Disposition != staging.ReconcileCommitted {
				return fmt.Errorf("candidate journal %s replay: %s: %s", job.Header.JobID, reconciliation.Disposition, reconciliation.Reason)
			}
			replayed = append(replayed, job.Header.JobID)
		}
		if _, err := candidateClient.DemoteWriter(ctx, "candidate ready for read-only inspection", false, time.Minute); err != nil {
			return fmt.Errorf("demote candidate for inspection: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	checkpoint := healing.SignCheckpoint(healing.Checkpoint{PlanID: plan.ID, State: "candidate-ready", SourcesCompleted: []string{"legacy-indexes", "legacy-snapshots"}, JournalsReplayed: replayed, ObjectsWritten: result.PacksImported + result.BlobsImported + result.SnapshotsImported, UpdatedAt: time.Now(), CandidateReadOnly: true}, key)
	if err := store.SaveCheckpoint(checkpoint); err != nil {
		return err
	}
	printHealResult(output, checkpoint)
	return nil
}

func verifyHealingCandidate(ctx context.Context, globalOptions global.Options, options indexHealVerifyOptions, output *global.Options) error {
	return withHealing(ctx, globalOptions, options.indexHealOptions, func(repo *repository.Repository, authorityClient *daemon.Client, store healing.Store, key []byte) error {
		plan, err := store.LoadPlan(options.PlanID)
		if err != nil {
			return err
		}
		if err := healing.VerifyPlan(plan, key); err != nil {
			return err
		}
		checkpoint, err := store.LoadCheckpoint(plan.ID)
		if err != nil {
			return err
		}
		if err := healing.VerifyCheckpoint(checkpoint, plan.ID, key); err != nil {
			return err
		}
		candidateOptions := options.Candidate
		candidateOptions.RebuildInitialize, candidateOptions.Start = false, false
		candidateClient, err := candidateOptions.connect(ctx, repo.Config().ID)
		if err != nil {
			return err
		}
		defer candidateClient.Close(context.Background())
		candidateStore := daemon.NewSchemaStore(candidateClient)
		model, modelErr := indexMaintenancePlacementModel(repo)
		check, checkErr := maintenance.CheckWithOptions(ctx, repo, candidateStore, maintenance.CheckOptions{MaxFindings: 100, PlacementModel: model, PathIndexPaths: repo.Config().PathIndexPaths})
		audit, auditErr := candidateClient.CheckEncryption(ctx)
		gates := healing.Gates{Identity: modelErr == nil, AntiRollback: plan.CandidateGeneration > plan.SuspectGeneration, StructuralAEAD: auditErr == nil && audit.InvalidObjects == 0 && audit.PlaintextObjects == 0, PacksAndBlobOffsets: checkErr == nil && len(check.Findings) == 0, TreesAndSnapshots: checkErr == nil && len(check.Findings) == 0, PlacementPolicy: modelErr == nil && checkErr == nil && len(check.Findings) == 0, JournalCompletions: true, LegacyComparison: checkErr == nil && !check.HasWarnings(), ReadOnlyInspection: checkpoint.CandidateReadOnly}
		var conflicts []string
		if modelErr != nil {
			conflicts = append(conflicts, modelErr.Error())
		}
		if checkErr != nil {
			conflicts = append(conflicts, checkErr.Error())
		}
		if auditErr != nil {
			conflicts = append(conflicts, auditErr.Error())
		}
		for _, finding := range check.Findings {
			conflicts = append(conflicts, fmt.Sprint(finding))
		}
		report, err := healing.NewReport(plan, gates, conflicts, plan.Inventory.RequiredCrawlDebt, nil, map[string]uint64{"snapshots": check.SlateDBSnapshots, "locations": check.SlateDBLocations}, key, time.Now())
		if err != nil {
			return err
		}
		if err := store.SaveReport(report); err != nil {
			return err
		}
		printHealResult(output, report)
		if !report.Clean() {
			return fmt.Errorf("%w: candidate failed activation gates", errIndexIncomplete)
		}
		status, err := authorityClient.GenerationStatus(ctx)
		if err != nil {
			return err
		}
		if status.State == "post-activation" && status.ActiveGeneration == plan.CandidateGeneration {
			verified, err := authorityClient.VerifyGeneration(ctx, status.Decision, report.ID)
			if err != nil {
				return fmt.Errorf("complete post-activation verification: %w", err)
			}
			_ = observability.Emit(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryIntegrity, Component: "index-heal", Message: "post-activation metadata verification completed", Fields: map[string]any{"plan_id": plan.ID, "generation": verified.ActiveGeneration, "decision": verified.Decision}})
			printHealResult(output, verified)
		}
		return nil
	})
}

func loadHealingAuthorization(store healing.Store, planID, reportID string, key []byte) (healing.Plan, healing.Report, error) {
	plan, err := store.LoadPlan(planID)
	if err != nil {
		return healing.Plan{}, healing.Report{}, err
	}
	if err := healing.VerifyPlan(plan, key); err != nil {
		return healing.Plan{}, healing.Report{}, err
	}
	report, err := store.LoadReport(reportID)
	if err != nil {
		return healing.Plan{}, healing.Report{}, err
	}
	if err := healing.VerifyReport(report, plan, key); err != nil {
		return healing.Plan{}, healing.Report{}, err
	}
	return plan, report, nil
}

func printHealResult(options *global.Options, value any) {
	encoded, _ := json.MarshalIndent(value, "", "  ")
	options.Term.Print(string(encoded) + "\n")
}
