package indexcmd

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/spf13/cobra"
)

type indexWriterOptions struct {
	Daemon       indexDaemonOptions
	RepositoryID string
}

type writerPromoteOptions struct {
	Reason              string
	ForceTakeover       bool
	ExpectedActiveEpoch string
	Timeout             time.Duration
}

type writerStatusProvider interface {
	WriterStatus(context.Context) (daemon.WriterStatus, error)
}

func (options writerPromoteOptions) Finalize() error {
	if options.ForceTakeover && options.ExpectedActiveEpoch == "" {
		return fmt.Errorf("--force-takeover requires --expected-active-epoch")
	}
	if _, _, err := parseExpectedActiveEpoch(options.ExpectedActiveEpoch); err != nil {
		return err
	}
	if options.Timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	return nil
}

func parseExpectedActiveEpoch(value string) (uint64, bool, error) {
	if value == "" {
		return 0, false, nil
	}
	if value == "auto" {
		return 0, true, nil
	}
	epoch, err := strconv.ParseUint(value, 10, 64)
	if err != nil || epoch == 0 {
		return 0, false, fmt.Errorf("invalid --expected-active-epoch %q: use a positive integer or auto", value)
	}
	return epoch, false, nil
}

func (options writerPromoteOptions) resolveExpectedActiveEpoch(ctx context.Context, client writerStatusProvider) (uint64, error) {
	epoch, automatic, err := parseExpectedActiveEpoch(options.ExpectedActiveEpoch)
	if err != nil || !options.ForceTakeover || !automatic {
		return epoch, err
	}
	status, err := client.WriterStatus(ctx)
	if err != nil {
		return 0, fmt.Errorf("retrieve active writer epoch: %w", err)
	}
	if status.ObservedEpoch == 0 {
		return 0, fmt.Errorf("cannot automatically select an expected active epoch: no active writer claim was observed")
	}
	return status.ObservedEpoch, nil
}

func newIndexWriterCommand(globalOptions *global.Options) *cobra.Command {
	var options indexWriterOptions
	command := &cobra.Command{
		Use:               "writer",
		Short:             "Inspect and control VaulticDB writer ownership",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
	}
	options.Daemon.AddFlags(command.PersistentFlags())
	command.PersistentFlags().StringVar(&options.RepositoryID, "repository-id", "", "repository identity bound to metadata writer ownership")
	mustMarkPersistentFlagRequired(command, "repository-id")
	command.AddCommand(
		newIndexWriterStatusCommand(globalOptions, &options),
		newIndexWriterDemoteCommand(globalOptions, &options),
		newIndexWriterPromoteCommand(globalOptions, &options),
	)
	return command
}

func newIndexWriterStatusCommand(globalOptions *global.Options, options *indexWriterOptions) *cobra.Command {
	return &cobra.Command{
		Use:               "status",
		Short:             "Show writer ownership and quiescence state",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			status, err := withDaemonSession(command.Context(), options.Daemon, options.RepositoryID,
				func(client *daemon.Client) (daemon.WriterStatus, error) {
					return client.WriterStatus(command.Context())
				})
			if err != nil {
				return err
			}
			printWriterStatus(globalOptions, status)
			return nil
		},
	}
}

func newIndexWriterDemoteCommand(globalOptions *global.Options, options *indexWriterOptions) *cobra.Command {
	var force bool
	var timeout time.Duration
	var reason string
	command := &cobra.Command{
		Use:               "demote",
		Short:             "Quiesce and relinquish writer ownership",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			status, err := withDaemonSession(command.Context(), options.Daemon, options.RepositoryID,
				func(client *daemon.Client) (daemon.WriterStatus, error) {
					return client.DemoteWriter(command.Context(), reason, force, timeout)
				})
			if err != nil {
				return err
			}
			printWriterStatus(globalOptions, status)
			return nil
		},
	}
	command.Flags().BoolVar(&force, "force", false, "bypass minimum writer tenure while preserving quiescence")
	command.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "maximum time to quiesce and close the writer")
	command.Flags().StringVar(&reason, "reason", "operator request", "auditable writer transition reason")
	return command
}

func newIndexWriterPromoteCommand(globalOptions *global.Options, options *indexWriterOptions) *cobra.Command {
	promoteOptions := writerPromoteOptions{
		Reason:              "operator request",
		ExpectedActiveEpoch: "auto",
		Timeout:             time.Hour,
	}
	command := &cobra.Command{
		Use:               "promote",
		Short:             "Acquire a freshly fenced writer epoch",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return promoteOptions.Finalize()
		},
		RunE: func(command *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(command.Context(), promoteOptions.Timeout)
			defer cancel()
			status, err := withDaemonSession(ctx, options.Daemon, options.RepositoryID,
				func(client *daemon.Client) (daemon.WriterStatus, error) {
					expectedActiveEpoch, err := promoteOptions.resolveExpectedActiveEpoch(ctx, client)
					if err != nil {
						return daemon.WriterStatus{}, err
					}
					return client.PromoteWriterWithTakeover(
						ctx, promoteOptions.Reason, promoteOptions.ForceTakeover, expectedActiveEpoch,
					)
				})
			if err != nil {
				return err
			}
			printWriterStatus(globalOptions, status)
			return nil
		},
	}
	command.Flags().StringVar(&promoteOptions.Reason, "reason", "operator request", "auditable writer transition reason")
	command.Flags().BoolVar(&promoteOptions.ForceTakeover, "force-takeover", false, "replace a crashed writer claim using an exact conditional update")
	command.Flags().DurationVar(&promoteOptions.Timeout, "timeout", time.Hour, "maximum time to acquire the writer epoch and open SlateDB")
	command.Flags().StringVar(
		&promoteOptions.ExpectedActiveEpoch,
		"expected-active-epoch",
		"auto",
		"active writer epoch observed before authorizing takeover, or auto to retrieve it",
	)
	return command
}

func printWriterStatus(globalOptions *global.Options, status daemon.WriterStatus) {
	if globalOptions.JSON {
		globalOptions.Term.Print(ui.ToJSONString(status))
		return
	}
	globalOptions.Term.Print(
		fmt.Sprintf(
			"writer %s; instance %s; epoch %d (observed %d); promotion-safe %t\n",
			status.Role,
			status.InstanceID,
			status.CurrentEpoch,
			status.ObservedEpoch,
			status.PromotionSafe,
		),
	)
	globalOptions.Term.Print(
		fmt.Sprintf("active writes %d; transactions %d; transition %s\n", status.ActiveWriteIntents, status.ActiveTransactions, status.TransitionReason),
	)
}
