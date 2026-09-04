package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	godebug "runtime/debug"
	"strings"

	"github.com/otuschhoff/vaultic/cmd/vaultic/backupcmd"
	"github.com/otuschhoff/vaultic/cmd/vaultic/indexcmd"
	"github.com/otuschhoff/vaultic/cmd/vaultic/keycmd"
	"github.com/otuschhoff/vaultic/cmd/vaultic/querycmd"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/otuschhoff/vaultic/internal/backend/all"
	"github.com/otuschhoff/vaultic/internal/configfile"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/env"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/hooks"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui/termstatus"
)

func init() {
	// don't import `go.uber.org/automaxprocs` to disable the log output
	_, _ = maxprocs.Set()
}

var ErrOK = errors.New("ok")

var cmdGroupDefault = "default"
var cmdGroupAdvanced = "advanced"

func newRootCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "vaultic",
		Short: "Backup and restore files",
		Long: `
vaultic is a backup program which allows saving multiple revisions of files and
directories in an encrypted repository stored on different backends.

The full documentation can be found at https://vaultic.readthedocs.io/ .
`,
		SilenceErrors:     true,
		SilenceUsage:      true,
		DisableAutoGenTag: true,

		PersistentPreRunE: func(c *cobra.Command, _ []string) error {
			switch c.Name() {
			case cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
				return nil
			}
			if err := applyProfile(c, globalOptions); err != nil {
				return err
			}
			if err := configureLogging(*globalOptions); err != nil {
				return err
			}
			if err := globalOptions.PreRun(needsPassword(c.Name())); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.AddGroup(
		&cobra.Group{
			ID:    cmdGroupDefault,
			Title: "Available Commands:",
		},
		&cobra.Group{
			ID:    cmdGroupAdvanced,
			Title: "Advanced Options:",
		},
	)

	globalOptions.AddFlags(cmd.PersistentFlags())

	// Use our "generate" command instead of the cobra provided "completion" command
	cmd.CompletionOptions.DisableDefaultCmd = true

	// globalOptions is passed to commands by reference to allow PersistentPreRunE to modify it
	cmd.AddCommand(
		backupcmd.NewCommand(globalOptions),
		newCacheCommand(globalOptions),
		newCatCommand(globalOptions),
		newCheckCommand(globalOptions),
		newConfigCommand(globalOptions),
		newCopyCommand(globalOptions),
		newDiffCommand(globalOptions),
		newDumpCommand(globalOptions),
		newFeaturesCommand(globalOptions),
		querycmd.NewFindCommand(globalOptions),
		newForgetCommand(globalOptions),
		newGenerateCommand(globalOptions),
		newInitCommand(globalOptions),
		indexcmd.NewCommand(globalOptions),
		keycmd.NewCommand(globalOptions),
		newListCommand(globalOptions),
		querycmd.NewLsCommand(globalOptions),
		newMigrateCommand(globalOptions),
		newMergeCommand(globalOptions),
		newOptionsCommand(globalOptions),
		newPruneCommand(globalOptions),
		newRepoInfoCommand(globalOptions),
		newRebuildIndexCommand(globalOptions),
		newRecoverCommand(globalOptions),
		newRepairCommand(globalOptions),
		newRestoreCommand(globalOptions),
		newRewriteCommand(globalOptions),
		newShowConfigCommand(globalOptions),
		querycmd.NewSnapshotsCommand(globalOptions),
		querycmd.NewStatsCommand(globalOptions),
		newTagCommand(globalOptions),
		newUnlockCommand(globalOptions),
		newVersionCommand(globalOptions),
		indexcmd.NewVerifyPacksCommand(globalOptions),
	)

	registerDebugCommand(cmd, globalOptions)
	registerMountCommand(cmd, globalOptions)
	registerSelfUpdateCommand(cmd, globalOptions)
	global.RegisterProfiling(cmd, os.Stderr)
	wrapProfileHooks(cmd, globalOptions)

	return cmd
}

func configureLogging(gopts global.Options) error {
	observability.SetDefaultSyslog(nil)
	if gopts.LogFile != "" {
		f, err := os.OpenFile(gopts.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("open log file %q: %w", gopts.LogFile, err)
		}
		log.SetOutput(io.MultiWriter(os.Stderr, f))
	}
	if len(gopts.SyslogTargets) > 0 {
		targets := make([]observability.SyslogTarget, len(gopts.SyslogTargets))
		for index, spec := range gopts.SyslogTargets {
			target, err := observability.ParseSyslogTarget(spec)
			if err != nil {
				return fmt.Errorf("invalid --syslog-target: %w", err)
			}
			targets[index] = target
		}
		hostname, _ := os.Hostname()
		observability.SetDefaultSyslog(observability.NewSyslogExporter(targets, hostname, "vaultic"))
	}
	return nil
}

func wrapProfileHooks(root *cobra.Command, gopts *global.Options) {
	for _, cmd := range root.Commands() {
		wrapProfileHooks(cmd, gopts)
	}
	if root.RunE == nil {
		return
	}

	run := root.RunE
	root.RunE = func(cmd *cobra.Command, args []string) (err error) {
		ctx, span := observability.StartCommand(cmd.Context(), gopts.OpenTelemetry, cmd.Name())
		defer span.End()
		cmd.SetContext(ctx)

		profile := gopts.Profile
		if profile == nil {
			return run(cmd, args)
		}

		label, _ := cmd.Flags().GetString("label")
		tags, _ := cmd.Flags().GetStringSlice("tag")
		values := hooks.Context{
			Action:        cmd.Name(),
			BackupLabel:   label,
			BackupSources: args,
			BackupTags:    tags,
		}
		runner := hooks.Runner{
			Stdout: gopts.Term.OutputWriter(),
			Stderr: gopts.Term.OutputWriter(),
			Warn: func(format string, args ...any) {
				gopts.Term.Error(fmt.Sprintf(format, args...))
			},
		}
		scopes := []configfile.Hooks{profile.Hooks["global"], profile.Hooks["repository"], profile.Hooks[cmd.Name()]}
		if err := runner.Run(cmd.Context(), hooks.Before, scopes, values); err != nil {
			return err
		}
		defer func() {
			phase := hooks.After
			if err != nil {
				phase = hooks.Failed
			}
			if hookErr := runner.Run(cmd.Context(), phase, scopes, values); hookErr != nil && err == nil {
				err = hookErr
			}
			if hookErr := runner.Run(cmd.Context(), hooks.Finally, scopes, values); hookErr != nil && err == nil {
				err = hookErr
			}
		}()
		return run(cmd, args)
	}
}

func applyProfile(cmd *cobra.Command, gopts *global.Options) error {
	profile, err := configfile.Load(gopts.UseProfiles)
	if err != nil {
		return err
	}
	gopts.Profile = profile

	// Profile values are defaults. Explicit CLI flags and VAULTIC_/RESTIC_
	// environment variables take precedence over them.
	envOverrides := func(name string) bool {
		name = strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
		_, ok := env.Lookup(name)
		return ok
	}
	apply := func(section string, flags *pflag.FlagSet) error {
		if flags == nil {
			return nil
		}
		return profile.ApplyFlags(section, flags, envOverrides)
	}

	persistentFlags := cmd.Root().PersistentFlags()
	if err := apply("global", persistentFlags); err != nil {
		return err
	}
	if err := apply("repository", persistentFlags); err != nil {
		return err
	}
	return apply(cmd.Name(), cmd.Flags())
}

// Distinguish commands that need the password from those that work without,
// so we don't run $VAULTIC_PASSWORD_COMMAND for no reason (it might prompt the
// user for authentication).
func needsPassword(cmd string) bool {
	switch cmd {
	case "cache", "generate", "help", "options", "self-update", "version", cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
		return false
	default:
		return true
	}
}

func tweakGoGC() {
	// lower GOGC from 100 to 50, unless it was manually overwritten by the user
	oldValue := godebug.SetGCPercent(50)
	if oldValue != 100 {
		godebug.SetGCPercent(oldValue)
	}
}

func printExitError(globalOptions global.Options, code int, message string) {
	if globalOptions.JSON {
		type jsonExitError struct {
			MessageType string `json:"message_type"` // exit_error
			Code        int    `json:"code"`
			Message     string `json:"message"`
		}

		jsonS := jsonExitError{
			MessageType: "exit_error",
			Code:        code,
			Message:     message,
		}

		err := json.NewEncoder(os.Stderr).Encode(jsonS)
		if err != nil {
			// ignore error as there's no good way to handle it
			_, _ = fmt.Fprintf(os.Stderr, "JSON encode failed: %v\n", err)
			debug.Log("JSON encode failed: %v\n", err)
			return
		}
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", message)
	}
}

func main() {
	tweakGoGC()
	// install custom global logger into a buffer, if an error occurs
	// we can show the logs
	logBuffer := bytes.NewBuffer(nil)
	log.SetOutput(logBuffer)

	err := feature.Flag.Apply(env.Get("FEATURES"), func(s string) {
		_, _ = fmt.Fprintln(os.Stderr, s)
	})
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		Exit(1)
	}

	debug.Log("main %#v", os.Args)
	debug.Log("vaultic %s compiled with %v on %v/%v",
		global.Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)

	globalOptions := global.Options{
		Backends: all.Backends(),
	}
	func() {
		term, cancel := termstatus.Setup(os.Stdin, os.Stdout, os.Stderr, globalOptions.Quiet)
		defer cancel()
		globalOptions.Term = term
		ctx := createGlobalContext(os.Stderr)
		err = newRootCommand(&globalOptions).ExecuteContext(ctx)
		switch err {
		case nil:
			err = ctx.Err()
		case ErrOK:
			// ErrOK overwrites context cancellation errors
			err = nil
		}
	}()

	var exitMessage string
	switch {
	case repository.IsAlreadyLocked(err):
		exitMessage = fmt.Sprintf("%v\nthe `unlock` command can be used to remove stale locks", err)
	case errors.Is(err, backupcmd.ErrInvalidSourceData):
		exitMessage = fmt.Sprintf("Warning: %v", err)
	case errors.IsFatal(err):
		exitMessage = err.Error()
	case errors.Is(err, repository.ErrNoKeyFound):
		exitMessage = fmt.Sprintf("Fatal: %v", err)
	case err != nil:
		exitMessage = fmt.Sprintf("%+v", err)

		if logBuffer.Len() > 0 {
			exitMessage += " also, the following messages were logged by a library:\n"
			sc := bufio.NewScanner(logBuffer)
			for sc.Scan() {
				exitMessage += fmt.Sprintln(sc.Text())
			}
		}
	}

	exitCode := exitCodeForError(err)

	if exitCode != 0 {
		printExitError(globalOptions, exitCode, exitMessage)
	}
	Exit(exitCode)
}

func exitCodeForError(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, backupcmd.ErrInvalidSourceData):
		return 3
	case errors.Is(err, indexcmd.ErrDifferences), errors.Is(err, indexcmd.ErrIncomplete):
		return 2
	case errors.Is(err, ErrFailedToRemoveOneOrMoreSnapshots):
		return 3
	case errors.Is(err, global.ErrNoRepository):
		return 10
	case repository.IsAlreadyLocked(err):
		return 11
	case errors.Is(err, repository.ErrNoKeyFound):
		return 12
	case errors.IsUnauthorized(err):
		return 12
	case errors.IsIntegrity(err):
		return 2
	case errors.IsRejected(err), errors.IsTransient(err), errors.IsUnavailable(err):
		return 1
	case errors.Is(err, context.Canceled):
		return 130
	default:
		return 1
	}
}
