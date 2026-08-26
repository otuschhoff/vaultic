// Package hooks executes profile-configured automation hooks.
package hooks

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/configfile"
)

const (
	Before  = "before"
	After   = "after"
	Failed  = "failed"
	Finally = "finally"
)

// Context contains values made available to hooks.
type Context struct {
	Type          string
	Action        string
	BackupLabel   string
	BackupSources []string
	BackupTags    []string
	SnapshotID    string
}

// Runner executes hooks without an implicit shell. Stdout and Stderr are
// optional and default to the process streams.
type Runner struct {
	Stdout io.Writer
	Stderr io.Writer
	Warn   func(string, ...any)
}

// Run executes all hooks of phase from scopes in order. A hook uses command
// plus args directly; a string command is split with backend.SplitShellStrings
// for compatibility, but is never passed to a shell.
func (r Runner) Run(ctx context.Context, phase string, scopes []configfile.Hooks, values Context) error {
	values.Type = phase
	for _, scope := range scopes {
		var list []configfile.Hook
		switch phase {
		case Before:
			list = scope.Before
		case After:
			list = scope.After
		case Failed:
			list = scope.Failed
		case Finally:
			list = scope.Finally
		default:
			return fmt.Errorf("unknown hook phase %q", phase)
		}
		for _, hook := range list {
			if err := r.runOne(ctx, hook, values); err != nil {
				switch hook.OnFailure {
				case "", "error":
					return err
				case "warn":
					if r.Warn != nil {
						r.Warn("hook %q failed: %v\n", hook.Command, err)
					}
				case "ignore":
				default:
					return fmt.Errorf("hook %q: invalid on-failure value %q", hook.Command, hook.OnFailure)
				}
			}
		}
	}
	return nil
}

func (r Runner) runOne(ctx context.Context, hook configfile.Hook, values Context) error {
	args, err := backend.SplitShellStrings(hook.Command)
	if err != nil {
		return fmt.Errorf("hook %q: %w", hook.Command, err)
	}
	args = append(args, hook.Args...)
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Env = append(os.Environ(), environment(values)...)
	cmd.Stdout = r.Stdout
	if cmd.Stdout == nil {
		cmd.Stdout = os.Stdout
	}
	cmd.Stderr = r.Stderr
	if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("hook %q: %w", hook.Command, err)
	}
	return nil
}

func environment(values Context) []string {
	entries := map[string]string{
		"HOOK_TYPE":      values.Type,
		"ACTION":         values.Action,
		"BACKUP_LABEL":   values.BackupLabel,
		"BACKUP_SOURCES": strings.Join(values.BackupSources, "\n"),
		"BACKUP_TAGS":    strings.Join(values.BackupTags, ","),
		"SNAPSHOT_ID":    values.SnapshotID,
	}
	result := make([]string, 0, len(entries)*2)
	for key, value := range entries {
		result = append(result, "VAULTIC_"+key+"="+value, "RUSTIC_"+key+"="+value)
	}
	return result
}
