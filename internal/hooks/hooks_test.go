package hooks

import (
	"bytes"
	"context"
	"runtime"
	"testing"

	"github.com/otuschhoff/vaultic/internal/configfile"
)

func TestRunnerExportsContextAndWarns(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses /bin/sh")
	}
	var output bytes.Buffer
	var warnings bytes.Buffer
	runner := Runner{Stdout: &output, Stderr: &output, Warn: func(format string, args ...any) {
		_, _ = warnings.WriteString("warn")
	}}
	scopes := []configfile.Hooks{{
		Before: []configfile.Hook{{Command: "sh -c 'printf %s \"$VAULTIC_ACTION:$RUSTIC_BACKUP_LABEL\"'"}},
		After:  []configfile.Hook{{Command: "sh -c 'exit 1'", OnFailure: "warn"}},
	}}
	values := Context{Action: "backup", BackupLabel: "daily", BackupSources: []string{"/home/me"}}
	if err := runner.Run(context.Background(), Before, scopes, values); err != nil {
		t.Fatal(err)
	}
	if output.String() != "backup:daily" {
		t.Fatalf("hook output = %q", output.String())
	}
	if err := runner.Run(context.Background(), After, scopes, values); err != nil {
		t.Fatal(err)
	}
	if warnings.Len() == 0 {
		t.Fatal("warning hook failure was not reported")
	}
}
