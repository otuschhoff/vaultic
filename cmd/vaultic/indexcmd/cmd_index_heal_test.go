package indexcmd

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
)

func TestIndexHealRegistersCompleteLifecycle(t *testing.T) {
	command := newIndexHealCommand(&global.Options{})
	want := map[string]bool{"status": false, "plan": false, "execute": false, "verify": false, "activate": false, "rollback": false, "retire": false}
	for _, child := range command.Commands() {
		if _, ok := want[child.Name()]; ok {
			want[child.Name()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("missing index heal %s command", name)
		}
	}
}

func TestIndexHealHighSeverityCommandsRequireAcknowledgement(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "activate", args: []string{"activate", "--plan", "a", "--report", "b"}},
		{name: "rollback", args: []string{"rollback", "--expected-decision", "2", "--report", "b"}},
		{name: "retire", args: []string{"retire", "--expected-decision", "2", "--report", "b", "--generation", "1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := newIndexHealCommand(&global.Options{})
			command.SetArgs(test.args)
			if err := command.ExecuteContext(context.Background()); err == nil {
				t.Fatal("high-severity command ran without explicit acknowledgement")
			}
		})
	}
}
