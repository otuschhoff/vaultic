package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestProductionDiscardsAreExplained(t *testing.T) {
	result, findings, err := audit("../..")
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		lines := make([]string, 0, len(findings))
		for _, item := range findings {
			lines = append(lines, fmt.Sprintf("%s:%d: %s", item.file, item.line, item.text))
		}
		t.Fatalf("%d of %d production blank assignments are unexplained:\n%s",
			len(findings), result.assignments, strings.Join(lines, "\n"))
	}
	t.Logf("production blank assignments: %d (language mechanics: %d, explained: %d, unexplained: 0)",
		result.assignments, result.mechanics, result.justified)
}
