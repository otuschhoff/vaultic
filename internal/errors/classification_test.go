package errors

import (
	stderrors "errors"
	"fmt"
	"testing"
)

func TestClassificationsPreserveCause(t *testing.T) {
	cause := stderrors.New("cause")
	tests := []struct {
		name       string
		err        error
		classified func(error) bool
	}{
		{"transient", &Transient{Err: cause}, IsTransient},
		{"rejected", &Rejected{Err: cause}, IsRejected},
		{"integrity", &Integrity{Err: cause}, IsIntegrity},
		{"unauthorized", &Unauthorized{Err: cause}, IsUnauthorized},
		{"unavailable", &Unavailable{Err: cause}, IsUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := fmt.Errorf("outer: %w", test.err)
			if !test.classified(err) || !stderrors.Is(err, cause) {
				t.Fatalf("classification or cause lost: %v", err)
			}
		})
	}
}
