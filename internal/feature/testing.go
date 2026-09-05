package feature

import (
	"fmt"
	"testing"
)

// TestSetFlag temporarily sets a feature flag to the given value until the
// returned function is called.
//
// Usage
// ```
// defer TestSetFlag(t, features.Flags, features.ExampleFlag, true)()
// ```
func TestSetFlag(_ *testing.T, f *FlagSet, flag FlagName, value bool) func() {
	current := f.Enabled(flag)

	panicIfCalled := func(msg string) {
		//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
		panic(msg)
	}

	if err := f.Apply(fmt.Sprintf("%s=%v", flag, value), panicIfCalled); err != nil {
		// not reachable
		//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
		panic(err)
	}

	return func() {
		if err := f.Apply(fmt.Sprintf("%s=%v", flag, current), panicIfCalled); err != nil {
			// not reachable
			//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
			panic(err)
		}
	}
}
