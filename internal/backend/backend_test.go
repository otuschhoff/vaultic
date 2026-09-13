package backend_test

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/test"
)

type testBackend struct {
	backend.Backend
}

func (t *testBackend) Unwrap() backend.Backend {
	return nil
}

type otherTestBackend struct {
	backend.Backend
}

func (t *otherTestBackend) Unwrap() backend.Backend {
	return t.Backend
}

func TestAsBackend(t *testing.T) {
	other := otherTestBackend{}
	test.Assert(t, backend.AsBackend[*testBackend](other) == nil, "otherTestBackend is not a testBackend backend")

	testBe := &testBackend{}
	test.Assert(t, backend.AsBackend[*testBackend](testBe) == testBe, "testBackend was not returned")

	wrapper := &otherTestBackend{Backend: testBe}
	test.Assert(t, backend.AsBackend[*testBackend](wrapper) == testBe, "failed to unwrap testBackend backend")

	wrapper.Backend = other
	test.Assert(t, backend.AsBackend[*testBackend](wrapper) == nil, "a wrapped otherTestBackend is not a testBackend")
}

type testConditionalWriter struct {
	backend.Backend
}

func (t *testConditionalWriter) CompareAndSwap(_ context.Context, _ backend.Handle, _ []byte, _ []byte) ([]byte, bool, error) {
	return nil, true, nil
}

func TestAsCapability(t *testing.T) {
	writer := &testConditionalWriter{}
	test.Assert(t, backend.AsCapability[backend.ConditionalWriter](writer) == writer, "capability not returned on direct backend")

	wrapper := &otherTestBackend{Backend: writer}
	test.Assert(t, backend.AsCapability[backend.ConditionalWriter](wrapper) == writer, "capability not discovered through unwrap")

	wrapper.Backend = otherTestBackend{}
	test.Assert(t, backend.AsCapability[backend.ConditionalWriter](wrapper) == nil, "non-capability backend unexpectedly matched")
}
