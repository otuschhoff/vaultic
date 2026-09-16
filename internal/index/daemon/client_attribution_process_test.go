package daemon

import (
	"bufio"
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
)

func assertAttributionCountersZero(t *testing.T, value reflect.Value) {
	t.Helper()
	if value.Kind() == reflect.Struct {
		valueType := value.Type()
		for index := 0; index < value.NumField(); index++ {
			if valueType.Field(index).Name == "LatencyBucketUpperUS" {
				continue
			}
			assertAttributionCountersZero(t, value.Field(index))
		}
		return
	}
	if value.Kind() == reflect.Slice {
		for index := 0; index < value.Len(); index++ {
			assertAttributionCountersZero(t, value.Index(index))
		}
		return
	}
	if value.Kind() == reflect.Uint64 && value.Uint() != 0 {
		t.Fatalf("disabled attribution counter is nonzero: %v", value.Uint())
	}
}

func TestProcessWriterStatusDisablesAttributionOnlyWithCapability(t *testing.T) {
	ctx := context.Background()
	options := Options{
		Socket: testSocket(t), RepositoryID: "attribution-disabled", DaemonPath: failureDaemonBinary(t),
		DataDir: t.TempDir(), ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_ATTRIBUTION_DISABLED=true"},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if _, err := client.WriteBatch(ctx, []Mutation{{Key: []byte("key"), Value: []byte("value")}}, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	value, found, err := client.Get(ctx, []byte("key"), "")
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("get after disabled-attribution write: value=%q found=%t err=%v", value, found, err)
	}
	status, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertAttributionCountersZero(t, reflect.ValueOf(status.Attribution))
}

func TestProcessWriterStatusAttributesServiceAndEngineBoundaries(t *testing.T) {
	ctx := context.Background()
	barrierPath := testSocket(t)
	barrier, err := net.Listen("unix", barrierPath)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	dataDir := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "attribution", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_MUTATION_BARRIER=" + barrierPath},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	before, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() {
		_, writeErr := client.WriteBatch(ctx, []Mutation{{Key: []byte("key"), Value: []byte("value")}}, nil, true, "")
		written <- writeErr
	}()
	connection, err := barrier.Accept()
	if err != nil {
		t.Fatal(err)
	}
	name, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(name) != "VAULTICDB_TEST_MUTATION_BARRIER" {
		t.Fatalf("barrier = %q", name)
	}
	blocked, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Attribution.AdmissionWait.Attempts != before.Attribution.AdmissionWait.Attempts+1 {
		t.Fatalf("blocked admission = %+v, before = %+v", blocked.Attribution.AdmissionWait, before.Attribution.AdmissionWait)
	}
	if blocked.Attribution.WriteBatchRequest.Attempts != before.Attribution.WriteBatchRequest.Attempts+1 || blocked.Attribution.WriteBatchRequest.Completed != before.Attribution.WriteBatchRequest.Completed || blocked.Attribution.WriteBatchRequest.Active != before.Attribution.WriteBatchRequest.Active+1 {
		t.Fatalf("blocked request lifecycle = %+v, before = %+v", blocked.Attribution.WriteBatchRequest, before.Attribution.WriteBatchRequest)
	}
	if _, err := connection.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}

	after, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Attribution.WriteBatchRequest.Attempts != before.Attribution.WriteBatchRequest.Attempts+1 || after.Attribution.WriteBatchRequest.Failures != before.Attribution.WriteBatchRequest.Failures {
		t.Fatalf("completed request = %+v, before = %+v", after.Attribution.WriteBatchRequest, before.Attribution.WriteBatchRequest)
	}
	if after.Attribution.EngineSubmit.Attempts != before.Attribution.EngineSubmit.Attempts+1 || after.Attribution.EngineSubmit.Failures != before.Attribution.EngineSubmit.Failures {
		t.Fatalf("engine submit = %+v, before = %+v", after.Attribution.EngineSubmit, before.Attribution.EngineSubmit)
	}
	if after.Attribution.DurableWait.Attempts != before.Attribution.DurableWait.Attempts+1 {
		t.Fatalf("durable wait = %+v, before = %+v", after.Attribution.DurableWait, before.Attribution.DurableWait)
	}
	if after.Attribution.EngineWriteBatches <= before.Attribution.EngineWriteBatches || after.Attribution.EngineWriteOps <= before.Attribution.EngineWriteOps {
		t.Fatalf("engine counters did not advance: after=%+v before=%+v", after.Attribution, before.Attribution)
	}
	if after.Attribution.EngineBatchQueueDepth != 0 || after.Attribution.EngineBatchQueue.Successes != before.Attribution.EngineBatchQueue.Successes+1 || after.Attribution.EngineBatchQueue.Completed != before.Attribution.EngineBatchQueue.Completed+1 || len(after.Attribution.EngineBatchQueue.LatencyBucketUpperUS) == 0 || len(after.Attribution.EngineBatchQueue.LatencyBucketUpperUS) != len(after.Attribution.EngineBatchQueue.LatencyBucketCounts) {
		t.Fatalf("engine batch queue = %+v, before = %+v", after.Attribution.EngineBatchQueue, before.Attribution.EngineBatchQueue)
	}
	if after.Attribution.EngineBatchService.Successes != before.Attribution.EngineBatchService.Successes+1 || after.Attribution.EngineBatchService.Completed != before.Attribution.EngineBatchService.Completed+1 || len(after.Attribution.EngineBatchService.LatencyBucketUpperUS) == 0 || len(after.Attribution.EngineBatchService.LatencyBucketUpperUS) != len(after.Attribution.EngineBatchService.LatencyBucketCounts) {
		t.Fatalf("engine batch service = %+v, before = %+v", after.Attribution.EngineBatchService, before.Attribution.EngineBatchService)
	}

	takeoverOptions := options
	takeoverOptions.Socket = testSocket(t)
	takeoverOptions.testEnvironment = nil
	takeover, err := Ensure(ctx, takeoverOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer takeover.Close(ctx)
	if _, err := takeover.PromoteWriterWithTakeover(ctx, "attribution fence failure", true, after.CurrentEpoch); err != nil {
		t.Fatal(err)
	}
	fencedWrite := make(chan error, 1)
	go func() {
		_, writeErr := client.WriteBatch(ctx, []Mutation{{Key: []byte("rejected"), Value: []byte("value")}}, nil, true, "")
		fencedWrite <- writeErr
	}()
	fenceBarrier, err := barrier.Accept()
	if err != nil {
		t.Fatal(err)
	}
	name, err = bufio.NewReader(fenceBarrier).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(name) != "VAULTICDB_TEST_MUTATION_BARRIER" {
		t.Fatalf("fenced write barrier = %q", name)
	}
	if _, err := fenceBarrier.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := fenceBarrier.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-fencedWrite; err == nil {
		t.Fatal("post-demotion write succeeded")
	}
	fenced, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fenced.Attribution.FenceCheck.Failures != after.Attribution.FenceCheck.Failures+1 {
		t.Fatalf("failed fence check = %+v, before = %+v", fenced.Attribution.FenceCheck, after.Attribution.FenceCheck)
	}
	if fenced.Attribution.WriteBatchRequest.Failures != after.Attribution.WriteBatchRequest.Failures+1 {
		t.Fatalf("failed write batch request = %+v, before = %+v", fenced.Attribution.WriteBatchRequest, after.Attribution.WriteBatchRequest)
	}
}
