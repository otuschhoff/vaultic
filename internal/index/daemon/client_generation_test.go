package daemon

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func TestSchemaStoreImportsSameLegacyPackConcurrently(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase4-concurrent-pack", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	packID := daemonTestID(20)
	imports := []LegacyPackImport{
		{
			SourceIndex: daemonTestID(21), PackID: packID,
			Record: schema.PackRecord{
				Type:              schema.PackData,
				PhysicalSize:      20,
				PhysicalSizeKnown: true,
				PayloadSize:       8,
				HeaderSize:        12,
				BlobCount:         1,
				Lifecycle:         schema.PackImported,
			},
			Blobs: map[schema.ID]schema.BlobRecord{
				daemonTestID(22): {Locations: []schema.BlobLocation{{PackID: packID, Offset: 0, Length: 8, Type: schema.BlobData}}},
			},
		},
		{
			SourceIndex: daemonTestID(23), PackID: packID,
			Record: schema.PackRecord{
				Type:              schema.PackData,
				PhysicalSize:      20,
				PhysicalSizeKnown: true,
				PayloadSize:       8,
				HeaderSize:        12,
				BlobCount:         1,
				Lifecycle:         schema.PackImported,
			},
			Blobs: map[schema.ID]schema.BlobRecord{
				daemonTestID(24): {Locations: []schema.BlobLocation{{PackID: packID, Offset: 8, Length: 8, Type: schema.BlobData}}},
			},
		},
	}
	errors := make([]error, len(imports))
	var group sync.WaitGroup
	for index := range imports {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			errors[index] = store.ImportLegacyPack(context.Background(), imports[index])
		}(index)
	}
	group.Wait()
	for _, err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	packValue, found, err := store.Get(context.Background(), schema.PackKey(packID))
	if err != nil || !found {
		t.Fatalf("read pack: found=%t err=%v", found, err)
	}
	record, err := schema.UnmarshalPackRecord(packValue)
	if err != nil || record.BlobCount != 2 || record.PayloadSize != 16 || record.HeaderSize != 4 || len(record.SourceIndexIDs) != 2 {
		t.Fatalf("concurrent pack record = %#v, err=%v", record, err)
	}
	aggregateValue, found, err := store.Get(context.Background(), schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("read aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 1 || aggregate.PayloadSize != 16 || aggregate.BlobCount != 2 || aggregate.PhysicalSize != 20 {
		t.Fatalf("concurrent aggregate = %#v, err=%v", aggregate, err)
	}
}

func TestSchemaStoreImportsLargeLegacyPackAcrossBatches(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase7-large-pack", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	packID := daemonTestID(100)
	blobs := make(map[schema.ID]schema.BlobRecord, 73)
	for index := range 73 {
		blobID := daemonTestID(byte(index + 101))
		blobs[blobID] = schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: packID, Offset: uint64(index), Length: 1, Type: schema.BlobData}}}
	}
	imported := LegacyPackImport{
		SourceIndex: daemonTestID(99), PackID: packID,
		Record: schema.PackRecord{
			Type:              schema.PackData,
			PhysicalSize:      73,
			PhysicalSizeKnown: true,
			PayloadSize:       73,
			BlobCount:         73,
			Lifecycle:         schema.PackImported,
		},
		Blobs: blobs, BatchSize: 1,
	}
	if err := store.ImportLegacyPack(context.Background(), imported); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(context.Background(), schema.PackKey(packID)); err != nil || !found {
		t.Fatalf("read pack after batched import: found=%t err=%v", found, err)
	}
	for blobID := range blobs {
		if _, found, err := store.Get(context.Background(), schema.BlobKey(blobID)); err != nil || !found {
			t.Fatalf("read blob %x after batched import: found=%t err=%v", blobID, found, err)
		}
	}
}

func TestManifestBatchingRespectsNegotiatedLimits(t *testing.T) {
	keys := make([][]byte, 200)
	values := make([][]byte, len(keys))
	for index := range keys {
		keys[index] = make([]byte, 39)
		values[index] = make([]byte, 128*1024)
	}
	limits := Limits{MaxBatchItems: 10_000, MaxMessageBytes: 16 * 1024 * 1024}
	batches := 0
	for start := 0; start < len(keys); batches++ {
		end, err := manifestBatchEnd(limits, keys, values, start)
		if err != nil {
			t.Fatal(err)
		}
		if end <= start || end > len(keys) {
			t.Fatalf("manifest batch %d made invalid progress %d -> %d", batches, start, end)
		}
		start = end
	}
	if batches < 2 {
		t.Fatalf("expected multiple manifest batches, got %d", batches)
	}
}

func TestS3CompatibleStorageRoundTrip(t *testing.T) {
	if os.Getenv("VAULTICDB_TEST_S3_ENDPOINT") == "" {
		t.Skip("VAULTICDB_TEST_S3_ENDPOINT is not configured")
	}
	options := Options{
		Socket: testSocket(t), RepositoryID: "phase3-s3", DaemonPath: daemonBinary(t),
		ObjectStore: "s3", S3Bucket: os.Getenv("VAULTICDB_TEST_S3_BUCKET"), S3Prefix: "phase3/roundtrip",
	}
	if options.S3Bucket == "" {
		options.S3Bucket = "vaulticdb-phase3"
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	durable, err := client.WriteBatch(ctx, []Mutation{{Key: []byte("s3:key"), Value: []byte("s3-value")}}, nil, true, "")
	if err != nil || !durable {
		t.Fatalf("S3 write = %t, %v", durable, err)
	}
	value, found, err := client.Get(ctx, []byte("s3:key"), "")
	if err != nil || !found || string(value) != "s3-value" {
		t.Fatalf("S3 read = %q, %t, %v", value, found, err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	options.Socket = testSocket(t)
	options.RepositoryID = "phase3-s3-other"
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if value, found, err := client.Get(ctx, []byte("s3:key"), ""); err != nil || found {
		t.Fatalf("S3 repository isolation read = %q, %t, %v", value, found, err)
	}
}

type schemaRecord interface{ MarshalBinary() ([]byte, error) }

func daemonTestID(value byte) schema.ID { var id schema.ID; id[0], id[31] = value, value; return id }

func encodeSchemaRecord(t *testing.T, record schemaRecord) []byte {
	t.Helper()
	encoded, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mustNextRevision(t *testing.T, next uint64) []byte {
	t.Helper()
	encoded, err := schema.MarshalNextRevision(next)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestEverySchemaRecordPersistsByteForByteAcrossRestart(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase3-all-records", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
	ctx := context.Background()
	id1, id2, id3 := daemonTestID(1), daemonTestID(2), daemonTestID(3)
	inodeKey := schema.InodeRevisionKey(1, 2, 1)
	directoryKey := schema.DirectoryRevisionKey(1, 1, 1)
	inodePointer, _ := (schema.CurrentPointer{Revision: 1, RecordKey: inodeKey}).MarshalBinary()
	directoryPointer, _ := (schema.CurrentPointer{Revision: 1, RecordKey: directoryKey}).MarshalBinary()
	nextRevision, _ := schema.MarshalNextRevision(2)
	indexCheckpoint := encodeSchemaRecord(t, schema.ImportCheckpointRecord{PacksImported: 1, BlobsImported: 2})
	snapshotCheckpoint := encodeSchemaRecord(t, schema.SnapshotImportCheckpointRecord{TreesVisited: 1, NodesImported: 2, DebtsCreated: 3})
	records := []Mutation{
		{
			Key: schema.BlobKey(id1),
			Value: encodeSchemaRecord(
				t,
				schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: id2, Offset: 1, Length: 2, UncompressedSize: 3, Type: schema.BlobData}}},
			),
		},
		{
			Key: schema.PackKey(id2),
			Value: encodeSchemaRecord(
				t,
				schema.PackRecord{
					Type:              schema.PackData,
					PhysicalSize:      3,
					PhysicalSizeKnown: true,
					PayloadSize:       2,
					HeaderSize:        1,
					BlobCount:         1,
					Lifecycle:         schema.PackPublished,
				},
			),
		},
		{
			Key:   schema.PackAggregateKey(schema.AggregateAll),
			Value: encodeSchemaRecord(t, schema.PackAggregate{PackCount: 1, PhysicalSize: 3, PayloadSize: 2, HeaderSize: 1, BlobCount: 1, UpdateSequence: 1}),
		},
		{Key: schema.CurrentInodeKey(1, 2), Value: inodePointer},
		{
			Key: inodeKey,
			Value: encodeSchemaRecord(
				t,
				schema.InodeRevision{
					ParentInode:  1,
					Known:        schema.KnownParent,
					ContentMode:  schema.ContentInline,
					ContentIDs:   []schema.ID{id1},
					ContentCount: 1,
					Freshness:    schema.FreshnessVerified,
				},
			),
		},
		{Key: schema.CurrentDirectoryKey(1, 1), Value: directoryPointer},
		{
			Key: directoryKey,
			Value: encodeSchemaRecord(
				t,
				schema.DirectoryRevision{Children: []schema.DirectoryChild{{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: inodeKey}}},
			),
		},
		{
			Key:   schema.SnapshotKey(id3),
			Value: encodeSchemaRecord(t, schema.SnapshotRecord{CommitSequence: 1, RootFSID: 1, RootInode: 1, RootRevision: 1, OriginalJSON: []byte("{}")}),
		},
		{
			Key:   schema.ContentManifestKey(id3, 0),
			Value: encodeSchemaRecord(t, schema.ContentManifest{TotalCount: 1, SegmentCount: 1, ContentIDs: []schema.ID{id1}}),
		},
		{Key: schema.ReverseManifestKey(id1, id3), Value: encodeSchemaRecord(t, schema.ReverseManifestRecord{State: schema.ReferenceCurrent})},
		{Key: schema.ReverseInodeKey(id1, 1, 2), Value: encodeSchemaRecord(t, schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceCurrent})},
		{
			Key: schema.ReferenceCountKey(id1),
			Value: encodeSchemaRecord(
				t,
				schema.ReferenceCountRecord{
					TotalReferences:    2,
					DistinctInodes:     1,
					DistinctRevisions:  1,
					DistinctManifests:  1,
					ReachableSnapshots: 1,
					UpdateSequence:     1,
				},
			),
		},
		{
			Key:   schema.GarbageCollectionKey(schema.GCBlob, id1),
			Value: encodeSchemaRecord(t, schema.GarbageCollectionRecord{State: schema.GCCandidate, ObservedCommit: 1}),
		},
		{
			Key:   schema.CrawlDebtKey(id3, id2),
			Value: encodeSchemaRecord(t, schema.CrawlDebtRecord{PathOrTree: []byte("tree"), Reason: schema.DebtMissingDirectory, Status: schema.DebtPending}),
		},
		{Key: schema.ImportCheckpointKey(id1), Value: indexCheckpoint},
		{Key: schema.SnapshotImportCheckpointKey(id3), Value: snapshotCheckpoint},
		{Key: schema.NextRevisionKey(), Value: nextRevision},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := client.WriteBatch(ctx, records, nil, true, "")
	if err != nil || !durable {
		t.Fatalf("write all records = %t, %v", durable, err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	keys := make([][]byte, len(records))
	for index := range records {
		keys[index] = records[index].Key
	}
	values, found, err := client.MultiGet(ctx, keys, "")
	if err != nil {
		t.Fatal(err)
	}
	for index := range records {
		if !found[index] || !bytes.Equal(values[index].Value, records[index].Value) {
			t.Fatalf("record %d changed across restart", index)
		}
	}
}

type testService struct {
	vaulticdbv1.UnimplementedVaulticDBServer
	protocol      string
	schema        string
	repo          string
	blockShutdown bool
	corruptKeys   bool
}

func testSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "vd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "d.sock")
}

func daemonBinary(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("VAULTICDB_TEST_BINARY"); binary != "" {
		return binary
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	binary := filepath.Join(filepath.Dir(source), "..", "..", "..", "vaulticdb", "target", "debug", "vaulticdb")
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	return binary
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func (s testService) Health(_ context.Context, request *vaulticdbv1.HealthRequest) (*vaulticdbv1.HealthResponse, error) {
	return &vaulticdbv1.HealthResponse{Ready: true, ProtocolVersion: s.protocol, SchemaVersion: s.schema, RepositoryId: request.GetRepositoryId()}, nil
}

func (s testService) Capabilities(_ context.Context, request *vaulticdbv1.CapabilitiesRequest) (*vaulticdbv1.CapabilitiesResponse, error) {
	return &vaulticdbv1.CapabilitiesResponse{
		ProtocolVersion: s.protocol, SchemaVersion: s.schema, RepositoryId: request.GetRepositoryId(),
		MaxBatchItems: 10_000, MaxMessageBytes: 16 * 1024 * 1024, MaxPageItems: 1_000,
	}, nil
}

func (s testService) Shutdown(ctx context.Context, _ *vaulticdbv1.Empty) (*vaulticdbv1.Empty, error) {
	if s.blockShutdown {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &vaulticdbv1.Empty{}, nil
}

func (s testService) Get(_ context.Context, request *vaulticdbv1.GetRequest) (*vaulticdbv1.GetResponse, error) {
	key := request.GetKey()
	if s.corruptKeys {
		key = []byte("wrong-key")
	}
	return &vaulticdbv1.GetResponse{Found: true, Key: key, Value: []byte("value")}, nil
}

func (s testService) MultiGet(_ context.Context, request *vaulticdbv1.MultiGetRequest) (*vaulticdbv1.MultiGetResponse, error) {
	results := make([]*vaulticdbv1.GetResponse, len(request.GetKeys()))
	for index, requestKey := range request.GetKeys() {
		key := requestKey
		if s.corruptKeys {
			key = []byte("wrong-key")
		}
		results[index] = &vaulticdbv1.GetResponse{Found: true, Key: key, Value: []byte("value")}
	}
	return &vaulticdbv1.MultiGetResponse{Results: results}, nil
}

func TestOptionsDefaults(t *testing.T) {
	options := (Options{}).withDefaults()
	if options.Socket != DefaultSocket("") || options.StartTimeout != 10*time.Second || options.RetryInterval != 25*time.Millisecond {
		t.Fatalf("unexpected defaults: %#v", options)
	}
}

func TestDefaultRPCContextHasDeadline(t *testing.T) {
	ctx, cancel := withDefaultRPCDeadline(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	remaining := time.Until(deadline)
	if !ok || remaining <= 0 || remaining > defaultRPCDeadline {
		t.Fatalf("default RPC deadline = %v, %t", deadline, ok)
	}
}

func TestDefaultSocketIsRepositoryScoped(t *testing.T) {
	if DefaultSocket("first") == DefaultSocket("second") {
		t.Fatal("repository-scoped socket paths must differ")
	}
}

func TestConnectValidatesDaemon(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: ProtocolVersion, schema: SchemaVersion})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "test-repo"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClientRejectsWrongResponseKeys(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: ProtocolVersion, schema: SchemaVersion, corruptKeys: true})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "test-repo"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	if _, _, err := client.Get(context.Background(), []byte("requested-key"), ""); err == nil || !strings.Contains(err.Error(), "wrong key") {
		t.Fatalf("expected wrong point-read key error, got %v", err)
	}
	if _, _, err := client.MultiGet(context.Background(), [][]byte{[]byte("first"), []byte("second")}, ""); err == nil ||
		!strings.Contains(err.Error(), "out-of-order") {
		t.Fatalf("expected out-of-order multi-get error, got %v", err)
	}
}

func TestConnectRejectsIncompatibleDaemon(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: "vaulticdb.v0", schema: SchemaVersion})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	if _, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "test-repo"}); err == nil {
		t.Fatal("expected incompatible daemon error")
	}
}

func TestSocketDir(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "runtime", "vaulticdb.sock")
	if got, want := SocketDir(socket), filepath.Join(dir, "runtime"); got != want {
		t.Fatalf("SocketDir() = %q, want %q", got, want)
	}
}

func TestEnsureStartsDaemonRecoversStaleSocketAndCleansUp(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	client, err := Ensure(context.Background(), Options{Socket: socket, RepositoryID: "test-repo", DaemonPath: daemonBinary(t)})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(socket)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket permissions: %v, %v", info.Mode(), err)
	}
	dir, err := os.Stat(filepath.Dir(socket))
	if err != nil || dir.Mode().Perm() != 0o700 {
		t.Fatalf("directory permissions: %v, %v", dir.Mode(), err)
	}
	capabilities, err := client.RPC().
		Capabilities(context.Background(), &vaulticdbv1.CapabilitiesRequest{RepositoryId: "test-repo", Context: requestContext(context.Background())})
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GetUnixSocket() || capabilities.GetTcpEnabled() || capabilities.GetMaxConcurrentRequests() != 128 {
		t.Fatalf("unexpected Unix transport capabilities: %#v", capabilities)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket remains after shutdown: %v", err)
	}
}

func TestEnsureRacesToOneDaemon(t *testing.T) {
	socket := testSocket(t)
	options := Options{Socket: socket, RepositoryID: "race-repo", DaemonPath: daemonBinary(t)}
	clients := make([]*Client, 4)
	errs := make([]error, len(clients))
	var group sync.WaitGroup
	for index := range clients {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			clients[index], errs[index] = Ensure(context.Background(), options)
		}(index)
	}
	group.Wait()
	var owner *Client
	for index, client := range clients {
		if errs[index] != nil {
			t.Fatal(errs[index])
		}
		if client.process != nil {
			owner = client
		} else if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if owner == nil {
		t.Fatal("no client owned the daemon")
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
