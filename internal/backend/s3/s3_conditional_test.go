package s3

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/options"
)

type fakeS3Object struct {
	body []byte
	etag string
}

type fakeS3Server struct {
	mu                  sync.Mutex
	object              *fakeS3Object
	lastIfMatch         string
	lastIfNoneMatch     string
	unsupportedCond     bool
	ignoreIfMatch       bool
	conflictDropOnce    bool
	serveStaleAfterHead bool
	headObserved        bool
}

func (server *fakeS3Server) handler(writer http.ResponseWriter, request *http.Request) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if request.Method != http.MethodGet && request.Method != http.MethodHead && request.Method != http.MethodPut {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if request.Method == http.MethodPut {
		server.lastIfMatch = request.Header.Get("If-Match")
		server.lastIfNoneMatch = request.Header.Get("If-None-Match")
		if server.unsupportedCond && (server.lastIfMatch != "" || server.lastIfNoneMatch != "") {
			writeS3Error(writer, http.StatusNotImplemented, "NotImplemented")
			return
		}
	}

	if server.object == nil {
		if request.Method == http.MethodPut {
			if server.lastIfMatch != "" {
				writeS3Error(writer, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				writeS3Error(writer, http.StatusBadRequest, "InvalidRequest")
				return
			}
			body = decodeSignedChunked(body)
			server.object = &fakeS3Object{body: body, etag: etagFor(body)}
			writer.Header().Set("ETag", fmt.Sprintf("\"%s\"", server.object.etag))
			writer.WriteHeader(http.StatusOK)
			return
		}
		writeS3Error(writer, http.StatusNotFound, "NoSuchKey")
		return
	}

	if request.Method == http.MethodGet || request.Method == http.MethodHead {
		if request.Method == http.MethodHead && server.serveStaleAfterHead {
			server.headObserved = true
		}
		if request.Method == http.MethodGet && server.headObserved {
			server.headObserved = false
			stale := []byte("one")
			writer.Header().Set("ETag", fmt.Sprintf("\"%s\"", etagFor(stale)))
			writer.Header().Set("Content-Length", fmt.Sprintf("%d", len(stale)))
			writer.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write(stale)
			return
		}
		writer.Header().Set("ETag", fmt.Sprintf("\"%s\"", server.object.etag))
		writer.Header().Set("Content-Length", fmt.Sprintf("%d", len(server.object.body)))
		writer.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		writer.WriteHeader(http.StatusOK)
		if request.Method == http.MethodGet {
			_, _ = writer.Write(server.object.body)
		}
		return
	}

	if server.lastIfNoneMatch == "*" {
		if server.conflictDropOnce {
			server.conflictDropOnce = false
			server.object = nil
		}
		writeS3Error(writer, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}

	if server.lastIfMatch != "" && !server.ignoreIfMatch {
		if strings.Trim(server.lastIfMatch, "\"") != server.object.etag {
			writeS3Error(writer, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		writeS3Error(writer, http.StatusBadRequest, "InvalidRequest")
		return
	}
	body = decodeSignedChunked(body)
	server.object = &fakeS3Object{body: body, etag: etagFor(body)}
	writer.Header().Set("ETag", fmt.Sprintf("\"%s\"", server.object.etag))
	writer.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	writer.WriteHeader(http.StatusOK)
}

func decodeSignedChunked(payload []byte) []byte {
	if !bytes.Contains(payload, []byte("chunk-signature=")) {
		return payload
	}
	reader := bufio.NewReader(bytes.NewReader(payload))
	decoded := make([]byte, 0, len(payload))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return payload
		}
		line = strings.TrimSpace(line)
		sizeField := line
		if marker := strings.IndexByte(sizeField, ';'); marker >= 0 {
			sizeField = sizeField[:marker]
		}
		size, err := strconv.ParseInt(sizeField, 16, 64)
		if err != nil || size < 0 {
			return payload
		}
		if size == 0 {
			return decoded
		}
		chunk := make([]byte, size)
		if _, err := io.ReadFull(reader, chunk); err != nil {
			return payload
		}
		decoded = append(decoded, chunk...)
		if _, err := reader.ReadString('\n'); err != nil {
			return payload
		}
	}
}

func writeS3Error(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/xml")
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte("<Error><Code>" + code + "</Code><Message>test</Message></Error>"))
}

func etagFor(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

func openTestConditionalBackend(t *testing.T, server *httptest.Server, provider string) backend.Backend {
	t.Helper()
	endpoint := strings.TrimPrefix(server.URL, "http://")
	store, err := Open(
		context.Background(),
		Config{
			Provider: provider, Endpoint: endpoint, UseHTTP: true, Region: "us-east-1",
			Bucket: "bucket", Prefix: "repo", BucketLookup: "path",
			KeyID: "test", Secret: options.NewSecretString("test-secret"),
		},
		server.Client().Transport,
		t.Logf,
	)
	if err != nil {
		t.Fatal(err)
	}
	if conditional := backend.AsBackend[*conditionalS3](store); conditional != nil {
		conditional.conditionalCreate = "strict"
		conditional.conditionalOverwrite = "strict"
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestConditionalWriterRejectsUnverifiedOverwriteCAS(t *testing.T) {
	fake := &fakeS3Server{ignoreIfMatch: true}
	httpServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer httpServer.Close()

	store := openTestConditionalBackend(t, httpServer, "ceph")
	conditional := backend.AsBackend[*conditionalS3](store)
	conditional.conditionalOverwrite = "unverified"
	handle := backend.Handle{Type: backend.StagingFile, Name: "control/state.json"}
	_, swapped, err := conditional.CompareAndSwap(t.Context(), handle, []byte("one"), []byte("two"))
	if !errors.Is(err, backend.ErrConditionalWriteUnsupported) || swapped {
		t.Fatalf("unverified overwrite CAS = (swapped %v, err %v)", swapped, err)
	}
}

func TestConditionalWriterVerifiesStrictOverwriteCAS(t *testing.T) {
	fake := &fakeS3Server{}
	httpServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer httpServer.Close()

	store := openTestConditionalBackend(t, httpServer, "ceph")
	conditional := backend.AsBackend[*conditionalS3](store)
	conditional.conditionalCreate = "unverified"
	conditional.conditionalOverwrite = "unverified"
	if err := conditional.probeConditionalWrites(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !conditional.strictConditionalOverwrite() {
		t.Fatal("expected stale ETag rejection to verify strict overwrite CAS")
	}
}

func TestConditionalWriterUsesProviderPreconditions(t *testing.T) {
	fake := &fakeS3Server{}
	httpServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer httpServer.Close()

	store := openTestConditionalBackend(t, httpServer, "ceph")
	writer := backend.AsCapability[backend.ConditionalWriter](store)
	if writer == nil {
		t.Fatal("conditional writer capability was not exposed")
	}
	handle := backend.Handle{Type: backend.StagingFile, Name: "control/state.json"}

	current, swapped, err := writer.CompareAndSwap(t.Context(), handle, nil, []byte("one"))
	if err != nil || !swapped || current != nil {
		t.Fatalf("create-if-missing = (%q, %v, %v)", current, swapped, err)
	}
	if fake.lastIfNoneMatch != "*" {
		t.Fatalf("If-None-Match = %q, want *", fake.lastIfNoneMatch)
	}

	current, swapped, err = writer.CompareAndSwap(t.Context(), handle, nil, []byte("two"))
	if err != nil || swapped || !bytes.Equal(current, []byte("one")) {
		t.Fatalf("conflicting create = (%q, %v, %v)", current, swapped, err)
	}

	current, swapped, err = writer.CompareAndSwap(t.Context(), handle, []byte("one"), []byte("two"))
	if err != nil || !swapped || !bytes.Equal(current, []byte("two")) {
		t.Fatalf("matching update = (%q, %v, %v)", current, swapped, err)
	}
	if fake.lastIfMatch == "" {
		t.Fatal("If-Match header was not sent")
	}

	current, swapped, err = writer.CompareAndSwap(t.Context(), handle, []byte("one"), []byte("three"))
	if err != nil || swapped || !bytes.Equal(current, []byte("two")) {
		t.Fatalf("stale expected = (%q, %v, %v)", current, swapped, err)
	}
}

func TestConditionalWriterBindsComparisonAndETagToOneGet(t *testing.T) {
	fake := &fakeS3Server{
		object:              &fakeS3Object{body: []byte("two"), etag: etagFor([]byte("two"))},
		serveStaleAfterHead: true,
	}
	httpServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer httpServer.Close()
	store := openTestConditionalBackend(t, httpServer, "ceph")
	writer := backend.AsCapability[backend.ConditionalWriter](store)
	handle := backend.Handle{Type: backend.StagingFile, Name: "control/state.json"}

	current, swapped, err := writer.CompareAndSwap(t.Context(), handle, []byte("one"), []byte("three"))
	if err != nil || swapped || !bytes.Equal(current, []byte("two")) {
		t.Fatalf("CAS with stale-after-HEAD server = (%q, %v, %v), want current two without swap", current, swapped, err)
	}
	fake.mu.Lock()
	headObserved := fake.headObserved
	fake.mu.Unlock()
	if headObserved {
		t.Fatal("CAS issued a HEAD before reading comparison bytes")
	}
}

func TestConditionalWriterCreateConflictReadbackNotFoundIsBenign(t *testing.T) {
	fake := &fakeS3Server{}
	httpServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer httpServer.Close()

	store := openTestConditionalBackend(t, httpServer, "ceph")
	writer := backend.AsCapability[backend.ConditionalWriter](store)
	if writer == nil {
		t.Fatal("conditional writer capability was not exposed")
	}
	handle := backend.Handle{Type: backend.StagingFile, Name: "control/state.json"}

	if _, swapped, err := writer.CompareAndSwap(t.Context(), handle, nil, []byte("seed")); err != nil || !swapped {
		t.Fatalf("seed create failed: swapped=%v err=%v", swapped, err)
	}

	fake.mu.Lock()
	fake.conflictDropOnce = true
	fake.mu.Unlock()

	current, swapped, err := writer.CompareAndSwap(t.Context(), handle, nil, []byte("next"))
	if err != nil {
		t.Fatalf("conflict readback not-found should be benign, got err=%v", err)
	}
	if swapped {
		t.Fatal("expected swapped=false when create-if-missing conflicts")
	}
	if current != nil {
		t.Fatalf("expected nil current on conflict readback not-found, got %q", current)
	}
}

func TestConditionalWriterUnsupportedPathFailsClosed(t *testing.T) {
	fake := &fakeS3Server{unsupportedCond: true}
	httpServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer httpServer.Close()

	store := openTestConditionalBackend(t, httpServer, "ceph")
	writer := backend.AsCapability[backend.ConditionalWriter](store)
	if writer == nil {
		t.Fatal("conditional writer capability was not exposed")
	}
	_, _, err := writer.CompareAndSwap(
		t.Context(),
		backend.Handle{Type: backend.StagingFile, Name: "control/state.json"},
		nil,
		[]byte("one"),
	)
	if !errors.Is(err, backend.ErrConditionalWriteUnsupported) {
		t.Fatalf("CompareAndSwap() error = %v, want ErrConditionalWriteUnsupported", err)
	}
}

func TestGenericProviderDoesNotExposeConditionalWriter(t *testing.T) {
	fake := &fakeS3Server{}
	httpServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer httpServer.Close()

	store := openTestConditionalBackend(t, httpServer, "generic")
	writer := backend.AsCapability[backend.ConditionalWriter](store)
	if writer != nil {
		t.Fatal("generic provider unexpectedly exposed conditional writer")
	}
}
