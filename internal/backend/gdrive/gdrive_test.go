package gdrive

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"google.golang.org/api/googleapi"
)

func TestParseConfig(t *testing.T) {
	for _, value := range []string{"gdrive:drive-id:/Backup/Repo", "gdrive:drive-id/Backup/Repo"} {
		config, err := ParseConfig(value)
		if err != nil {
			t.Fatal(err)
		}
		if config.DriveID != "drive-id" || config.Prefix != "Backup/Repo" || config.Connections != 4 {
			t.Fatalf("unexpected config: %#v", config)
		}
	}
}

func TestRecordedResponsesCoverPaginationRangeAndTokenRefresh(t *testing.T) {
	var tokenRequests, listPages int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/token":
			tokenRequests++
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"access_token":"access-token","token_type":"Bearer","expires_in":3600}`)
		case request.URL.Path == "/files" && strings.Contains(request.URL.Query().Get("q"), "name = 'snapshots'"):
			_, _ = io.WriteString(writer, `{"files":[{"id":"folder-snapshots","name":"snapshots","mimeType":"application/vnd.google-apps.folder"}]}`)
		case request.URL.Path == "/files" && request.URL.Query().Get("pageToken") == "":
			listPages++
			_, _ = io.WriteString(writer, `{"nextPageToken":"next","files":[{"id":"file-a","name":"a","size":"6","md5Checksum":"e80b5017098950fc58aad83c8c14978e"}]}`)
		case request.URL.Path == "/files":
			listPages++
			_, _ = io.WriteString(writer, `{"files":[{"id":"file-b","name":"b","size":"3"}]}`)
		case request.URL.Path == "/files/file-a" && request.URL.Query().Get("alt") == "media":
			if request.Header.Get("Range") != "bytes=1-3" {
				t.Errorf("range = %q", request.Header.Get("Range"))
			}
			writer.Header().Set("Content-Length", "3")
			writer.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(writer, "bcd")
		case request.URL.Path == "/files/file-a":
			_, _ = io.WriteString(writer, `{"id":"file-a","name":"a","size":"6","md5Checksum":"e80b5017098950fc58aad83c8c14978e"}`)
		default:
			t.Errorf("unexpected request: %s", request.URL.String())
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	config := NewConfig()
	config.DriveID = "drive-id"
	config.apiEndpoint = server.URL + "/"
	ctx := WithCredentials(context.Background(), Credentials{
		ClientID: "client", ClientSecret: "secret", RefreshToken: "refresh",
		TokenURI: server.URL + "/token", Scopes: []string{"drive.file"},
	})
	opened, err := Open(ctx, config, http.DefaultTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	if err := opened.List(ctx, backend.SnapshotFile, func(info backend.FileInfo) error {
		names = append(names, info.Name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "a,b" || listPages != 2 || tokenRequests != 1 {
		t.Fatalf("names=%v pages=%d tokens=%d", names, listPages, tokenRequests)
	}
	var loaded string
	if err := opened.Load(ctx, backend.Handle{Type: backend.SnapshotFile, Name: "a"}, 3, 1, func(reader io.Reader) error {
		value, err := io.ReadAll(reader)
		loaded = string(value)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if loaded != "bcd" {
		t.Fatalf("loaded %q", loaded)
	}
}

func TestQuotaErrorsRemainRetryable(t *testing.T) {
	store := &gdrive{}
	for _, reason := range []string{"rateLimitExceeded", "userRateLimitExceeded"} {
		err := &googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: reason}}}
		if store.IsPermanentError(err) {
			t.Fatalf("quota reason %q marked permanent", reason)
		}
	}
	if !store.IsPermanentError(&googleapi.Error{Code: http.StatusForbidden}) {
		t.Fatal("authorization failure was not permanent")
	}
}

func TestSaveStatAndRemove(t *testing.T) {
	var uploaded, deleted bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/token":
			_, _ = io.WriteString(writer, `{"access_token":"access-token","token_type":"Bearer","expires_in":3600}`)
		case request.Method == http.MethodGet && request.URL.Path == "/files" && strings.Contains(request.URL.Query().Get("q"), "name = 'snapshots'"):
			_, _ = io.WriteString(writer, `{"files":[{"id":"folder-snapshots","name":"snapshots","mimeType":"application/vnd.google-apps.folder"}]}`)
		case request.Method == http.MethodGet && request.URL.Path == "/files" && strings.Contains(request.URL.Query().Get("q"), "name = 'snapshot-id'"):
			_, _ = io.WriteString(writer, `{"files":[]}`)
		case request.Method == http.MethodPost && strings.Contains(request.URL.Path, "/files"):
			uploaded = true
			_, _ = io.WriteString(writer, `{"id":"uploaded","size":"7","md5Checksum":"321c3cf486ed509164edec1e1981fec8"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/files/uploaded":
			_, _ = io.WriteString(writer, `{"id":"uploaded","name":"snapshot-id","size":"7","md5Checksum":"321c3cf486ed509164edec1e1981fec8"}`)
		case request.Method == http.MethodDelete && request.URL.Path == "/files/uploaded":
			deleted = true
			writer.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.String())
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	opened := openTestBackend(t, server.URL)
	handle := backend.Handle{Type: backend.SnapshotFile, Name: "snapshot-id"}
	if err := opened.Save(context.Background(), handle, backend.NewByteReader([]byte("payload"), opened.Hasher())); err != nil {
		t.Fatal(err)
	}
	info, err := opened.Stat(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 7 || !uploaded {
		t.Fatalf("stat=%#v uploaded=%v", info, uploaded)
	}
	if err := opened.Remove(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("Drive delete was not requested")
	}
}

func TestDuplicateFoldersFailClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/token":
			_, _ = io.WriteString(writer, `{"access_token":"access-token","token_type":"Bearer","expires_in":3600}`)
		case request.URL.Path == "/files":
			_, _ = io.WriteString(writer, `{"files":[{"id":"one","name":"snapshots","mimeType":"application/vnd.google-apps.folder"},{"id":"two","name":"snapshots","mimeType":"application/vnd.google-apps.folder"}]}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	opened := openTestBackend(t, server.URL)
	err := opened.Save(context.Background(), backend.Handle{Type: backend.SnapshotFile, Name: "snapshot-id"}, backend.NewByteReader([]byte("payload"), opened.Hasher()))
	if err == nil || !strings.Contains(err.Error(), "duplicate entries") {
		t.Fatalf("duplicate folder error = %v", err)
	}
}

func openTestBackend(t *testing.T, endpoint string) backend.Backend {
	t.Helper()
	config := NewConfig()
	config.DriveID = "drive-id"
	config.apiEndpoint = endpoint + "/"
	ctx := WithCredentials(context.Background(), Credentials{
		ClientID: "client", ClientSecret: "secret", RefreshToken: "refresh",
		TokenURI: endpoint + "/token", Scopes: []string{"drive"},
	})
	opened, err := Open(ctx, config, http.DefaultTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	return opened
}

func TestLiveGoogleDrive(t *testing.T) {
	clientID := os.Getenv("VAULTIC_TEST_GDRIVE_CLIENT_ID")
	clientSecret := os.Getenv("VAULTIC_TEST_GDRIVE_CLIENT_SECRET")
	refreshToken := os.Getenv("VAULTIC_TEST_GDRIVE_REFRESH_TOKEN")
	driveID := os.Getenv("VAULTIC_TEST_GDRIVE_DRIVE_ID")
	if clientID == "" || clientSecret == "" || refreshToken == "" || driveID == "" {
		t.Log("VAULTIC_TEST_GDRIVE_* credentials not configured; live test disabled")
		return
	}
	config := NewConfig()
	config.DriveID = driveID
	config.RootFolderID = os.Getenv("VAULTIC_TEST_GDRIVE_ROOT_FOLDER_ID")
	config.Prefix = "_vaultic-live-test"
	ctx := WithCredentials(context.Background(), Credentials{
		ClientID: clientID, ClientSecret: clientSecret, RefreshToken: refreshToken,
		Scopes: []string{"https://www.googleapis.com/auth/drive"},
	})
	opened, err := Open(ctx, config, http.DefaultTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	handle := backend.Handle{Type: backend.SnapshotFile, Name: "probe-" + time.Now().UTC().Format("20060102T150405.000000000")}
	defer func() {
		if err := opened.Remove(ctx, handle); err != nil {
			t.Errorf("clean up live Drive test: %v", err)
		}
	}()
	payload := []byte("vaultic-google-drive-live-test")
	if err := opened.Save(ctx, handle, backend.NewByteReader(payload, opened.Hasher())); err != nil {
		t.Fatal(err)
	}
	var loaded []byte
	if err := opened.Load(ctx, handle, 0, 0, func(reader io.Reader) error {
		var readErr error
		loaded, readErr = io.ReadAll(reader)
		return readErr
	}); err != nil {
		t.Fatal(err)
	}
	if string(loaded) != string(payload) {
		t.Fatalf("live Drive payload mismatch")
	}
	if err := opened.Remove(ctx, handle); err != nil {
		t.Fatal(err)
	}
}
