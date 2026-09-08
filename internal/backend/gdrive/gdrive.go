// Package gdrive provides a native Google Drive backend.
package gdrive

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/layout"
	"github.com/otuschhoff/vaultic/internal/backend/location"
	"github.com/otuschhoff/vaultic/internal/backend/util"
	"github.com/otuschhoff/vaultic/internal/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const folderMIME = "application/vnd.google-apps.folder"

type Credentials struct {
	ClientID           string
	ClientSecret       string
	RefreshToken       string
	TokenURI           string
	Scopes             []string
	ServiceAccountJSON []byte
	Subject            string
}

type credentialContextKey struct{}

func WithCredentials(ctx context.Context, credentials Credentials) context.Context {
	return context.WithValue(ctx, credentialContextKey{}, credentials)
}

type gdrive struct {
	service     *drive.Service
	driveID     string
	rootID      string
	connections uint
	cacheMu     sync.RWMutex
	pathIDs     map[string]string
	folderLocks sync.Map
	layout.Layout
}

var _ backend.Backend = (*gdrive)(nil)

func NewFactory() location.Factory {
	return location.NewHTTPBackendFactory("gdrive", ParseConfig, location.NoPassword, Create, Open)
}

func Open(ctx context.Context, config Config, transport http.RoundTripper, _ func(string, ...any)) (backend.Backend, error) {
	credentials, ok := ctx.Value(credentialContextKey{}).(Credentials)
	if !ok {
		return nil, errors.New("gdrive credentials must be supplied by a broker lease")
	}
	client, err := oauthClient(ctx, transport, credentials)
	if err != nil {
		return nil, err
	}
	serviceOptions := []option.ClientOption{option.WithHTTPClient(client)}
	if config.apiEndpoint != "" {
		serviceOptions = append(serviceOptions, option.WithEndpoint(config.apiEndpoint))
	}
	service, err := drive.NewService(ctx, serviceOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "create Google Drive service")
	}
	rootID := config.RootFolderID
	if rootID == "" {
		rootID = "root"
	}
	return &gdrive{
		service:     service,
		driveID:     config.DriveID,
		rootID:      rootID,
		connections: config.Connections,
		pathIDs:     map[string]string{"": rootID},
		Layout:      layout.NewDefaultLayout(config.Prefix, path.Join),
	}, nil
}

func Create(ctx context.Context, config Config, transport http.RoundTripper, log func(string, ...any)) (backend.Backend, error) {
	return Open(ctx, config, transport, log)
}

func oauthClient(ctx context.Context, transport http.RoundTripper, credentials Credentials) (*http.Client, error) {
	base := &http.Client{Transport: transport}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, base)
	scopes := credentials.Scopes
	if len(scopes) == 0 {
		scopes = []string{drive.DriveScope}
	}
	var source oauth2.TokenSource
	if len(credentials.ServiceAccountJSON) != 0 {
		config, err := google.JWTConfigFromJSON(credentials.ServiceAccountJSON, scopes...)
		if err != nil {
			return nil, errors.Wrap(err, "decode Google service-account credential")
		}
		config.Subject = credentials.Subject
		source = config.TokenSource(ctx)
	} else {
		if credentials.ClientID == "" || credentials.ClientSecret == "" || credentials.RefreshToken == "" {
			return nil, errors.New("Google Drive OAuth credential is incomplete")
		}
		tokenURI := credentials.TokenURI
		if tokenURI == "" {
			tokenURI = google.Endpoint.TokenURL
		}
		config := oauth2.Config{
			ClientID: credentials.ClientID, ClientSecret: credentials.ClientSecret,
			Endpoint: oauth2.Endpoint{TokenURL: tokenURI}, Scopes: scopes,
		}
		source = config.TokenSource(ctx, &oauth2.Token{RefreshToken: credentials.RefreshToken})
	}
	return oauth2.NewClient(ctx, source), nil
}

func (store *gdrive) Properties() backend.Properties {
	return backend.Properties{Connections: store.connections, HasAtomicReplace: false, HasFlakyErrors: true}
}

func (store *gdrive) Hasher() hash.Hash { return md5.New() }

func (store *gdrive) Save(ctx context.Context, handle backend.Handle, reader backend.RewindReader) error {
	name := store.Filename(handle)
	parent, base := path.Split(name)
	parentID, err := store.ensureFolder(ctx, strings.TrimSuffix(parent, "/"))
	if err != nil {
		return err
	}
	if existing, err := store.findChild(ctx, parentID, base, false); err != nil {
		return err
	} else if existing != nil {
		if existing.Size == reader.Length() && strings.EqualFold(existing.Md5Checksum, hex.EncodeToString(reader.Hash())) {
			store.cache(name, existing.Id)
			return nil
		}
		return errors.Errorf("Google Drive object %q already exists with different content", name)
	}
	file := &drive.File{Name: base, Parents: []string{parentID}}
	created, err := store.service.Files.Create(file).
		Media(reader, googleapi.ContentType("application/octet-stream")).
		SupportsAllDrives(true).
		Fields("id,size,md5Checksum").
		Context(ctx).
		Do()
	if err != nil {
		return errors.WithStack(err)
	}
	if created.Size != reader.Length() {
		return errors.Errorf("wrote %d bytes instead of the expected %d bytes", created.Size, reader.Length())
	}
	store.cache(name, created.Id)
	return nil
}

func (store *gdrive) Load(ctx context.Context, handle backend.Handle, length int, offset int64, consume func(io.Reader) error) error {
	return util.DefaultLoad(ctx, handle, length, offset, store.openReader, consume)
}

func (store *gdrive) openReader(ctx context.Context, handle backend.Handle, length int, offset int64) (io.ReadCloser, error) {
	file, err := store.file(ctx, store.Filename(handle))
	if err != nil {
		return nil, err
	}
	request := store.service.Files.Get(file.Id).SupportsAllDrives(true).Context(ctx)
	if length > 0 {
		request.Header().Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(length)-1))
	} else if offset > 0 {
		request.Header().Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	response, err := request.Download()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if length > 0 && response.ContentLength != int64(length) {
		_ = response.Body.Close() // Preserve the short-read error; response cleanup is best effort.
		return nil, &googleapi.Error{Code: http.StatusRequestedRangeNotSatisfiable, Message: "vaultic-file-too-short"}
	}
	return response.Body, nil
}

func (store *gdrive) Stat(ctx context.Context, handle backend.Handle) (backend.FileInfo, error) {
	file, err := store.file(ctx, store.Filename(handle))
	if err != nil {
		return backend.FileInfo{}, err
	}
	return backend.FileInfo{Name: handle.Name, Size: file.Size}, nil
}

func (store *gdrive) Remove(ctx context.Context, handle backend.Handle) error {
	name := store.Filename(handle)
	file, err := store.file(ctx, name)
	if store.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	err = store.service.Files.Delete(file.Id).SupportsAllDrives(true).Context(ctx).Do()
	if store.IsNotExist(err) {
		err = nil
	}
	if err == nil {
		store.uncache(name)
	}
	return errors.WithStack(err)
}

func (store *gdrive) List(ctx context.Context, fileType backend.FileType, callback func(backend.FileInfo) error) error {
	directory, _ := store.Basedir(fileType)
	folderID, err := store.resolve(ctx, directory, false)
	if store.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	pageToken := ""
	for {
		call := store.service.Files.List().Q(fmt.Sprintf("'%s' in parents and trashed = false", escapeQuery(folderID))).
			SupportsAllDrives(true).IncludeItemsFromAllDrives(true).
			Fields("nextPageToken,files(id,name,size,md5Checksum,mimeType)").PageSize(1000).Context(ctx)
		if store.driveID != "" {
			call = call.Corpora("drive").DriveId(store.driveID)
		}
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		result, err := call.Do()
		if err != nil {
			return errors.WithStack(err)
		}
		for _, file := range result.Files {
			if file.MimeType == folderMIME {
				continue
			}
			store.cache(path.Join(directory, file.Name), file.Id)
			if err := callback(backend.FileInfo{Name: file.Name, Size: file.Size}); err != nil {
				return err
			}
		}
		if result.NextPageToken == "" {
			return nil
		}
		pageToken = result.NextPageToken
	}
}

func (store *gdrive) Delete(ctx context.Context) error { return util.DefaultDelete(ctx, store) }
func (store *gdrive) Close() error                     { return nil }
func (store *gdrive) Warmup(context.Context, []backend.Handle) ([]backend.Handle, error) {
	return nil, nil
}
func (store *gdrive) WarmupWait(context.Context, []backend.Handle) error { return nil }

func (store *gdrive) IsNotExist(err error) bool {
	var apiError *googleapi.Error
	return errors.As(err, &apiError) && apiError.Code == http.StatusNotFound
}

func (store *gdrive) IsPermanentError(err error) bool {
	if store.IsNotExist(err) {
		return true
	}
	var apiError *googleapi.Error
	if !errors.As(err, &apiError) {
		return false
	}
	if apiError.Code == http.StatusRequestedRangeNotSatisfiable || apiError.Code == http.StatusUnauthorized {
		return true
	}
	if apiError.Code == http.StatusForbidden {
		for _, item := range apiError.Errors {
			if item.Reason == "rateLimitExceeded" || item.Reason == "userRateLimitExceeded" {
				return false
			}
		}
		return true
	}
	return false
}

func (store *gdrive) ensureFolder(ctx context.Context, name string) (string, error) {
	if name == "" || name == "." {
		return store.rootID, nil
	}
	lockValue, _ := store.folderLocks.LoadOrStore(name, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	if id, ok := store.cached(name); ok {
		return id, nil
	}
	parent, base := path.Split(name)
	parentID, err := store.ensureFolder(ctx, strings.TrimSuffix(parent, "/"))
	if err != nil {
		return "", err
	}
	found, err := store.findChild(ctx, parentID, base, true)
	if err != nil {
		return "", err
	}
	if found == nil {
		found, err = store.service.Files.Create(&drive.File{Name: base, MimeType: folderMIME, Parents: []string{parentID}}).
			SupportsAllDrives(true).Fields("id,name,mimeType").Context(ctx).Do()
		if err != nil {
			return "", errors.WithStack(err)
		}
	}
	store.cache(name, found.Id)
	return found.Id, nil
}

func (store *gdrive) resolve(ctx context.Context, name string, create bool) (string, error) {
	name = strings.Trim(path.Clean(name), "/")
	if name == "." || name == "" {
		return store.rootID, nil
	}
	if id, ok := store.cached(name); ok {
		return id, nil
	}
	if create {
		return store.ensureFolder(ctx, name)
	}
	parent, base := path.Split(name)
	parentID, err := store.resolve(ctx, strings.TrimSuffix(parent, "/"), false)
	if err != nil {
		return "", err
	}
	file, err := store.findChild(ctx, parentID, base, true)
	if err != nil {
		return "", err
	}
	if file == nil {
		return "", &googleapi.Error{Code: http.StatusNotFound, Message: "Google Drive folder not found"}
	}
	store.cache(name, file.Id)
	return file.Id, nil
}

func (store *gdrive) file(ctx context.Context, name string) (*drive.File, error) {
	if id, ok := store.cached(name); ok {
		file, err := store.service.Files.Get(id).SupportsAllDrives(true).Fields("id,name,size,md5Checksum,mimeType").Context(ctx).Do()
		if store.IsNotExist(err) {
			store.uncache(name)
		} else {
			return file, errors.WithStack(err)
		}
	}
	parent, base := path.Split(name)
	parentID, err := store.resolve(ctx, strings.TrimSuffix(parent, "/"), false)
	if err != nil {
		return nil, err
	}
	file, err := store.findChild(ctx, parentID, base, false)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, &googleapi.Error{Code: http.StatusNotFound, Message: "Google Drive object not found"}
	}
	store.cache(name, file.Id)
	return file, nil
}

func (store *gdrive) findChild(ctx context.Context, parentID, name string, folder bool) (*drive.File, error) {
	mimeClause := "mimeType != '" + folderMIME + "'"
	if folder {
		mimeClause = "mimeType = '" + folderMIME + "'"
	}
	query := fmt.Sprintf("'%s' in parents and name = '%s' and %s and trashed = false", escapeQuery(parentID), escapeQuery(name), mimeClause)
	call := store.service.Files.List().Q(query).SupportsAllDrives(true).IncludeItemsFromAllDrives(true).
		Fields("files(id,name,size,md5Checksum,mimeType)").PageSize(2).Context(ctx)
	if store.driveID != "" {
		call = call.Corpora("drive").DriveId(store.driveID)
	}
	result, err := call.Do()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(result.Files) > 1 {
		return nil, errors.Errorf("Google Drive contains duplicate entries named %q", name)
	}
	if len(result.Files) == 0 {
		return nil, nil
	}
	return result.Files[0], nil
}

func escapeQuery(value string) string {
	return strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(value)
}

func (store *gdrive) cache(name, id string) {
	store.cacheMu.Lock()
	store.pathIDs[name] = id
	store.cacheMu.Unlock()
}

func (store *gdrive) uncache(name string) {
	store.cacheMu.Lock()
	delete(store.pathIDs, name)
	store.cacheMu.Unlock()
}

func (store *gdrive) cached(name string) (string, bool) {
	store.cacheMu.RLock()
	id, ok := store.pathIDs[name]
	store.cacheMu.RUnlock()
	return id, ok
}
