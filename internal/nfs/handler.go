package nfs

import (
	"bytes"
	"container/list"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"io/fs"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	billy "github.com/go-git/go-billy/v5"
	gonfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
	nfsfile "github.com/willscott/go-nfs/file"
)

const (
	handleVersion      = 2
	handleHeaderSize   = 23
	handleTagSize      = 16
	inlinePathLimit    = gonfs.FHSize - handleHeaderSize - handleTagSize
	handleKindPath     = 0
	handleKindPathHash = 1
	handlePathHashSize = 16
	mountLifetime      = 10 * time.Minute
	anonymousID        = uint32(65534)
)

type handleRecord struct {
	key  string
	path string
}

type verifierRecord struct {
	key     string
	path    string
	entries []fs.FileInfo
	expires time.Time
}

type mountRecord struct {
	key      string
	hostname string
	dirpath  string
	expires  time.Time
}

type handler struct {
	filesystem *Filesystem
	exportName string
	limit      int

	mu          sync.Mutex
	key         [32]byte
	exportID    [8]byte
	generation  uint32
	closed      bool
	handles     map[string]*list.Element
	handleLRU   list.List
	verifiers   map[string]*list.Element
	verifierLRU list.List
	mounts      map[string]*mountRecord
}

func newHandler(filesystem *Filesystem, exportName string, limit int) (*handler, error) {
	h := &handler{
		filesystem: filesystem,
		exportName: exportName,
		limit:      limit,
		handles:    make(map[string]*list.Element),
		verifiers:  make(map[string]*list.Element),
		mounts:     make(map[string]*mountRecord),
	}
	if _, err := rand.Read(h.key[:]); err != nil {
		return nil, err
	}
	if _, err := rand.Read(h.exportID[:]); err != nil {
		zero(h.key[:])
		return nil, err
	}
	var generation [4]byte
	if _, err := rand.Read(generation[:]); err != nil {
		h.close()
		return nil, err
	}
	h.generation = binary.BigEndian.Uint32(generation[:])
	return h, nil
}

func (h *handler) Mount(_ context.Context, conn net.Conn, request gonfs.MountRequest) (gonfs.MountStatus, billy.Filesystem, []gonfs.AuthFlavor) {
	if string(request.Dirpath) != h.exportName {
		return gonfs.MountStatusErrNoEnt, nil, nil
	}
	now := time.Now()
	hostname := remoteHost(conn)
	key := hostname + "\x00" + h.exportName
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return gonfs.MountStatusErrServerFault, nil, nil
	}
	h.expireMountsLocked(now)
	if len(h.mounts) >= h.limit {
		for oldestKey, record := range h.mounts {
			if record.expires.After(now) {
				delete(h.mounts, oldestKey)
				break
			}
		}
	}
	h.mounts[key] = &mountRecord{key: key, hostname: hostname, dirpath: h.exportName, expires: now.Add(mountLifetime)}
	h.mu.Unlock()
	return gonfs.MountStatusOk, h.filesystem, []gonfs.AuthFlavor{gonfs.AuthFlavorNull, gonfs.AuthFlavorUnix}
}

func (h *handler) Change(billy.Filesystem) billy.Change { return nil }

type authUnixCredential struct {
	Stamp       uint32
	MachineName string
	UID         uint32
	GID         uint32
	Groups      []uint32
}

func (h *handler) Access(_ context.Context, header rpc.Header, filesystem billy.Filesystem, components []string, requested uint32) (uint32, error) {
	if filesystem != h.filesystem {
		return 0, os.ErrPermission
	}
	info, err := h.filesystem.Lstat(h.filesystem.Join(components...))
	if err != nil {
		return 0, err
	}
	metadata := nfsfile.GetInfo(info)
	uid := anonymousID
	gid := anonymousID
	var groups []uint32
	switch gonfs.AuthFlavor(header.Cred.Flavor) {
	case gonfs.AuthFlavorNull:
	case gonfs.AuthFlavorUnix:
		var credential authUnixCredential
		if err := xdr.Read(bytes.NewReader(header.Cred.Body), &credential); err != nil {
			return 0, os.ErrPermission
		}
		if credential.UID != 0 {
			uid, gid, groups = credential.UID, credential.GID, credential.Groups
		}
	default:
		return 0, os.ErrPermission
	}
	permissionBits := uint32(info.Mode().Perm() & 7)
	if uid == metadata.UID {
		permissionBits = uint32(info.Mode().Perm()>>6) & 7
	} else if gid == metadata.GID || slices.Contains(groups, metadata.GID) {
		permissionBits = uint32(info.Mode().Perm()>>3) & 7
	}
	var allowed uint32
	if permissionBits&4 != 0 {
		allowed |= gonfs.AccessRead
	}
	if permissionBits&1 != 0 {
		if info.IsDir() {
			allowed |= gonfs.AccessLookup
		} else {
			allowed |= gonfs.AccessExecute
		}
	}
	return requested & allowed, nil
}

func (h *handler) FSStat(_ context.Context, _ billy.Filesystem, stat *gonfs.FSStat) error {
	stat.CacheHint = time.Hour
	return nil
}

func (h *handler) HandleLimit() int { return h.limit }

func (h *handler) ToHandle(filesystem billy.Filesystem, components []string) []byte {
	if filesystem != h.filesystem {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	key := h.key
	exportID := h.exportID
	generation := h.generation
	h.mu.Unlock()
	defer zero(key[:])
	canonical := canonicalPath(components)
	info, err := h.filesystem.Lstat(h.filesystem.Join(components...))
	if err != nil {
		return nil
	}
	identity := nfsfile.GetInfo(info).Fileid
	payload := []byte(canonical)
	kind := byte(handleKindPath)
	if len(payload) > inlinePathLimit {
		kind = handleKindPathHash
		digest := sha256.Sum256([]byte(canonical))
		payload = digest[:handlePathHashSize]
	}
	bodySize := handleHeaderSize + len(payload)
	handleSize := bodySize + handleTagSize
	padding := (4 - handleSize%4) % 4
	body := make([]byte, bodySize+padding)
	body[0] = handleVersion
	copy(body[1:9], exportID[:])
	binary.BigEndian.PutUint32(body[9:13], generation)
	binary.BigEndian.PutUint64(body[13:21], identity)
	body[21] = kind
	body[22] = byte(len(payload))
	copy(body[handleHeaderSize:], payload)
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(body) // hash.Hash.Write always accepts the full input without error.
	tag := mac.Sum(nil)
	handle := append(body, tag[:handleTagSize]...)

	h.mu.Lock()
	if !h.closed {
		h.rememberHandleLocked(string(handle), canonical)
	}
	h.mu.Unlock()
	return handle
}

func (h *handler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	if len(handle) < handleHeaderSize+handleTagSize || len(handle) > gonfs.FHSize || handle[0] != handleVersion {
		return nil, nil, staleError()
	}
	h.mu.Lock()
	closed := h.closed
	key := append([]byte(nil), h.key[:]...)
	exportID := h.exportID
	generation := h.generation
	h.mu.Unlock()
	defer zero(key)
	if closed || subtle.ConstantTimeCompare(handle[1:9], exportID[:]) != 1 || binary.BigEndian.Uint32(handle[9:13]) != generation {
		return nil, nil, staleError()
	}
	body := handle[:len(handle)-handleTagSize]
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(body) // hash.Hash.Write always accepts the full input without error.
	if subtle.ConstantTimeCompare(handle[len(handle)-handleTagSize:], mac.Sum(nil)[:handleTagSize]) != 1 {
		return nil, nil, staleError()
	}
	payloadLength := int(body[22])
	if payloadLength > len(body)-handleHeaderSize {
		return nil, nil, staleError()
	}
	payload := body[handleHeaderSize : handleHeaderSize+payloadLength]
	var canonical string
	switch body[21] {
	case handleKindPath:
		canonical = string(payload)
	case handleKindPathHash:
		if len(payload) != handlePathHashSize {
			return nil, nil, staleError()
		}
		h.mu.Lock()
		element := h.handles[string(handle)]
		if element != nil {
			h.handleLRU.MoveToFront(element)
			canonical = element.Value.(handleRecord).path
		}
		h.mu.Unlock()
		if element == nil {
			canonical = h.reconstruct(binary.BigEndian.Uint64(handle[13:21]), payload)
			if canonical == "" {
				return nil, nil, staleError()
			}
			h.mu.Lock()
			if !h.closed {
				h.rememberHandleLocked(string(handle), canonical)
			}
			h.mu.Unlock()
		}
	default:
		return nil, nil, staleError()
	}
	components := splitCanonical(canonical)
	info, err := h.filesystem.Lstat(h.filesystem.Join(components...))
	if err != nil || nfsfile.GetInfo(info).Fileid != binary.BigEndian.Uint64(handle[13:21]) {
		return nil, nil, staleError()
	}
	return h.filesystem, components, nil
}

func (h *handler) reconstruct(identity uint64, wantedHash []byte) string {
	paths := []string{"/"}
	for len(paths) != 0 {
		current := paths[len(paths)-1]
		paths = paths[:len(paths)-1]
		info, err := h.filesystem.Lstat(current)
		if err != nil {
			continue
		}
		digest := sha256.Sum256([]byte(current))
		if nfsfile.GetInfo(info).Fileid == identity && subtle.ConstantTimeCompare(digest[:handlePathHashSize], wantedHash) == 1 {
			return current
		}
		if !info.IsDir() {
			continue
		}
		entries, err := h.filesystem.ReadDir(current)
		if err != nil {
			continue
		}
		for index := len(entries) - 1; index >= 0; index-- {
			child := "/" + entries[index].Name()
			if current != "/" {
				child = current + "/" + entries[index].Name()
			}
			paths = append(paths, child)
		}
	}
	return ""
}

func (h *handler) InvalidateHandle(billy.Filesystem, []byte) error {
	return nil
}

func (h *handler) VerifierFor(path string, entries []fs.FileInfo) uint64 {
	canonical, err := canonicalVerifierPath(path)
	if err != nil {
		return 0
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return 0
	}
	key := h.key
	exportID := h.exportID
	h.mu.Unlock()
	defer zero(key[:])
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("vaultic-nfs-verifier-v1\x00")) // hash.Hash.Write cannot fail.
	_, _ = mac.Write(exportID[:])                           // hash.Hash.Write cannot fail.
	_, _ = mac.Write([]byte(canonical))                     // hash.Hash.Write cannot fail.
	var number [8]byte
	for _, entry := range entries {
		_, _ = mac.Write([]byte{0})            // hash.Hash.Write cannot fail.
		_, _ = mac.Write([]byte(entry.Name())) // hash.Hash.Write cannot fail.
		binary.BigEndian.PutUint64(number[:], nfsfile.GetInfo(entry).Fileid)
		_, _ = mac.Write(number[:]) // hash.Hash.Write cannot fail.
		binary.BigEndian.PutUint64(number[:], uint64(entry.Size()))
		_, _ = mac.Write(number[:]) // hash.Hash.Write cannot fail.
		binary.BigEndian.PutUint64(number[:], uint64(entry.Mode()))
		_, _ = mac.Write(number[:]) // hash.Hash.Write cannot fail.
	}
	verifier := binary.BigEndian.Uint64(mac.Sum(nil)[:8])
	cacheKey := verifierKey(canonical, verifier)
	record := verifierRecord{key: cacheKey, path: canonical, entries: append([]fs.FileInfo(nil), entries...), expires: time.Now().Add(mountLifetime)}
	h.mu.Lock()
	if !h.closed {
		if old := h.verifiers[cacheKey]; old != nil {
			h.verifierLRU.Remove(old)
		}
		h.verifiers[cacheKey] = h.verifierLRU.PushFront(record)
		for h.verifierLRU.Len() > h.limit {
			h.removeOldestVerifierLocked()
		}
	}
	h.mu.Unlock()
	return verifier
}

func (h *handler) DataForVerifier(path string, verifier uint64) []fs.FileInfo {
	canonical, err := canonicalVerifierPath(path)
	if err != nil {
		return nil
	}
	cacheKey := verifierKey(canonical, verifier)
	h.mu.Lock()
	defer h.mu.Unlock()
	element := h.verifiers[cacheKey]
	if element == nil {
		return nil
	}
	record := element.Value.(verifierRecord)
	if time.Now().After(record.expires) || record.path != canonical {
		delete(h.verifiers, cacheKey)
		h.verifierLRU.Remove(element)
		return nil
	}
	h.verifierLRU.MoveToFront(element)
	return append([]fs.FileInfo(nil), record.entries...)
}

func (h *handler) rememberHandleLocked(key, path string) {
	if old := h.handles[key]; old != nil {
		h.handleLRU.Remove(old)
	}
	h.handles[key] = h.handleLRU.PushFront(handleRecord{key: key, path: path})
	for h.handleLRU.Len() > h.limit {
		oldest := h.handleLRU.Back()
		record := oldest.Value.(handleRecord)
		delete(h.handles, record.key)
		h.handleLRU.Remove(oldest)
	}
}

func (h *handler) removeOldestVerifierLocked() {
	oldest := h.verifierLRU.Back()
	if oldest == nil {
		return
	}
	record := oldest.Value.(verifierRecord)
	delete(h.verifiers, record.key)
	h.verifierLRU.Remove(oldest)
}

func (h *handler) expireMountsLocked(now time.Time) {
	for key, record := range h.mounts {
		if !record.expires.After(now) {
			delete(h.mounts, key)
		}
	}
}

func (h *handler) activeMounts() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.expireMountsLocked(time.Now())
	return len(h.mounts)
}

func (h *handler) Mounts() []gonfs.MountEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.expireMountsLocked(time.Now())
	result := make([]gonfs.MountEntry, 0, len(h.mounts))
	for _, record := range h.mounts {
		result = append(result, gonfs.MountEntry{Hostname: record.hostname, Dirpath: record.dirpath})
	}
	return result
}

func (h *handler) Unmount(_ context.Context, conn net.Conn, dirpath string) {
	h.mu.Lock()
	delete(h.mounts, remoteHost(conn)+"\x00"+dirpath)
	h.mu.Unlock()
}

func (h *handler) UnmountAll(_ context.Context, conn net.Conn) {
	hostname := remoteHost(conn)
	h.mu.Lock()
	for key, record := range h.mounts {
		if record.hostname == hostname {
			delete(h.mounts, key)
		}
	}
	h.mu.Unlock()
}

func (h *handler) Exports() []gonfs.MountExport {
	return []gonfs.MountExport{{
		Dirpath:     h.exportName,
		AuthFlavors: []gonfs.AuthFlavor{gonfs.AuthFlavorNull, gonfs.AuthFlavorUnix},
	}}
}

func (h *handler) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	zero(h.key[:])
	zero(h.exportID[:])
	h.handles = nil
	h.verifiers = nil
	h.mounts = nil
	h.handleLRU.Init()
	h.verifierLRU.Init()
}

func remoteHost(conn net.Conn) string {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

func verifierKey(path string, verifier uint64) string {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], verifier)
	return path + "\x00" + string(encoded[:])
}

func canonicalVerifierPath(raw string) (string, error) {
	components, err := strictPath(raw)
	if err != nil {
		return "", err
	}
	return canonicalPath(components), nil
}

func splitCanonical(path string) []string {
	if path == "/" {
		return nil
	}
	return strings.Split(strings.TrimPrefix(path, "/"), "/")
}

func staleError() error {
	return &gonfs.NFSStatusError{NFSStatus: gonfs.NFSStatusStale, WrappedErr: os.ErrNotExist}
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

var _ gonfs.Handler = (*handler)(nil)
var _ gonfs.CachingHandler = (*handler)(nil)
var _ gonfs.MountObserver = (*handler)(nil)
