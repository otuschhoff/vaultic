package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	gonfs "github.com/willscott/go-nfs"
)

const (
	rpcCallMessage  = 0
	rpcReplyMessage = 1
	rpcAccepted     = 0
	rpcDenied       = 1

	rpcSuccess          = 0
	rpcProgramUnavail   = 1
	rpcProgramMismatch  = 2
	rpcProcedureUnavail = 3
	rpcVersionMismatch  = 0
	rpcAuthError        = 1
)

type wireServer struct {
	server          *Server
	addresses       Addresses
	rootHandle      []byte
	fileHandle      []byte
	directoryHandle []byte
}

type rawRPCReply struct {
	replyStatus uint32
	result      uint32
	body        []byte
}

func TestRawRPCProtocolErrors(t *testing.T) {
	fixture := startWireServer(t, Config{})

	tests := []struct {
		name       string
		address    string
		rpcVersion uint32
		program    uint32
		version    uint32
		procedure  uint32
		reply      uint32
		result     uint32
		body       []byte
	}{
		{"rpc-version", fixture.addresses.NFS, 3, gonfs.NFSProgram, 3, 0, rpcDenied, rpcVersionMismatch, words(2, 2)},
		{"nfs-version", fixture.addresses.NFS, 2, gonfs.NFSProgram, 2, 0, rpcAccepted, rpcProgramMismatch, words(3, 3)},
		{"mount-version", fixture.addresses.Mount, 2, gonfs.MountProgram, 2, 0, rpcAccepted, rpcProgramMismatch, words(3, 3)},
		{"unknown-procedure", fixture.addresses.NFS, 2, gonfs.NFSProgram, 3, 99, rpcAccepted, rpcProcedureUnavail, nil},
		{"nfs-on-mount-port", fixture.addresses.Mount, 2, gonfs.NFSProgram, 3, 0, rpcAccepted, rpcProgramUnavail, nil},
		{"nfs-v2-on-mount-port", fixture.addresses.Mount, 2, gonfs.NFSProgram, 2, 0, rpcAccepted, rpcProgramUnavail, nil},
		{"mount-on-nfs-port", fixture.addresses.NFS, 2, gonfs.MountProgram, 3, 0, rpcAccepted, rpcProgramUnavail, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reply := rawRPCCall(t, test.address, rawRPCRequest(test.rpcVersion, test.program, test.version, test.procedure, nil))
			if reply.replyStatus != test.reply || reply.result != test.result || !bytes.Equal(reply.body, test.body) {
				t.Fatalf("reply status/result/body = %d/%d/%x, want %d/%d/%x", reply.replyStatus, reply.result, reply.body, test.reply, test.result, test.body)
			}
		})
	}
}

func TestRawRPCFragmentedRequest(t *testing.T) {
	fixture := startWireServer(t, Config{})
	request := rawRPCRequest(2, gonfs.NFSProgram, 3, 0, nil)
	reply := rawRPCCallFragments(t, fixture.addresses.NFS, request[:12], request[12:])
	if reply.replyStatus != rpcAccepted || reply.result != rpcSuccess {
		t.Fatalf("fragmented request reply = %s", describeReply(reply))
	}
}

func TestRawRPCAuthenticationErrors(t *testing.T) {
	fixture := startWireServer(t, Config{})
	tests := []struct {
		name     string
		request  []byte
		authStat uint32
	}{
		{"unsupported-flavor", rawRPCRequestAuth(2, gonfs.NFSProgram, 3, 0, 99, nil, 0, nil, nil), 5},
		{"null-with-body", rawRPCRequestAuth(2, gonfs.NFSProgram, 3, 0, 0, []byte{0, 0, 0, 0}, 0, nil, nil), 1},
		{"malformed-auth-sys", rawRPCRequestAuth(2, gonfs.NFSProgram, 3, 0, 1, []byte{0, 0, 0, 1}, 0, nil, nil), 1},
		{"bad-verifier", rawRPCRequestAuth(2, gonfs.MountProgram, 3, 0, 0, nil, 1, []byte{0, 0, 0, 0}, nil), 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reply := rawRPCCall(t, fixture.addresses.NFS, test.request)
			if reply.replyStatus != rpcDenied || reply.result != rpcAuthError || !bytes.Equal(reply.body, words(test.authStat)) {
				t.Fatalf("authentication reply = %s, want denied AUTH_ERROR %d", describeReply(reply), test.authStat)
			}
		})
	}
}

func TestRawNFSReadOnlyProceduresAndCommit(t *testing.T) {
	fixture := startWireServer(t, Config{})
	attributes := unsetSetAttributes()
	directoryEntry := func(name string) []byte {
		return append(opaque(fixture.rootHandle), opaque([]byte(name))...)
	}

	tests := []struct {
		name        string
		procedure   gonfs.NFSProcedure
		arguments   []byte
		failureSize int
	}{
		{"setattr", gonfs.NFSProcedureSetAttr, join(opaque(fixture.fileHandle), attributes, words(0)), 8},
		{"write", gonfs.NFSProcedureWrite, join(opaque(fixture.fileHandle), uint64Word(0), words(1, 0), opaque([]byte("x"))), 8},
		{"create", gonfs.NFSProcedureCreate, join(directoryEntry("new-file"), words(0), attributes), 8},
		{"mkdir", gonfs.NFSProcedureMkDir, join(directoryEntry("new-directory"), attributes), 8},
		{"symlink", gonfs.NFSProcedureSymlink, join(directoryEntry("new-link"), attributes, opaque([]byte("a-file"))), 8},
		{"mknod-fifo", gonfs.NFSProcedureMkNod, join(directoryEntry("new-pipe"), words(7), attributes), 8},
		{"remove", gonfs.NFSProcedureRemove, directoryEntry("a-file"), 8},
		{"rmdir", gonfs.NFSProcedureRmDir, directoryEntry("directory"), 8},
		{"rename", gonfs.NFSProcedureRename, join(directoryEntry("a-file"), directoryEntry("renamed")), 16},
		{"link", gonfs.NFSProcedureLink, join(opaque(fixture.fileHandle), directoryEntry("linked")), 12},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reply := rawRPCCall(t, fixture.addresses.NFS, rawNFSRequest(uint32(test.procedure), test.arguments))
			assertNFSFailure(t, reply, gonfs.NFSStatusROFS, test.failureSize)
		})
	}

	commitArgs := join(opaque(fixture.fileHandle), uint64Word(0), words(0))
	reply := rawRPCCall(t, fixture.addresses.NFS, rawNFSRequest(uint32(gonfs.NFSProcedureCommit), commitArgs))
	if reply.replyStatus != rpcAccepted || reply.result != rpcSuccess || len(reply.body) != 104 {
		t.Fatalf("COMMIT %s, want accepted success with 104-byte result", describeReply(reply))
	}
	if status := binary.BigEndian.Uint32(reply.body[:4]); status != uint32(gonfs.NFSStatusOk) {
		t.Fatalf("COMMIT status = %d", status)
	}
	if prePresent, postPresent := binary.BigEndian.Uint32(reply.body[4:8]), binary.BigEndian.Uint32(reply.body[8:12]); prePresent != 0 || postPresent != 1 {
		t.Fatalf("COMMIT wcc presence = %d/%d", prePresent, postPresent)
	}
}

func TestRawNFSDirectoryCookieValidation(t *testing.T) {
	fixture := startWireServer(t, Config{})
	procedures := []struct {
		name      string
		procedure gonfs.NFSProcedure
		counts    []byte
	}{
		{"readdir", gonfs.NFSProcedureReadDir, words(4096)},
		{"readdirplus", gonfs.NFSProcedureReadDirPlus, words(512, 4096)},
	}
	for _, procedure := range procedures {
		t.Run(procedure.name, func(t *testing.T) {
			call := func(handle []byte, cookie, verifier uint64) rawRPCReply {
				arguments := join(opaque(handle), uint64Word(cookie), uint64Word(verifier), procedure.counts)
				return rawRPCCall(t, fixture.addresses.NFS, rawNFSRequest(uint32(procedure.procedure), arguments))
			}

			initial := call(fixture.rootHandle, 0, 0)
			verifier := directoryVerifier(t, initial)
			if verifier == 0 {
				t.Fatal("initial directory verifier is zero")
			}
			assertNFSFailure(t, call(fixture.rootHandle, 1, 0), gonfs.NFSStatusBadCookie, 4)
			assertNFSFailure(t, call(fixture.rootHandle, 1, verifier^1), gonfs.NFSStatusBadCookie, 4)
			assertNFSFailure(t, call(fixture.directoryHandle, 1, verifier), gonfs.NFSStatusBadCookie, 4)
			assertNFSFailure(t, call(fixture.rootHandle, 999, verifier), gonfs.NFSStatusBadCookie, 4)
			resumed := call(fixture.rootHandle, 1, verifier)
			if resumed.replyStatus != rpcAccepted || resumed.result != rpcSuccess || nfsStatus(t, resumed) != gonfs.NFSStatusOk {
				t.Fatalf("cookie 1 did not resume: %s", describeReply(resumed))
			}
			if resumedVerifier := directoryVerifier(t, resumed); resumedVerifier != verifier {
				t.Fatalf("resumed verifier = %#x, want %#x", resumedVerifier, verifier)
			}
		})
	}
}

func TestRawRPCMalformedClientsReleaseConnectionSlot(t *testing.T) {
	fixture := startWireServer(t, Config{MaxConnections: 1, MaxRequestSize: 256, ConnectionIdleTimeout: 100 * time.Millisecond})
	waitForActiveConnections(t, fixture.server, 0)

	malformedCredential := join(
		words(0x7661756c, rpcCallMessage, 2, gonfs.NFSProgram, 3, uint32(gonfs.NFSProcedureNull)),
		words(0, ^uint32(0), 0, 0),
	)
	malformedHandle := join(rawNFSRequest(uint32(gonfs.NFSProcedureGetAttr), nil), words(^uint32(0)))
	tests := []struct {
		name    string
		address string
		record  []byte
		marker  uint32
		idle    bool
	}{
		{"credential-length", fixture.addresses.NFS, malformedCredential, uint32(len(malformedCredential)) | 1<<31, false},
		{"handle-length", fixture.addresses.NFS, malformedHandle, uint32(len(malformedHandle)) | 1<<31, false},
		{"oversized-record", fixture.addresses.NFS, nil, uint32(fixture.server.cfg.MaxRequestSize+1) | 1<<31, false},
		{"idle-timeout", fixture.addresses.NFS, nil, 0, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sendRejectedRecord(t, test.address, test.marker, test.record, test.idle)
			waitForActiveConnections(t, fixture.server, 0)
		})
	}

	nullReply := rawRPCCall(t, fixture.addresses.NFS, rawNFSRequest(uint32(gonfs.NFSProcedureNull), nil))
	if nullReply.replyStatus != rpcAccepted || nullReply.result != rpcSuccess || len(nullReply.body) != 0 {
		t.Fatalf("valid NFS NULL after malformed clients: %s", describeReply(nullReply))
	}
	waitForActiveConnections(t, fixture.server, 0)
	mountReply := rawRPCCall(t, fixture.addresses.Mount, rawRPCRequest(2, gonfs.MountProgram, 3, uint32(gonfs.MountProcMount), opaque([]byte("/snapshot"))))
	if mountReply.replyStatus != rpcAccepted || mountReply.result != rpcSuccess ||
		len(mountReply.body) < 8 || binary.BigEndian.Uint32(mountReply.body[:4]) != uint32(gonfs.MountStatusOk) {
		t.Fatalf("valid MOUNT after malformed clients: %s", describeReply(mountReply))
	}
}

func TestRawRPCRejectsExcessiveFragments(t *testing.T) {
	fixture := startWireServer(t, Config{MaxConnections: 1, ConnectionIdleTimeout: time.Second})
	connection, err := net.DialTimeout("tcp", fixture.addresses.NFS, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	markers := bytes.Repeat([]byte{0, 0, 0, 0}, gonfs.MaxRecordFragments+1)
	if _, err := connection.Write(markers); err != nil {
		t.Fatal(err)
	}
	var reply [1]byte
	if _, err := connection.Read(reply[:]); err == nil {
		t.Fatal("excessive fragment chain remained connected")
	}
	waitForActiveConnections(t, fixture.server, 0)
}

func startWireServer(t *testing.T, config Config) wireServer {
	t.Helper()
	filesystem, _ := testFilesystem(t, 64)
	config.EphemeralPorts = true
	if config.MaxReadSize == 0 {
		config.MaxReadSize = 64
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = time.Second
	}
	server, err := New(filesystem.fs, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	addresses := waitReady(t, server)
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
	})

	mountReply := rawRPCCall(t, addresses.Mount, rawRPCRequest(2, gonfs.MountProgram, 3, uint32(gonfs.MountProcMount), opaque([]byte("/snapshot"))))
	if mountReply.replyStatus != rpcAccepted || mountReply.result != rpcSuccess ||
		len(mountReply.body) < 8 || binary.BigEndian.Uint32(mountReply.body[:4]) != uint32(gonfs.MountStatusOk) {
		t.Fatalf("mount fixture: %s", describeReply(mountReply))
	}
	rootHandle, rest := readOpaque(t, mountReply.body[4:])
	if len(rootHandle) == 0 || len(rest) < 4 {
		t.Fatalf("mount fixture handle/auth flavors = %x/%x", rootHandle, rest)
	}
	waitForActiveConnections(t, server, 0)
	fileHandle := rawLookup(t, addresses.NFS, rootHandle, "a-file")
	waitForActiveConnections(t, server, 0)
	directoryHandle := rawLookup(t, addresses.NFS, rootHandle, "directory")
	waitForActiveConnections(t, server, 0)

	return wireServer{server: server, addresses: addresses, rootHandle: rootHandle, fileHandle: fileHandle, directoryHandle: directoryHandle}
}

func rawRPCRequest(rpcVersion, program, version, procedure uint32, body []byte) []byte {
	request := words(0x7661756c, rpcCallMessage, rpcVersion, program, version, procedure, 0, 0, 0, 0)
	return append(request, body...)
}

func rawRPCRequestAuth(
	rpcVersion, program, version, procedure, credentialFlavor uint32,
	credentialBody []byte, verifierFlavor uint32, verifierBody, body []byte,
) []byte {
	request := words(0x7661756c, rpcCallMessage, rpcVersion, program, version, procedure, credentialFlavor, uint32(len(credentialBody)))
	request = append(request, credentialBody...)
	request = append(request, make([]byte, (4-len(credentialBody)%4)%4)...)
	request = append(request, words(verifierFlavor, uint32(len(verifierBody)))...)
	request = append(request, verifierBody...)
	request = append(request, make([]byte, (4-len(verifierBody)%4)%4)...)
	return append(request, body...)
}

func rawNFSRequest(procedure uint32, body []byte) []byte {
	credential := join(words(0x12345678), opaque([]byte("vaultic-wire-test")), words(12, 34, 0))
	request := words(0x7661756c, rpcCallMessage, 2, gonfs.NFSProgram, 3, procedure, uint32(gonfs.AuthFlavorUnix), uint32(len(credential)))
	request = append(request, credential...)
	request = append(request, words(0, 0)...)
	return append(request, body...)
}

func rawLookup(t *testing.T, address string, directoryHandle []byte, name string) []byte {
	t.Helper()
	reply := rawRPCCall(t, address, rawNFSRequest(uint32(gonfs.NFSProcedureLookup), join(opaque(directoryHandle), opaque([]byte(name)))))
	if reply.replyStatus != rpcAccepted || reply.result != rpcSuccess || nfsStatus(t, reply) != gonfs.NFSStatusOk {
		t.Fatalf("lookup %q: %s", name, describeReply(reply))
	}
	handle, _ := readOpaque(t, reply.body[4:])
	if len(handle) == 0 {
		t.Fatalf("lookup %q returned an empty handle", name)
	}
	return handle
}

func rawRPCCall(t *testing.T, address string, request []byte) rawRPCReply {
	return rawRPCCallFragments(t, address, request)
}

func rawRPCCallFragments(t *testing.T, address string, fragments ...[]byte) rawRPCReply {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for index, fragment := range fragments {
		marker := words(uint32(len(fragment)))
		if index == len(fragments)-1 {
			marker = words(uint32(len(fragment)) | 1<<31)
		}
		if _, err := io.Copy(connection, io.MultiReader(bytes.NewReader(marker), bytes.NewReader(fragment))); err != nil {
			t.Fatal(err)
		}
	}

	marker := make([]byte, 4)
	if _, err := io.ReadFull(connection, marker); err != nil {
		t.Fatal(err)
	}
	fragment := binary.BigEndian.Uint32(marker)
	if fragment&(1<<31) == 0 {
		t.Fatal("RPC response used multiple record fragments")
	}
	length := fragment &^ (1 << 31)
	if length > 1<<20 {
		t.Fatalf("RPC response is unexpectedly large: %d", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(connection, payload); err != nil {
		t.Fatal(err)
	}
	return parseRawRPCReply(t, payload)
}

func parseRawRPCReply(t *testing.T, payload []byte) rawRPCReply {
	t.Helper()
	reader := bytes.NewReader(payload)
	read := func(label string) uint32 {
		var value uint32
		if err := binary.Read(reader, binary.BigEndian, &value); err != nil {
			t.Fatalf("read RPC %s from %x: %v", label, payload, err)
		}
		return value
	}
	if xid, messageType := read("xid"), read("message type"); xid != 0x7661756c || messageType != rpcReplyMessage {
		t.Fatalf("RPC reply xid/type = %#x/%d", xid, messageType)
	}
	reply := rawRPCReply{replyStatus: read("reply status")}
	switch reply.replyStatus {
	case rpcAccepted:
		_ = read("verifier flavor")
		verifierLength := read("verifier length")
		padding := (4 - verifierLength%4) % 4
		if verifierLength > uint32(reader.Len()) || verifierLength+padding > uint32(reader.Len()) {
			t.Fatalf("invalid RPC verifier length %d in %x", verifierLength, payload)
		}
		if _, err := reader.Seek(int64(verifierLength+padding), io.SeekCurrent); err != nil {
			t.Fatal(err)
		}
		reply.result = read("accept status")
	case rpcDenied:
		reply.result = read("reject status")
	default:
		t.Fatalf("unknown RPC reply status %d", reply.replyStatus)
	}
	reply.body = make([]byte, reader.Len())
	if _, err := io.ReadFull(reader, reply.body); err != nil {
		t.Fatal(err)
	}
	return reply
}

func words(values ...uint32) []byte {
	encoded := make([]byte, 4*len(values))
	for index, value := range values {
		binary.BigEndian.PutUint32(encoded[index*4:], value)
	}
	return encoded
}

func opaque(value []byte) []byte {
	encoded := append(words(uint32(len(value))), value...)
	return append(encoded, make([]byte, (4-len(value)%4)%4)...)
}

func readOpaque(t *testing.T, encoded []byte) ([]byte, []byte) {
	t.Helper()
	if len(encoded) < 4 {
		t.Fatalf("short XDR opaque: %x", encoded)
	}
	length := binary.BigEndian.Uint32(encoded[:4])
	padded := length + (4-length%4)%4
	if padded > uint32(len(encoded)-4) {
		t.Fatalf("invalid XDR opaque length %d in %x", length, encoded)
	}
	return append([]byte(nil), encoded[4:4+length]...), encoded[4+padded:]
}

func uint64Word(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func join(parts ...[]byte) []byte {
	var joined []byte
	for _, part := range parts {
		joined = append(joined, part...)
	}
	return joined
}

func unsetSetAttributes() []byte {
	return words(0, 0, 0, 0, 0, 0)
}

func nfsStatus(t *testing.T, reply rawRPCReply) gonfs.NFSStatus {
	t.Helper()
	if len(reply.body) < 4 {
		t.Fatalf("short NFS response: %s", describeReply(reply))
	}
	return gonfs.NFSStatus(binary.BigEndian.Uint32(reply.body[:4]))
}

func assertNFSFailure(t *testing.T, reply rawRPCReply, status gonfs.NFSStatus, failureSize int) {
	t.Helper()
	if reply.replyStatus != rpcAccepted || reply.result != rpcSuccess || nfsStatus(t, reply) != status {
		t.Fatalf("NFS failure: %s, want status %d", describeReply(reply), status)
	}
	if len(reply.body) < 4+failureSize {
		t.Fatalf("NFS failure body is %d bytes, want at least %d: %x", len(reply.body), 4+failureSize, reply.body)
	}
	if suffix := reply.body[4 : 4+failureSize]; !bytes.Equal(suffix, make([]byte, failureSize)) {
		t.Fatalf("NFS failure body has malformed absent attributes: %x", suffix)
	}
}

func directoryVerifier(t *testing.T, reply rawRPCReply) uint64 {
	t.Helper()
	if reply.replyStatus != rpcAccepted || reply.result != rpcSuccess || nfsStatus(t, reply) != gonfs.NFSStatusOk {
		t.Fatalf("directory call failed: %s", describeReply(reply))
	}
	reader := bytes.NewReader(reply.body[4:])
	var attributesPresent uint32
	if err := binary.Read(reader, binary.BigEndian, &attributesPresent); err != nil {
		t.Fatal(err)
	}
	if attributesPresent != 0 {
		if reader.Len() < 84 {
			t.Fatalf("short directory attributes in %x", reply.body)
		}
		if _, err := reader.Seek(84, io.SeekCurrent); err != nil {
			t.Fatal(err)
		}
	}
	var verifier uint64
	if err := binary.Read(reader, binary.BigEndian, &verifier); err != nil {
		t.Fatal(err)
	}
	return verifier
}

func sendRejectedRecord(t *testing.T, address string, marker uint32, record []byte, idle bool) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if !idle {
		// Early server rejection may close during the intentional malformed write.
		_, _ = io.Copy(connection, io.MultiReader(bytes.NewReader(words(marker)), bytes.NewReader(record)))
	}
	var one [1]byte
	if _, err := connection.Read(one[:]); err != nil {
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			t.Fatalf("malformed RPC connection was not rejected before deadline: %v", err)
		}
		return
	}
	// A syntactically complete request may receive an RPC/NFS rejection. Closing
	// the client must still release the server-side slot promptly.
}

func waitForActiveConnections(t *testing.T, server *Server, wanted int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if server.Stats().ActiveConnections == wanted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("active connections = %d, want %d", server.Stats().ActiveConnections, wanted)
}

func describeReply(reply rawRPCReply) string {
	return fmt.Sprintf("reply=%d result=%d body=%x", reply.replyStatus, reply.result, reply.body)
}
