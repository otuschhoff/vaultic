// Package schema defines the versioned binary key and value model stored by
// vaulticdb. It has no dependency on SlateDB or the daemon transport.
package schema

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const Version byte = 0

var ErrMalformed = errors.New("malformed slatedb schema record")

type ID [32]byte

type KeyKind byte

const (
	KeyBlob KeyKind = iota + 1
	KeyPack
	KeyPackAggregate
	KeyCurrentInode
	KeyInodeRevision
	KeyCurrentDirectory
	KeyDirectoryRevision
	KeySnapshot
	KeyContentManifest
	KeyReverseManifest
	KeyReverseInode
	KeyReferenceCount
	KeyGarbageCollection
	KeyCrawlDebt
	KeyImportCheckpoint
	KeySnapshotImportCheckpoint
	KeyNextRevision
	KeyHardlinkRefs
	KeyExportCheckpoint
)

type AggregateKind byte

const (
	AggregateData AggregateKind = iota + 1
	AggregateTree
	AggregateMixed
	AggregateUnknown
	AggregateAll
)

type GCTarget byte

const (
	GCBlob GCTarget = iota + 1
	GCPack
)

type ParsedKey struct {
	Kind      KeyKind
	ID        ID
	SecondID  ID
	FSID      uint32
	Inode     uint64
	Revision  uint64
	Segment   uint32
	Aggregate AggregateKind
	GCTarget  GCTarget
}

func BlobKey(id ID) []byte           { return idKey("b:", id) }
func PackKey(id ID) []byte           { return idKey("p:", id) }
func SnapshotKey(id ID) []byte       { return idKey("s:", id) }
func ReferenceCountKey(id ID) []byte { return idKey("rc:", id) }
func CurrentInodeKey(fsid uint32, inode uint64) []byte {
	return inodeKey("i:", fsid, inode)
}
func CurrentDirectoryKey(fsid uint32, inode uint64) []byte {
	return inodeKey("d:", fsid, inode)
}
func InodeRevisionKey(fsid uint32, inode, revision uint64) []byte {
	return revisionKey("iv:", fsid, inode, revision)
}
func DirectoryRevisionKey(fsid uint32, inode, revision uint64) []byte {
	return revisionKey("dv:", fsid, inode, revision)
}
func HardlinkRefsKey(fsid uint32, inode, revision uint64) []byte {
	return revisionKey("hr:", fsid, inode, revision)
}
func ContentManifestKey(id ID, segment uint32) []byte {
	key := make([]byte, 3+32+4)
	copy(key, "cm:")
	copy(key[3:], id[:])
	binary.BigEndian.PutUint32(key[35:], segment)
	return key
}
func ReverseManifestKey(blob, manifest ID) []byte {
	key := make([]byte, 3+64)
	copy(key, "rm:")
	copy(key[3:], blob[:])
	copy(key[35:], manifest[:])
	return key
}
func ReverseInodeKey(blob ID, fsid uint32, inode uint64) []byte {
	key := make([]byte, 3+32+4+8)
	copy(key, "ri:")
	copy(key[3:], blob[:])
	binary.BigEndian.PutUint32(key[35:], fsid)
	binary.BigEndian.PutUint64(key[39:], inode)
	return key
}
func GarbageCollectionKey(target GCTarget, id ID) []byte {
	var prefix string
	switch target {
	case GCBlob:
		prefix = "gc:b:"
	case GCPack:
		prefix = "gc:p:"
	default:
		return nil
	}
	return idKey(prefix, id)
}
func CrawlDebtKey(snapshot, work ID) []byte {
	key := make([]byte, 2+64)
	copy(key, "q:")
	copy(key[2:], snapshot[:])
	copy(key[34:], work[:])
	return key
}
func ImportCheckpointKey(index ID) []byte            { return idKey("meta:import-index:", index) }
func SnapshotImportCheckpointKey(snapshot ID) []byte { return idKey("meta:import-snapshot:", snapshot) }
func ExportCheckpointKey(snapshot ID) []byte         { return idKey("meta:export-snapshot:", snapshot) }
func PackAggregateKey(kind AggregateKind) []byte {
	name := map[AggregateKind]string{
		AggregateData: "data", AggregateTree: "tree", AggregateMixed: "mixed",
		AggregateUnknown: "unknown", AggregateAll: "all",
	}[kind]
	return []byte("a:pack:" + name)
}
func NextRevisionKey() []byte { return []byte("meta:next-revision-seq") }

func ParseKey(key []byte) (ParsedKey, error) {
	var parsed ParsedKey
	switch {
	case len(key) == 34 && string(key[:2]) == "b:":
		parsed.Kind = KeyBlob
		copy(parsed.ID[:], key[2:])
	case len(key) == 34 && string(key[:2]) == "p:":
		parsed.Kind = KeyPack
		copy(parsed.ID[:], key[2:])
	case len(key) == 34 && string(key[:2]) == "s:":
		parsed.Kind = KeySnapshot
		copy(parsed.ID[:], key[2:])
	case len(key) == 35 && string(key[:3]) == "rc:":
		parsed.Kind = KeyReferenceCount
		copy(parsed.ID[:], key[3:])
	case len(key) == 14 && string(key[:2]) == "i:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyCurrentInode, binary.BigEndian.Uint32(key[2:6]), binary.BigEndian.Uint64(key[6:])
	case len(key) == 14 && string(key[:2]) == "d:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyCurrentDirectory, binary.BigEndian.Uint32(key[2:6]), binary.BigEndian.Uint64(key[6:])
	case len(key) == 23 && string(key[:3]) == "iv:":
		parsed.Kind, parsed.FSID, parsed.Inode, parsed.Revision = KeyInodeRevision, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:15]), binary.BigEndian.Uint64(key[15:])
	case len(key) == 23 && string(key[:3]) == "dv:":
		parsed.Kind, parsed.FSID, parsed.Inode, parsed.Revision = KeyDirectoryRevision, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:15]), binary.BigEndian.Uint64(key[15:])
	case len(key) == 23 && string(key[:3]) == "hr:":
		parsed.Kind, parsed.FSID, parsed.Inode, parsed.Revision = KeyHardlinkRefs, binary.BigEndian.Uint32(key[3:7]), binary.BigEndian.Uint64(key[7:15]), binary.BigEndian.Uint64(key[15:])
	case len(key) == 39 && string(key[:3]) == "cm:":
		parsed.Kind, parsed.Segment = KeyContentManifest, binary.BigEndian.Uint32(key[35:])
		copy(parsed.ID[:], key[3:35])
	case len(key) == 67 && string(key[:3]) == "rm:":
		parsed.Kind = KeyReverseManifest
		copy(parsed.ID[:], key[3:35])
		copy(parsed.SecondID[:], key[35:])
	case len(key) == 47 && string(key[:3]) == "ri:":
		parsed.Kind, parsed.FSID, parsed.Inode = KeyReverseInode, binary.BigEndian.Uint32(key[35:39]), binary.BigEndian.Uint64(key[39:])
		copy(parsed.ID[:], key[3:35])
	case len(key) == 66 && string(key[:2]) == "q:":
		parsed.Kind = KeyCrawlDebt
		copy(parsed.ID[:], key[2:34])
		copy(parsed.SecondID[:], key[34:])
	case len(key) == 50 && string(key[:18]) == "meta:import-index:":
		parsed.Kind = KeyImportCheckpoint
		copy(parsed.ID[:], key[18:])
	case len(key) == 53 && string(key[:21]) == "meta:import-snapshot:":
		parsed.Kind = KeySnapshotImportCheckpoint
		copy(parsed.ID[:], key[21:])
	case len(key) == 53 && string(key[:21]) == "meta:export-snapshot:":
		parsed.Kind = KeyExportCheckpoint
		copy(parsed.ID[:], key[21:])
	case len(key) == 37 && (string(key[:5]) == "gc:b:" || string(key[:5]) == "gc:p:"):
		parsed.Kind, parsed.GCTarget = KeyGarbageCollection, GCBlob
		if key[3] == 'p' {
			parsed.GCTarget = GCPack
		}
		copy(parsed.ID[:], key[5:])
	case string(key) == "meta:next-revision-seq":
		parsed.Kind = KeyNextRevision
	default:
		if kind, ok := parseAggregate(key); ok {
			parsed.Kind, parsed.Aggregate = KeyPackAggregate, kind
		} else {
			return ParsedKey{}, fmt.Errorf("%w: unknown or incorrectly sized key", ErrMalformed)
		}
	}
	return parsed, nil
}

func idKey(prefix string, id ID) []byte {
	key := make([]byte, len(prefix)+len(id))
	copy(key, prefix)
	copy(key[len(prefix):], id[:])
	return key
}
func inodeKey(prefix string, fsid uint32, inode uint64) []byte {
	key := make([]byte, len(prefix)+12)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], fsid)
	binary.BigEndian.PutUint64(key[len(prefix)+4:], inode)
	return key
}
func revisionKey(prefix string, fsid uint32, inode, revision uint64) []byte {
	key := make([]byte, len(prefix)+20)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], fsid)
	binary.BigEndian.PutUint64(key[len(prefix)+4:], inode)
	binary.BigEndian.PutUint64(key[len(prefix)+12:], revision)
	return key
}
func parseAggregate(key []byte) (AggregateKind, bool) {
	for kind, name := range map[AggregateKind]string{
		AggregateData: "data", AggregateTree: "tree", AggregateMixed: "mixed",
		AggregateUnknown: "unknown", AggregateAll: "all",
	} {
		if string(key) == "a:pack:"+name {
			return kind, true
		}
	}
	return 0, false
}
