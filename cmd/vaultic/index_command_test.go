package main

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
)

func TestIndexCommandGroupDoesNotChangeListIndex(t *testing.T) {
	root := newRootCommand(&global.Options{})
	paths := [][]string{
		{"index", "import"}, {"index", "export"}, {"index", "check"}, {"index", "rebuild-pack-stats"},
		{"index", "gc"}, {"index", "analytics"}, {"index", "growth"}, {"index", "user-stats"}, {"index", "gdpr", "audit"},
		{"index", "encrypt"}, {"index", "unlock", "status"}, {"index", "unlock", "contribute"}, {"index", "unlock", "lock"},
		{"index", "keys", "status"}, {"index", "keys", "add-slot"}, {"index", "keys", "remove-slot"},
		{"index", "keys", "rotate-kek"}, {"index", "keys", "rotate-dek"}, {"index", "keys", "store-master-key"},
		{"index", "keys", "mirror-envelope"}, {"index", "keys", "quorum", "migrate-prepare"},
		{"index", "keys", "quorum", "migrate-finalize"}, {"index", "keys", "quorum", "verify"},
		{"index", "keys", "quorum", "enroll-macos-secure-enclave"}, {"index", "keys", "quorum", "generate-attestation-key"},
		{"index", "keys", "quorum", "attest-bypasses"}, {"index", "keys", "quorum", "create-group"},
		{"index", "keys", "quorum", "add-member"}, {"index", "keys", "quorum", "remove-member"},
		{"index", "keys", "quorum", "set-threshold"}, {"index", "keys", "quorum", "replace-member"},
		{"index", "keys", "escrow", "create"}, {"index", "keys", "escrow", "recover"},
	}
	for _, path := range paths {
		command, args, err := root.Find(path)
		if err != nil || command == nil || len(args) != 0 || command.Name() != path[len(path)-1] {
			t.Fatalf("find %v = %v, %v, %v", path, command, args, err)
		}
	}
	command, args, err := root.Find([]string{"list", "index"})
	if err != nil || command == nil || command.Name() != "list" || len(args) != 1 || args[0] != "index" {
		t.Fatalf("list index = %v, %v, %v", command, args, err)
	}
}
