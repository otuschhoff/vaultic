package workingkv

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkingPolicyStore(t *testing.T) {
	for _, mode := range []Mode{ModeRAM, ModeKV} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			policy, err := NewPolicy(mode, 2<<20, root)
			if err != nil {
				t.Fatal(err)
			}
			ctx := WithPolicy(context.Background(), policy)
			store, err := OpenWorking(ctx, filepath.Join(root, "absent"), "fixture-")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, []Entry{{[]byte("key"), []byte("value")}}); err != nil {
				t.Fatal(err)
			}
			if mode == ModeRAM {
				files, err := os.ReadDir(root)
				if err != nil || len(files) != 0 {
					t.Fatal("RAM created files", err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if policy.Budget().Used != 0 {
				t.Fatal("policy leaked capacity")
			}
			files, err := os.ReadDir(root)
			if err != nil || len(files) != 0 {
				t.Fatal("working files survived close", err)
			}
		})
	}
}

func TestM3SecureMapParity(t *testing.T) {
	for _, mode := range []Mode{ModeRAM, ModeKV} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			policy, _ := NewPolicy(mode, 16<<20, root)
			ctx := WithPolicy(t.Context(), policy)
			store, err := OpenSecureMap(ctx, root, "vaultic-secure-")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			key := []byte("sensitive-directory\x00\xff")
			for _, value := range [][]byte{nil, []byte("sensitive-child-name"), bytes.Repeat([]byte("sensitive-child-name"), 100000)} {
				if err := store.PutValue(ctx, key, value); err != nil {
					t.Fatal(err)
				}
				got, found, err := store.GetValue(ctx, key)
				if err != nil || !found || !bytes.Equal(got, value) {
					t.Fatal("secure value mismatch", err)
				}
			}
			if mode == ModeRAM {
				entries, _ := os.ReadDir(root)
				if len(entries) != 0 {
					t.Fatal("RAM used scratch")
				}
			} else {
				err := filepath.WalkDir(store.Path(), func(path string, entry os.DirEntry, err error) error {
					if err != nil || entry.IsDir() {
						return err
					}
					raw, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					if bytes.Contains(raw, []byte("sensitive-directory")) || bytes.Contains(raw, []byte("sensitive-child-name")) {
						t.Error("plaintext working state on disk")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			rows, err := store.WorkingStore.Scan(ctx, nil, nil, 1024)
			if err != nil {
				t.Fatal(err)
			}
			rootKey := store.token(key)
			domain := store.domain
			store.domain = []byte("vaultic-working/v2/other-kind")
			if value, _, err := store.GetValue(ctx, key); err == nil || value != nil {
				t.Fatal("wrong kind/version accepted")
			}
			store.domain = domain
			for _, row := range rows {
				if bytes.Equal(row.Key, rootKey) {
					row.Value[len(row.Value)-1] ^= 1
					if err := store.WorkingStore.Put(ctx, []Entry{row}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if value, _, err := store.GetValue(ctx, key); err == nil || value != nil {
				t.Fatal("tampered root accepted")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if policy.Budget().Used != 0 {
				t.Fatal("secure map leaked")
			}
		})
	}
}
