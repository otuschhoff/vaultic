package backupcmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type BoundedInodeManifest struct {
	Version    int      `json:"version"`
	Source     string   `json:"source"`
	InodeLimit uint64   `json:"inode_limit"`
	Paths      []string `json:"paths"`
}

func loadBoundedInodeManifest(filename, source string) (map[string]bool, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, (64<<20)+1))
	decoder.DisallowUnknownFields()
	var manifest BoundedInodeManifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("bounded inode manifest has trailing data")
	}
	if manifest.Version != 1 || manifest.Source != source || !filepath.IsAbs(source) || filepath.Clean(source) != source || manifest.InodeLimit == 0 || manifest.InodeLimit > 200_000 || uint64(len(manifest.Paths)) > manifest.InodeLimit {
		return nil, fmt.Errorf("invalid bounded inode manifest source or limit")
	}
	selected := make(map[string]bool, len(manifest.Paths))
	for _, relative := range manifest.Paths {
		if relative != "." && (!filepath.IsLocal(relative) || filepath.Clean(relative) != relative) {
			return nil, fmt.Errorf("noncanonical bounded inode path")
		}
		path := filepath.Join(source, relative)
		if selected[path] {
			return nil, fmt.Errorf("duplicate bounded inode path")
		}
		selected[path] = true
	}
	if !selected[source] {
		return nil, fmt.Errorf("bounded inode manifest omits source root")
	}
	for path := range selected {
		if path != source && !selected[filepath.Dir(path)] {
			return nil, fmt.Errorf("bounded inode manifest omits an ancestor")
		}
	}
	return selected, nil
}
