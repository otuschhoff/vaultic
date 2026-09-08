package fs

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

type PathMapping struct {
	Source string
	Target string
}

type PrefixMap struct {
	FS       FS
	mappings []PathMapping
}

func NewPrefixMap(filesystem FS, mappings []PathMapping) (*PrefixMap, error) {
	if filesystem == nil {
		return nil, fmt.Errorf("prefix map requires a filesystem")
	}
	cleaned := make([]PathMapping, 0, len(mappings))
	for _, mapping := range mappings {
		if mapping.Source == "" || mapping.Target == "" {
			return nil, fmt.Errorf("prefix map paths cannot be empty")
		}
		cleaned = append(cleaned, PathMapping{Source: filepath.Clean(mapping.Source), Target: filepath.Clean(mapping.Target)})
	}
	sort.Slice(cleaned, func(left, right int) bool { return len(cleaned[left].Source) > len(cleaned[right].Source) })
	return &PrefixMap{FS: filesystem, mappings: cleaned}, nil
}

func (filesystem *PrefixMap) OpenFile(name string, flag int, metadataOnly bool) (File, error) {
	return filesystem.FS.OpenFile(filesystem.mapPath(name), flag, metadataOnly)
}

func (filesystem *PrefixMap) Lstat(name string) (*ExtendedFileInfo, error) {
	return filesystem.FS.Lstat(filesystem.mapPath(name))
}

func (filesystem *PrefixMap) Join(elem ...string) string      { return filesystem.FS.Join(elem...) }
func (filesystem *PrefixMap) Separator() string               { return filesystem.FS.Separator() }
func (filesystem *PrefixMap) Abs(path string) (string, error) { return filesystem.FS.Abs(path) }
func (filesystem *PrefixMap) Clean(path string) string        { return filesystem.FS.Clean(path) }
func (filesystem *PrefixMap) VolumeName(path string) string   { return filesystem.FS.VolumeName(path) }
func (filesystem *PrefixMap) IsAbs(path string) bool          { return filesystem.FS.IsAbs(path) }
func (filesystem *PrefixMap) Dir(path string) string          { return filesystem.FS.Dir(path) }
func (filesystem *PrefixMap) Base(path string) string         { return filesystem.FS.Base(path) }

func (filesystem *PrefixMap) mapPath(name string) string {
	cleaned := filepath.Clean(name)
	for _, mapping := range filesystem.mappings {
		relative, err := filepath.Rel(mapping.Source, cleaned)
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return filepath.Join(mapping.Target, relative)
		}
	}
	return name
}
