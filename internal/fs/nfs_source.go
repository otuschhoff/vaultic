package fs

import (
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"
)

type NFSSource struct {
	Server string
	Path   string
}

type nfsMount struct {
	point, root, source, kind, options string
	device                             uint64
}

func pathWithin(name, root string) bool {
	return name == root || strings.HasPrefix(name, strings.TrimSuffix(root, "/")+"/")
}

func mountedNFSSource(name string, mounts []nfsMount) (NFSSource, *nfsMount, error) {
	absolute, err := filepath.Abs(name)
	if err != nil {
		return NFSSource{}, nil, err
	}
	var selected *nfsMount
	for index := range mounts {
		mount := &mounts[index]
		if pathWithin(absolute, mount.point) && (selected == nil || len(mount.point) >= len(selected.point)) {
			selected = mount
		}
	}
	if selected == nil || selected.kind != "nfs" {
		return NFSSource{}, nil, nil
	}
	options := make(map[string]string)
	for _, option := range strings.Split(selected.options, ",") {
		key, value, _ := strings.Cut(option, "=")
		options[key] = value
	}
	if options["vers"] != "3" && options["nfsvers"] != "3" {
		return NFSSource{}, nil, nil
	}
	if security := options["sec"]; security != "" && security != "sys" {
		return NFSSource{}, nil, fmt.Errorf("direct NFS cannot preserve mount security %q at %s", security, selected.point)
	}
	separator := strings.LastIndex(selected.source, ":/")
	if separator < 1 {
		return NFSSource{}, nil, fmt.Errorf("invalid NFS mount source %q", selected.source)
	}
	server := strings.Trim(selected.source[:separator], "[]")
	remote := path.Join(selected.source[separator+1:], selected.root, strings.TrimPrefix(absolute, selected.point))
	return NFSSource{Server: server, Path: remote}, selected, nil
}

func IsNFSSource(source string) bool {
	return strings.HasPrefix(strings.ToLower(source), "nfs:")
}

func ParseNFSSource(source string) (NFSSource, error) {
	parsed, err := url.Parse(source)
	if err != nil {
		return NFSSource{}, fmt.Errorf("invalid NFS source: %w", err)
	}
	if parsed.Scheme != "nfs" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.Port() != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" ||
		!strings.HasPrefix(parsed.Path, "/") || strings.ContainsRune(parsed.Path, 0) {
		return NFSSource{}, fmt.Errorf("NFS source must be nfs://server:/absolute/path without credentials, port, query or fragment")
	}
	for _, component := range strings.Split(parsed.Path, "/") {
		if component == ".." {
			return NFSSource{}, fmt.Errorf("NFS source must not contain parent traversal")
		}
	}
	return NFSSource{Server: parsed.Hostname(), Path: path.Clean(parsed.Path)}, nil
}
