package fs

import (
	"net/url"
	"path"
	"strings"
)

func nfsURL(server, remote string) string {
	if strings.Contains(server, ":") {
		server = "[" + server + "]"
	}
	return (&url.URL{Scheme: "nfs", Host: server + ":", Path: remote}).String()
}

func (filesystem *NFS) Clean(name string) string {
	if IsNFSSource(name) {
		if source, err := ParseNFSSource(name); err == nil {
			return nfsURL(source.Server, source.Path)
		}
		return name
	}
	return filesystem.local.Clean(name)
}

func (filesystem *NFS) Abs(name string) (string, error) {
	if IsNFSSource(name) {
		source, err := ParseNFSSource(name)
		if err != nil {
			return "", err
		}
		return nfsURL(source.Server, source.Path), nil
	}
	return filesystem.local.Abs(name)
}

func (filesystem *NFS) IsAbs(name string) bool {
	return IsNFSSource(name) || filesystem.local.IsAbs(name)
}

func (filesystem *NFS) VolumeName(name string) string {
	if source, err := ParseNFSSource(name); err == nil {
		return "nfs@" + url.QueryEscape(source.Server)
	}
	return filesystem.local.VolumeName(name)
}

func (filesystem *NFS) Join(elements ...string) string {
	if len(elements) == 0 {
		return ""
	}
	first := elements[0]
	if strings.HasPrefix(first, "nfs@") {
		if server, err := url.QueryUnescape(strings.TrimPrefix(first, "nfs@")); err == nil {
			first = nfsURL(server, "/")
		}
	}
	if source, err := ParseNFSSource(first); err == nil {
		parts := append([]string{source.Path}, elements[1:]...)
		return nfsURL(source.Server, path.Join(parts...))
	}
	return filesystem.local.Join(elements...)
}

func (filesystem *NFS) Dir(name string) string {
	if source, err := ParseNFSSource(name); err == nil {
		return nfsURL(source.Server, path.Dir(source.Path))
	}
	return filesystem.local.Dir(name)
}

func (filesystem *NFS) Base(name string) string {
	if source, err := ParseNFSSource(name); err == nil {
		return path.Base(source.Path)
	}
	return filesystem.local.Base(name)
}

func (filesystem *NFS) linkPath(name, target string) string {
	if path.IsAbs(target) {
		if source, err := ParseNFSSource(name); err == nil {
			return nfsURL(source.Server, target)
		}
		return target
	}
	return filesystem.Join(filesystem.Dir(name), target)
}
