package selfupdate

import (
	"archive/zip"
	"bufio"
	"bytes"
	"compress/bzip2"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"
)

func findHash(buf []byte, filename string) (hash []byte, err error) {
	scanner := bufio.NewScanner(bytes.NewReader(buf))
	for scanner.Scan() {
		data := strings.Split(scanner.Text(), "  ")
		if len(data) != 2 {
			continue
		}

		if data[1] == filename {
			h, err := hex.DecodeString(data[0])
			if err != nil {
				return nil, err
			}

			return h, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan checksum list: %w", err)
	}

	return nil, fmt.Errorf("hash for file %v not found", filename)
}

func extractToFile(buf []byte, filename, target string, printf func(string, ...any)) error {
	var reader io.Reader = bytes.NewReader(buf)
	switch filepath.Ext(filename) {
	case ".bz2":
		reader = bzip2.NewReader(reader)
	case ".zip":
		zipReader, err := zip.NewReader(bytes.NewReader(buf), int64(len(buf)))
		if err != nil {
			return err
		}

		if len(zipReader.File) != 1 {
			return errors.New("ZIP archive contains more than one file")
		}

		file, err := zipReader.File[0].Open()
		if err != nil {
			return err
		}

		defer func() {
			_ = file.Close() // Preserve the download failure; Close only releases the incomplete file.
		}()

		reader = file
	}

	// Write everything to a temp file
	dir := filepath.Dir(target)
	newFile, err := os.CreateTemp(dir, "vaultic")
	if err != nil {
		return err
	}

	n, err := io.Copy(newFile, reader)
	if err != nil {
		_ = newFile.Close()           // Preserve the copy failure while releasing the incomplete update.
		_ = os.Remove(newFile.Name()) // The incomplete update is never selected for installation.
		return err
	}
	if err = newFile.Sync(); err != nil {
		_ = newFile.Close()           // Preserve the sync failure while releasing the incomplete update.
		_ = os.Remove(newFile.Name()) // The incomplete update is never selected for installation.
		return err
	}
	if err = newFile.Close(); err != nil {
		_ = os.Remove(newFile.Name()) // A failed cleanup cannot replace the more actionable close failure.
		return err
	}

	mode := os.FileMode(0755)
	// attempt to find the original mode
	if fi, err := os.Lstat(target); err == nil {
		mode = fi.Mode()
	}

	// Remove the original binary.
	if err := removeResticBinary(dir, target); err != nil {
		return err
	}

	// Rename the temp file to the final location atomically.
	if err := os.Rename(newFile.Name(), target); err != nil {
		return err
	}

	printf("saved %d bytes in %v\n", n, target)
	return os.Chmod(target, mode)
}

// DownloadLatestStableRelease downloads the latest stable released version of
// vaultic and saves it to target. It returns the version string for the newest
// version. The function printf is used to print progress information.
func DownloadLatestStableRelease(ctx context.Context, target, currentVersion string, printf func(string, ...any)) (version string, err error) {
	if printf == nil {
		printf = func(string, ...any) {}
	}

	printf("find latest release of vaultic at GitHub\n")

	rel, err := GitHubLatestRelease(ctx, "vaultic", "vaultic")
	if err != nil {
		return "", err
	}

	if rel.Version == currentVersion {
		printf("vaultic is up to date\n")
		return currentVersion, nil
	}

	printf("latest version is %v\n", rel.Version)

	_, sha256sums, err := getGithubDataFile(ctx, rel.Assets, "SHA256SUMS", printf)
	if err != nil {
		return "", err
	}

	_, sig, err := getGithubDataFile(ctx, rel.Assets, "SHA256SUMS.asc", printf)
	if err != nil {
		return "", err
	}

	ok, err := GPGVerify(sha256sums, sig)
	if err != nil {
		return "", err
	}

	if !ok {
		return "", errors.New("GPG signature verification of the file SHA256SUMS failed")
	}

	printf("GPG signature verification succeeded\n")

	ext := "bz2"
	if runtime.GOOS == "windows" {
		ext = "zip"
	}

	suffix := fmt.Sprintf("%s_%s.%s", runtime.GOOS, runtime.GOARCH, ext)
	downloadFilename, buf, err := getGithubDataFile(ctx, rel.Assets, suffix, printf)
	if err != nil {
		return "", err
	}

	printf("downloaded %v\n", downloadFilename)

	wantHash, err := findHash(sha256sums, downloadFilename)
	if err != nil {
		return "", err
	}

	gotHash := sha256.Sum256(buf)
	if !bytes.Equal(wantHash, gotHash[:]) {
		return "", fmt.Errorf("SHA256 hash mismatch, want hash %02x, got %02x", wantHash, gotHash)
	}

	err = extractToFile(buf, downloadFilename, target, printf)
	if err != nil {
		return "", err
	}

	return rel.Version, nil
}
