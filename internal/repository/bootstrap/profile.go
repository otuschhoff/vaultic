package bootstrap

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/BurntSushi/toml"
)

type Profile struct {
	Format       uint   `toml:"format" json:"format"`
	RepositoryID string `toml:"repository_id" json:"repository_id"`
	AnchorFile   string `toml:"anchor_file" json:"anchor_file"`
	Seeds        []Seed `toml:"seed" json:"seeds"`
}

type Seed struct {
	ID       string `toml:"id" json:"id"`
	Location string `toml:"location" json:"location"`
}

func LoadProfile(path string) (Profile, error) {
	var profile Profile
	metadata, err := toml.DecodeFile(path, &profile)
	if err != nil {
		return Profile{}, fmt.Errorf("decode bootstrap profile: %w", err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) != 0 {
		return Profile{}, fmt.Errorf("bootstrap profile contains unknown field %q", undecoded[0])
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, err
	}
	return profile, nil
}

func (profile Profile) Validate() error {
	if profile.Format != Format || profile.RepositoryID == "" || len(profile.Seeds) == 0 {
		return fmt.Errorf("bootstrap profile requires format, repository identity, and seeds")
	}
	seen := make(map[string]struct{}, len(profile.Seeds))
	for index, seed := range profile.Seeds {
		if seed.ID == "" || seed.Location == "" || containsCredential(seed.Location) {
			return fmt.Errorf("bootstrap seed %q is incomplete or credential-bearing", seed.ID)
		}
		if _, exists := seen[seed.ID]; exists {
			return fmt.Errorf("duplicate bootstrap seed %q", seed.ID)
		}
		if index > 0 && profile.Seeds[index-1].ID >= seed.ID {
			return fmt.Errorf("bootstrap seeds must be in canonical ID order")
		}
		seen[seed.ID] = struct{}{}
	}
	return nil
}

func LoadAnchor(path string) (Anchor, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return Anchor{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return Anchor{}, fmt.Errorf("bootstrap anchor must be a private regular file")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Anchor{}, err
	}
	var anchor Anchor
	if err := json.Unmarshal(encoded, &anchor); err != nil {
		return Anchor{}, fmt.Errorf("decode bootstrap anchor: %w", err)
	}
	if anchor.RepositoryID == "" || anchor.Generation == 0 || !validDigest(anchor.SHA256) {
		return Anchor{}, fmt.Errorf("invalid bootstrap anchor")
	}
	return anchor, nil
}

func StoreAnchor(path string, anchor Anchor) error {
	if anchor.RepositoryID == "" || anchor.Generation == 0 || !validDigest(anchor.SHA256) {
		return fmt.Errorf("invalid bootstrap anchor")
	}
	if current, err := LoadAnchor(path); err == nil {
		if current.RepositoryID != anchor.RepositoryID || anchor.Generation < current.Generation || anchor.Generation == current.Generation && anchor.SHA256 != current.SHA256 {
			return fmt.Errorf("bootstrap anchor update would conflict or roll back")
		}
		if current == anchor {
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	encoded, err := json.Marshal(anchor)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".anchor-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func ExportOffline(directory string, anchor Anchor, encodedManifest []byte) error {
	if len(encodedManifest) == 0 || len(encodedManifest) > MaxManifestBytes {
		return fmt.Errorf("invalid offline bootstrap manifest")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return err
	}
	manifestPath := filepath.Join(directory, fmt.Sprintf("topology-%020d.enc", anchor.Generation))
	file, err := os.OpenFile(manifestPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(encodedManifest); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return StoreAnchor(filepath.Join(directory, "anchor.json"), anchor)
}

func CanonicalizeProfile(profile *Profile) {
	sort.Slice(profile.Seeds, func(i, j int) bool { return profile.Seeds[i].ID < profile.Seeds[j].ID })
}
