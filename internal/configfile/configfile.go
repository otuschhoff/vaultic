// Package configfile loads vaultic's local TOML profiles.
package configfile

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/pflag"
)

const DefaultProfile = "vaultic"

// Hook describes a command hook configured in a profile.
type Hook struct {
	Command   string   `toml:"command"`
	Args      []string `toml:"args"`
	OnFailure string   `toml:"on-failure"`
}

// Hooks holds hooks for one configuration scope. Entries may be strings or
// tables in TOML; decoding is normalized by Load.
type Hooks struct {
	Before  []Hook
	After   []Hook
	Failed  []Hook
	Finally []Hook
}

// SnapshotJob is a named backup configuration from [[backup.snapshots]].
type SnapshotJob struct {
	Name    string
	Sources []string
	Values  map[string]any
	Hooks   Hooks
}

// Profile is a fully merged profile. Sections are keyed by command name, with
// global and repository being applied to every command.
type Profile struct {
	Sections  map[string]map[string]any
	Hooks     map[string]Hooks
	Snapshots []SnapshotJob
	Files     []string
}

// Load resolves profile names, recursively loads use-profiles includes, and
// merges them from least to most specific. Later roots override earlier roots.
func Load(names []string) (*Profile, error) {
	if len(names) == 0 {
		names = []string{DefaultProfile}
	}

	p := &Profile{Sections: make(map[string]map[string]any), Hooks: make(map[string]Hooks)}
	for _, name := range names {
		path, err := resolve(name, "")
		if err != nil {
			if name == DefaultProfile && os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		if err := p.load(path, nil); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *Profile) load(path string, stack []string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if slices.Contains(stack, path) {
		return fmt.Errorf("profile include cycle: %s", strings.Join(append(stack, path), " -> "))
	}

	var raw map[string]any
	if _, err := toml.DecodeFile(path, &raw); err != nil {
		return fmt.Errorf("decode profile %q: %w", path, err)
	}

	includes, err := stringSlice(raw["use-profiles"])
	if err != nil {
		return fmt.Errorf("profile %q: use-profiles: %w", path, err)
	}
	for _, include := range includes {
		includePath, err := resolve(include, filepath.Dir(path))
		if err != nil {
			return err
		}
		if err := p.load(includePath, append(stack, path)); err != nil {
			return err
		}
	}

	for name, value := range raw {
		if name == "use-profiles" {
			continue
		}
		section, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("profile %q: section %q must be a table", path, name)
		}
		if name == "backup" {
			if err := p.extractBackup(section); err != nil {
				return fmt.Errorf("profile %q: %w", path, err)
			}
		}
		p.mergeSection(name, section)
	}
	p.Files = append(p.Files, path)
	return nil
}

func (p *Profile) mergeSection(name string, values map[string]any) {
	dst := p.Sections[name]
	if dst == nil {
		dst = make(map[string]any)
		p.Sections[name] = dst
	}
	for key, value := range values {
		if key == "hooks" || key == "snapshots" {
			continue
		}
		dst[key] = value
	}
	if hooks, ok := values["hooks"].(map[string]any); ok {
		p.Hooks[name] = mergeHooks(p.Hooks[name], decodeHooks(hooks))
	}
}

func (p *Profile) extractBackup(section map[string]any) error {
	jobs, ok := section["snapshots"]
	if !ok {
		return nil
	}
	list, ok := jobs.([]map[string]any)
	if !ok {
		return fmt.Errorf("backup.snapshots must be an array of tables")
	}
	for _, values := range list {
		name, _ := values["name"].(string)
		sources, err := stringSlice(values["sources"])
		if err != nil {
			return fmt.Errorf("backup.snapshots[%q].sources: %w", name, err)
		}
		if name == "" || len(sources) == 0 {
			return fmt.Errorf("backup.snapshots entries require name and sources")
		}
		job := SnapshotJob{Name: name, Sources: sources, Values: make(map[string]any)}
		for key, value := range values {
			if key != "name" && key != "sources" && key != "hooks" {
				job.Values[key] = value
			}
		}
		if hooks, ok := values["hooks"].(map[string]any); ok {
			job.Hooks = decodeHooks(hooks)
		}
		p.Snapshots = append(p.Snapshots, job)
	}
	return nil
}

func resolve(name, base string) (string, error) {
	if filepath.IsAbs(name) || strings.ContainsRune(name, filepath.Separator) || strings.HasSuffix(name, ".toml") {
		if !filepath.IsAbs(name) && base != "" {
			name = filepath.Join(base, name)
		}
		if !strings.HasSuffix(name, ".toml") {
			name += ".toml"
		}
		_, err := os.Stat(name)
		return name, err
	}

	filename := name + ".toml"
	paths := []string{filepath.Join(".", filename)}
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		paths = append(paths, filepath.Join(xdg, "vaultic", filename))
	} else if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, ".config", "vaultic", filename))
	}
	paths = append(paths, filepath.Join("/etc/vaultic", filename))
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	return "", os.ErrNotExist
}

// ApplyFlags applies a profile section to pflag values not explicitly set on
// the command line and not provided by the primary or legacy environment var.
func (p *Profile) ApplyFlags(section string, flags *pflag.FlagSet, envLookup func(string) bool) error {
	return ApplyValues(p.Sections[section], flags, envLookup)
}

// ApplyValues applies arbitrary configuration values to matching flags. It is
// used by named backup jobs after their parent backup section was applied.
func ApplyValues(values map[string]any, flags *pflag.FlagSet, envLookup func(string) bool) error {
	for key, value := range values {
		flag := flags.Lookup(key)
		if flag == nil || flag.Changed || envLookup(key) {
			continue
		}
		text, err := flagValue(value)
		if err != nil {
			return fmt.Errorf("option %s: %w", key, err)
		}
		if err := flags.Set(key, text); err != nil {
			return fmt.Errorf("option %s: %w", key, err)
		}
	}
	return nil
}

func flagValue(value any) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case bool, int64, float64:
		return fmt.Sprint(value), nil
	case []any:
		parts := make([]string, len(value))
		for i, entry := range value {
			text, err := flagValue(entry)
			if err != nil {
				return "", err
			}
			parts[i] = text
		}
		return strings.Join(parts, ","), nil
	default:
		return "", fmt.Errorf("unsupported value %T", value)
	}
}

func stringSlice(value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("must be an array of strings")
	}
	result := make([]string, len(items))
	for i, item := range items {
		var ok bool
		result[i], ok = item.(string)
		if !ok {
			return nil, fmt.Errorf("must be an array of strings")
		}
	}
	return result, nil
}

func decodeHooks(values map[string]any) Hooks {
	return Hooks{
		Before:  decodeHookList(values["run-before"]),
		After:   decodeHookList(values["run-after"]),
		Failed:  decodeHookList(values["run-failed"]),
		Finally: decodeHookList(values["run-finally"]),
	}
}

func decodeHookList(value any) []Hook {
	var result []Hook
	for _, entry := range toSlice(value) {
		switch entry := entry.(type) {
		case string:
			result = append(result, Hook{Command: entry, OnFailure: "error"})
		case map[string]any:
			command, _ := entry["command"].(string)
			args, _ := stringSlice(entry["args"])
			onFailure, _ := entry["on-failure"].(string)
			if onFailure == "" {
				onFailure = "error"
			}
			result = append(result, Hook{Command: command, Args: args, OnFailure: onFailure})
		}
	}
	return result
}

func toSlice(value any) []any {
	switch value := value.(type) {
	case []any:
		return value
	case nil:
		return nil
	default:
		return []any{value}
	}
}

func mergeHooks(left, right Hooks) Hooks {
	left.Before = append(left.Before, right.Before...)
	left.After = append(left.After, right.After...)
	left.Failed = append(left.Failed, right.Failed...)
	left.Finally = append(left.Finally, right.Finally...)
	return left
}
