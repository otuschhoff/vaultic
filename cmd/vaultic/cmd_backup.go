package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	uppathdiff "github.com/otuschhoff/pathdiff"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/configfile"
	"github.com/otuschhoff/vaultic/internal/crawl"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/env"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/filter"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/hooks"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/analytics"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/staging"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/textfile"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/backup"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func newBackupCommand(globalOptions *global.Options) *cobra.Command {
	var opts BackupOptions

	cmd := &cobra.Command{
		Use:   "backup [flags] [FILE/DIR] ...",
		Short: "Create a new backup of files and/or directories",
		Long: `
The "backup" command creates a new snapshot and saves the files and directories
given as the arguments.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was a fatal error (no snapshot created).
Exit status is 3 if some source data could not be read (incomplete snapshot created).
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return opts.Finalize()
		},
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 && globalOptions.Profile != nil && len(globalOptions.Profile.Snapshots) != 0 {
				return runProfileBackupJobs(cmd.Context(), opts, *globalOptions, globalOptions.Term, cmd.Flags())
			}
			return runBackup(cmd.Context(), opts, *globalOptions, globalOptions.Term, args)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// BackupOptions bundles all options for the backup command.
type BackupOptions struct {
	filter.ExcludePatternOptions

	Parent                    string
	GroupBy                   data.SnapshotGroupByOptions
	Force                     bool
	ExcludeOtherFS            bool
	ExcludeIfPresent          []string
	ExcludeCaches             bool
	ExcludeLargerThan         string
	ExcludeCloudFiles         bool
	Stdin                     bool
	StdinFilename             string
	StdinCommand              bool
	Tags                      data.TagLists
	Host                      string
	Label                     string
	Description               string
	DescriptionFrom           string
	DeleteNever               bool
	DeleteAfter               string
	FilesFrom                 []string
	FilesFromVerbatim         []string
	FilesFromRaw              []string
	TimeStamp                 string
	WithAtime                 bool
	IgnoreInode               bool
	IgnoreCtime               bool
	UseFsSnapshot             bool
	DryRun                    bool
	ReadConcurrency           uint
	NoScan                    bool
	UseCWalk                  bool
	CWalkConcurrency          int
	UsePathdiff               bool
	PathdiffEndpoint          string
	PathdiffRequireCoverage   bool
	PathdiffSVMMap            string
	SkipIfUnchanged           bool
	ProfileNames              []string
	Init                      bool
	List                      bool
	AllowDeferredCommit       bool
	DeferredMode              string
	DeferredExpiry            time.Duration
	AcknowledgeMetadataBypass bool

	readConcurrencyFlag *pflag.Flag
}

func (opts *BackupOptions) AddFlags(f *pflag.FlagSet) {
	f.StringVar(&opts.Parent, "parent", "", "use this parent `snapshot` (default: latest snapshot in the group determined by --group-by and not newer than the timestamp determined by --time)")
	opts.GroupBy = data.SnapshotGroupByOptions{Host: true, Path: true}
	f.VarP(&opts.GroupBy, "group-by", "g", "`group` snapshots by host, paths and/or tags, separated by comma (disable grouping with '')")
	f.BoolVarP(&opts.Force, "force", "f", false, `force re-reading the source files/directories (overrides the "parent" flag)`)

	opts.ExcludePatternOptions.Add(f)

	f.BoolVarP(&opts.ExcludeOtherFS, "one-file-system", "x", false, "exclude other file systems, don't cross filesystem boundaries and subvolumes")
	f.StringArrayVar(&opts.ExcludeIfPresent, "exclude-if-present", nil, "takes `filename[:header]`, exclude contents of directories containing filename (except filename itself) if header of that file is as provided (can be specified multiple times)")
	f.BoolVar(&opts.ExcludeCaches, "exclude-caches", false, `excludes cache directories that are marked with a CACHEDIR.TAG file. See https://bford.info/cachedir/ for the Cache Directory Tagging Standard`)
	f.StringVar(&opts.ExcludeLargerThan, "exclude-larger-than", "", "max `size` of the files to be backed up (allowed suffixes: k/K, m/M, g/G, t/T)")
	f.BoolVar(&opts.Stdin, "stdin", false, "read backup from stdin")
	f.StringVar(&opts.StdinFilename, "stdin-filename", "stdin", "`filename` to use when reading from stdin")
	f.BoolVar(&opts.StdinCommand, "stdin-from-command", false, "interpret arguments as command to execute and store its stdout")
	f.Var(&opts.Tags, "tag", "add `tags` for the new snapshot in the format `tag[,tag,...]` (can be specified multiple times)")
	f.StringVar(&opts.Label, "label", "", "set a `label` for the new snapshot (for grouping/filtering)")
	f.StringVar(&opts.Description, "description", "", "add a `description` to the new snapshot")
	f.StringVar(&opts.DescriptionFrom, "description-from", "", "read the snapshot description from `file`")
	f.BoolVar(&opts.DeleteNever, "delete-never", false, "mark the snapshot as never deletable by forget (delete protection)")
	f.StringVar(&opts.DeleteAfter, "delete-after", "", "mark the snapshot as not deletable before a `duration` from now (e.g. 10d; delete protection)")
	f.UintVar(&opts.ReadConcurrency, "read-concurrency", 0, "read `n` files concurrently (default: $VAULTIC_READ_CONCURRENCY or 2)")
	f.StringVarP(&opts.Host, "host", "H", "", "set the `hostname` for the snapshot manually (default: $VAULTIC_HOST). To prevent an expensive rescan use the \"parent\" flag")
	f.StringVar(&opts.Host, "hostname", "", "set the `hostname` for the snapshot manually")
	err := f.MarkDeprecated("hostname", "use --host")
	if err != nil {
		// MarkDeprecated only returns an error when the flag could not be found
		panic(err) //nolint:forbidigo // flag registration is a construction-time invariant
	}
	f.StringArrayVar(&opts.FilesFrom, "files-from", nil, "read the files to backup from `file` (can be combined with file args; can be specified multiple times)")
	f.StringArrayVar(&opts.FilesFromVerbatim, "files-from-verbatim", nil, "read the files to backup from `file` (can be combined with file args; can be specified multiple times)")
	f.StringArrayVar(&opts.FilesFromRaw, "files-from-raw", nil, "read the files to backup from `file` (can be combined with file args; can be specified multiple times)")
	f.StringVar(&opts.TimeStamp, "time", "", "`time` of the backup (ex. '2012-11-01 22:08:41') (default: now)")
	f.BoolVar(&opts.WithAtime, "with-atime", false, "store the atime for all files and directories")
	f.BoolVar(&opts.IgnoreInode, "ignore-inode", false, "ignore inode number and ctime changes when checking for modified files (default: $VAULTIC_IGNORE_INODE or false)")
	f.BoolVar(&opts.IgnoreCtime, "ignore-ctime", false, "ignore ctime changes when checking for modified files (default: $VAULTIC_IGNORE_CTIME or false)")
	f.BoolVarP(&opts.DryRun, "dry-run", "n", false, "do not upload or write any data, just show what would be done")
	f.BoolVar(&opts.NoScan, "no-scan", false, "do not run scanner to estimate size of backup")
	f.BoolVar(&opts.UseCWalk, "use-cwalk", false, "use parallel cwalk traversal for the backup scanner")
	f.IntVar(&opts.CWalkConcurrency, "cwalk-concurrency", runtime.GOMAXPROCS(0), "run `n` concurrent cwalk workers")
	f.BoolVar(&opts.UsePathdiff, "use-pathdiff", false, "use verified pathdiff events to skip unchanged subtrees")
	f.StringVar(&opts.PathdiffEndpoint, "pathdiff-endpoint", "", "pathdiff control socket `path`")
	f.BoolVar(&opts.PathdiffRequireCoverage, "pathdiff-require-coverage", false, "fail instead of performing a full crawl when pathdiff coverage is unverified")
	f.StringVar(&opts.PathdiffSVMMap, "pathdiff-svm-map", "", "pathdiff source-to-LIF/SVM/volume topology JSON `file`")
	if runtime.GOOS == "windows" {
		f.BoolVar(&opts.UseFsSnapshot, "use-fs-snapshot", false, "use filesystem snapshot where possible (currently only Windows VSS)")
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		f.BoolVar(&opts.ExcludeCloudFiles, "exclude-cloud-files", false, "excludes online-only cloud files (such as OneDrive, iCloud drive, …)")
	}
	f.BoolVar(&opts.SkipIfUnchanged, "skip-if-unchanged", false, "skip snapshot creation if identical to parent snapshot")
	f.BoolVar(&opts.AllowDeferredCommit, "allow-deferred-commit", false, "acknowledge that durable data remains unavailable as a snapshot until metadata reconciliation")
	f.StringVar(&opts.DeferredMode, "deferred-mode", "", "deferred ingest mode (auto, read-only-assisted, or data-plane-only)")
	f.DurationVar(&opts.DeferredExpiry, "deferred-expiry", 72*time.Hour, "staging journal expiry for deferred ingest")
	f.BoolVar(&opts.AcknowledgeMetadataBypass, "acknowledge-metadata-bypass", false, "acknowledge that data-plane-only mode ignores unavailable or corrupt metadata")
	f.StringSliceVar(&opts.ProfileNames, "name", nil, "run named [[backup.snapshots]] profile job (repeatable)")
	f.BoolVar(&opts.Init, "init", false, "initialize the repository if it does not exist")
	f.BoolVar(&opts.List, "ls", false, "list the contents of the created snapshot")

	opts.readConcurrencyFlag = f.Lookup("read-concurrency")

	// parse read inode and ctime from env, on error the default value will be used
	opts.IgnoreInode, _ = strconv.ParseBool(env.Get("IGNORE_INODE"))
	opts.IgnoreCtime, _ = strconv.ParseBool(env.Get("IGNORE_CTIME"))

	// parse host from env, if not exists or empty the default value will be used
	if host := env.Get("HOST"); host != "" {
		opts.Host = host
	}
}

func runProfileBackupJobs(ctx context.Context, base BackupOptions, gopts global.Options, term ui.Terminal, commandFlags *pflag.FlagSet) error {
	profile := gopts.Profile
	selected := make(map[string]bool, len(base.ProfileNames))
	for _, name := range base.ProfileNames {
		selected[name] = true
	}
	found := make(map[string]bool, len(selected))

	for _, job := range profile.Snapshots {
		if len(selected) != 0 && !selected[job.Name] {
			continue
		}
		found[job.Name] = true

		var jobOpts BackupOptions
		jobFlags := pflag.NewFlagSet("profile backup", pflag.ContinueOnError)
		jobFlags.SetInterspersed(true)
		jobOpts.AddFlags(jobFlags)
		envOverrides := func(name string) bool {
			_, ok := env.Lookup(strings.ToUpper(strings.ReplaceAll(name, "-", "_")))
			return ok
		}
		// Apply parsed [backup] settings directly. This preserves TOML arrays
		// such as exclude-if-present and globs without pflag's display format.
		if err := configfile.ApplyValues(profile.Sections["backup"], jobFlags, envOverrides); err != nil {
			return err
		}
		if err := configfile.ApplyValues(job.Values, jobFlags, envOverrides); err != nil {
			return err
		}
		// Profile application intentionally preserves pflag.Changed. Copy only
		// explicit CLI scalar values last so CLI > job > [backup] precedence.
		commandFlags.Visit(func(flag *pflag.Flag) {
			if jobFlags.Lookup(flag.Name) != nil && flag.Value.Type() != "stringArray" {
				_ = jobFlags.Set(flag.Name, flag.Value.String())
			}
		})
		if err := jobOpts.Finalize(); err != nil {
			return err
		}

		values := hooks.Context{Action: "backup", BackupLabel: jobOpts.Label, BackupSources: job.Sources, BackupTags: jobOpts.Tags.Flatten()}
		runner := hooks.Runner{Stdout: term.OutputWriter(), Stderr: term.OutputWriter(), Warn: func(format string, args ...any) { term.Error(fmt.Sprintf(format, args...)) }}
		if err := runner.Run(ctx, hooks.Before, []configfile.Hooks{job.Hooks}, values); err != nil {
			return err
		}
		err := runBackup(ctx, jobOpts, gopts, term, job.Sources)
		phase := hooks.After
		if err != nil {
			phase = hooks.Failed
		}
		if hookErr := runner.Run(ctx, phase, []configfile.Hooks{job.Hooks}, values); hookErr != nil && err == nil {
			err = hookErr
		}
		if hookErr := runner.Run(ctx, hooks.Finally, []configfile.Hooks{job.Hooks}, values); hookErr != nil && err == nil {
			err = hookErr
		}
		if err != nil {
			return err
		}
	}
	for name := range selected {
		if !found[name] {
			return errors.Fatalf("profile backup job %q was not found", name)
		}
	}
	return nil
}

func (opts *BackupOptions) Finalize() error {
	if envVal := env.Get("READ_CONCURRENCY"); envVal != "" && !opts.readConcurrencyFlag.Changed {
		n, err := strconv.ParseUint(envVal, 10, 32)
		if err != nil {
			return errors.Fatalf("invalid value for VAULTIC_READ_CONCURRENCY (legacy: RESTIC_READ_CONCURRENCY) %q: %v", envVal, err)
		}
		opts.ReadConcurrency = uint(n)
	}
	if opts.Host == "" {
		hostname, err := os.Hostname()
		if err != nil {
			debug.Log("os.Hostname() returned err: %v", err)
			return nil
		}
		opts.Host = hostname
	}
	return nil
}

var backupFSTestHook func(fs fs.FS) fs.FS

// ErrInvalidSourceData is used to report an incomplete backup
var ErrInvalidSourceData = errors.New("at least one source file could not be read")

// ErrNoSourceData is used to report that no source data was found
var ErrNoSourceData = errors.Fatal("all source directories/files do not exist")

// filterExisting returns the items that exist and can be accessed. It returns
// ErrNoSourceData if none remain, or ErrInvalidSourceData if some were skipped.
func filterExisting(items []string, warnf func(msg string, args ...any)) (result []string, err error) {
	for _, item := range items {
		_, err := fs.Lstat(item)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				warnf("%v does not exist, skipping\n", item)
			} else {
				warnf("%v cannot be accessed, skipping\n", item)
			}
			continue
		}

		result = append(result, item)
	}

	if len(result) == 0 {
		return nil, ErrNoSourceData
	} else if len(result) < len(items) {
		return result, ErrInvalidSourceData
	}

	return result, nil
}

// readLines reads all lines from the named file and returns them as a
// string slice.
//
// If filename is empty, readPatternsFromFile returns an empty slice.
// If filename is a dash (-), readPatternsFromFile will read the lines from the
// standard input.
func readLines(filename string, stdin io.ReadCloser) ([]string, error) {
	if filename == "" {
		return nil, nil
	}

	var (
		data []byte
		err  error
	)

	if filename == "-" {
		data, err = io.ReadAll(stdin)
	} else {
		data, err = textfile.Read(filename)
	}

	if err != nil {
		return nil, err
	}

	var lines []string

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

// readFilenamesFromFileRaw reads a list of filenames from the given file,
// or stdin if filename is "-". Each filename is terminated by a zero byte,
// which is stripped off.
func readFilenamesFromFileRaw(filename string, stdin io.ReadCloser) (names []string, err error) {
	f := stdin
	if filename != "-" {
		if f, err = os.Open(filename); err != nil {
			return nil, err
		}
	}

	names, err = readFilenamesRaw(f)
	if err != nil {
		// ignore subsequent errors
		_ = f.Close()
		return nil, err
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}

	return names, nil
}

func readFilenamesRaw(r io.Reader) (names []string, err error) {
	br := bufio.NewReader(r)
	for {
		name, err := br.ReadString(0)
		switch err {
		case nil:
		case io.EOF:
			if name == "" {
				return names, nil
			}
			return nil, errors.Fatal("--files-from-raw: trailing zero byte missing")
		default:
			return nil, err
		}

		name = name[:len(name)-1]
		if name == "" {
			// The empty filename is never valid. Handle this now to
			// prevent downstream code from erroneously backing up
			// filepath.Clean("") == ".".
			return nil, errors.Fatal("--files-from-raw: empty filename in listing")
		}
		names = append(names, name)
	}
}

// Check returns an error when an invalid combination of options was set.
func (opts BackupOptions) Check(gopts global.Options, args []string) error {
	if opts.DeferredMode != "" && !opts.AllowDeferredCommit {
		return errors.Fatal("--deferred-mode requires --allow-deferred-commit")
	}
	if opts.AllowDeferredCommit && opts.DeferredMode != "auto" && opts.DeferredMode != "read-only-assisted" && opts.DeferredMode != "data-plane-only" {
		return errors.Fatal("--allow-deferred-commit requires --deferred-mode=auto, read-only-assisted, or data-plane-only")
	}
	if opts.DeferredMode == "data-plane-only" && !opts.AcknowledgeMetadataBypass {
		return errors.Fatal("--deferred-mode=data-plane-only requires --acknowledge-metadata-bypass")
	}
	if opts.AllowDeferredCommit && (opts.Parent != "" || opts.SkipIfUnchanged || opts.DryRun || opts.Init || opts.List) {
		return errors.Fatal("deferred ingest cannot use --parent, --skip-if-unchanged, --dry-run, --init, or --list")
	}
	if opts.AllowDeferredCommit && opts.DeferredExpiry <= 0 {
		return errors.Fatal("--deferred-expiry must be positive")
	}
	if opts.UseCWalk && opts.CWalkConcurrency < 1 {
		return errors.Fatal("--cwalk-concurrency must be at least 1")
	}
	if opts.UsePathdiff && !opts.UseCWalk {
		return errors.Fatal("--use-pathdiff requires --use-cwalk")
	}
	if opts.UsePathdiff && opts.PathdiffEndpoint == "" {
		return errors.Fatal("--use-pathdiff requires --pathdiff-endpoint")
	}
	if opts.UsePathdiff && opts.PathdiffSVMMap == "" {
		return errors.Fatal("--use-pathdiff requires --pathdiff-svm-map")
	}
	if opts.PathdiffRequireCoverage && !opts.UsePathdiff {
		return errors.Fatal("--pathdiff-require-coverage requires --use-pathdiff")
	}
	if gopts.Password == "" && !gopts.InsecureNoPassword {
		if opts.Stdin {
			return errors.Fatal("cannot read both password and data from stdin")
		}

		filesFrom := append(append(opts.FilesFrom, opts.FilesFromVerbatim...), opts.FilesFromRaw...)
		if slices.Contains(filesFrom, "-") {
			return errors.Fatal("unable to read password from stdin when data is to be read from stdin, use --password-file or $VAULTIC_PASSWORD")
		}
	}

	if opts.Stdin || opts.StdinCommand {
		if len(opts.FilesFrom) > 0 {
			return errors.Fatal("--stdin and --files-from cannot be used together")
		}
		if len(opts.FilesFromVerbatim) > 0 {
			return errors.Fatal("--stdin and --files-from-verbatim cannot be used together")
		}
		if len(opts.FilesFromRaw) > 0 {
			return errors.Fatal("--stdin and --files-from-raw cannot be used together")
		}

		if len(args) > 0 && !opts.StdinCommand {
			return errors.Fatal("--stdin was specified and files/dirs were listed as arguments")
		}
	}

	return nil
}

// collectRejectByNameFuncs returns a list of all functions which may reject data
// from being saved in a snapshot based on path only
func collectRejectByNameFuncs(opts BackupOptions, repo *repository.Repository, warnf func(msg string, args ...any)) (fs []archiver.RejectByNameFunc, err error) {
	// exclude vaultic cache
	if repo.Cache() != nil {
		f, err := rejectResticCache(repo)
		if err != nil {
			return nil, err
		}

		fs = append(fs, f)
	}

	fsPatterns, err := opts.ExcludePatternOptions.CollectPatterns(warnf)
	if err != nil {
		return nil, err
	}
	for _, pat := range fsPatterns {
		fs = append(fs, archiver.RejectByNameFunc(pat))
	}

	return fs, nil
}

// collectRejectFuncs returns a list of all functions which may reject data
// from being saved in a snapshot based on path and file info
func collectRejectFuncs(opts BackupOptions, targets []string, fs fs.FS, warnf func(msg string, args ...any)) (funcs []archiver.RejectFunc, err error) {
	// allowed devices
	if opts.ExcludeOtherFS && !opts.Stdin && !opts.StdinCommand {
		f, err := archiver.RejectByDevice(targets, fs)
		if err != nil {
			return nil, err
		}
		funcs = append(funcs, f)
	}

	if len(opts.ExcludeLargerThan) != 0 && !opts.Stdin && !opts.StdinCommand {
		maxSize, err := ui.ParseBytes(opts.ExcludeLargerThan)
		if err != nil {
			return nil, err
		}

		f, err := archiver.RejectBySize(maxSize)
		if err != nil {
			return nil, err
		}
		funcs = append(funcs, f)
	}

	if opts.ExcludeCloudFiles && !opts.Stdin && !opts.StdinCommand {
		f, err := archiver.RejectCloudFiles(warnf)
		if err != nil {
			return nil, err
		}
		funcs = append(funcs, f)
	}

	if opts.ExcludeCaches {
		opts.ExcludeIfPresent = append(opts.ExcludeIfPresent, "CACHEDIR.TAG:Signature: 8a477f597d28d172789f06886806bc55")
	}

	for _, spec := range opts.ExcludeIfPresent {
		f, err := archiver.RejectIfPresent(spec, warnf)
		if err != nil {
			return nil, err
		}

		funcs = append(funcs, f)
	}

	return funcs, nil
}

// collectTargets returns a list of target files/dirs from several sources.
func collectTargets(opts BackupOptions, args []string, warnf func(msg string, args ...any), stdin io.ReadCloser) (targets []string, err error) {
	if opts.Stdin || opts.StdinCommand {
		return nil, nil
	}

	for _, file := range opts.FilesFrom {
		fromfile, err := readLines(file, stdin)
		if err != nil {
			return nil, err
		}

		// expand wildcards
		for _, line := range fromfile {
			line = strings.TrimSpace(line)
			if line == "" || line[0] == '#' { // '#' marks a comment.
				continue
			}

			var expanded []string
			expanded, err := filepath.Glob(line)
			if err != nil {
				return nil, fmt.Errorf("pattern: %s: %w", line, err)
			}
			if len(expanded) == 0 {
				warnf("pattern %q does not match any files, skipping\n", line)
			}
			targets = append(targets, expanded...)
		}
	}

	for _, file := range opts.FilesFromVerbatim {
		fromfile, err := readLines(file, stdin)
		if err != nil {
			return nil, err
		}
		for _, line := range fromfile {
			if line == "" {
				continue
			}
			targets = append(targets, line)
		}
	}

	for _, file := range opts.FilesFromRaw {
		fromfile, err := readFilenamesFromFileRaw(file, stdin)
		if err != nil {
			return nil, err
		}
		targets = append(targets, fromfile...)
	}

	// Merge args into files-from so we can reuse the normal args checks
	// and have the ability to use both files-from and args at the same time.
	targets = append(targets, args...)
	if len(targets) == 0 && !opts.Stdin {
		return nil, errors.Fatal("nothing to backup, please specify source files/dirs")
	}

	return filterExisting(targets, warnf)
}

// parent returns the ID of the parent snapshot. If there is none, nil is
// returned.
func findParentSnapshot(ctx context.Context, repo vaultic.ListerLoaderUnpacked, opts BackupOptions, targets []string, timeStampLimit time.Time) (*data.Snapshot, error) {
	if opts.Force {
		return nil, nil
	}

	snName := opts.Parent
	if snName == "" {
		snName = "latest"
	}
	f := data.SnapshotFilter{TimestampLimit: timeStampLimit}
	if opts.GroupBy.Host {
		f.Hosts = []string{opts.Host}
	}
	if opts.GroupBy.Path {
		f.Paths = targets
	}
	if opts.GroupBy.Tag {
		f.Tags = []data.TagList{opts.Tags.Flatten()}
	}

	sn, _, err := f.FindLatest(ctx, repo, repo, snName)
	// Snapshot not found is ok if no explicit parent was set
	if opts.Parent == "" && errors.Is(err, data.ErrNoSnapshotFound) {
		err = nil
	}
	return sn, err
}

func runBackup(ctx context.Context, opts BackupOptions, gopts global.Options, term ui.Terminal, args []string) error {
	var vsscfg fs.VSSConfig
	var err error

	var printer backup.ProgressPrinter
	if gopts.JSON {
		printer = backup.NewJSONProgress(term, gopts.Verbosity)
	} else {
		printer = backup.NewTextProgress(term, gopts.Verbosity)
	}
	if runtime.GOOS == "windows" {
		if vsscfg, err = fs.ParseVSSConfig(gopts.Extended); err != nil {
			return err
		}
	}

	err = opts.Check(gopts, args)
	if err != nil {
		return err
	}

	success := true
	targets, err := collectTargets(opts, args, printer.E, term.InputRaw())
	if err != nil {
		if errors.Is(err, ErrInvalidSourceData) {
			success = false
		} else {
			return err
		}
	}

	timeStamp := time.Now()
	backupStart := timeStamp
	if opts.TimeStamp != "" {
		timeStamp, err = time.ParseInLocation(global.TimeFormat, opts.TimeStamp, time.Local)
		if err != nil {
			return errors.Fatalf("error in time option: %v", err)
		}
	}

	if gopts.Verbosity >= 2 && !gopts.JSON {
		printer.P("open repository")
	}
	if opts.Init {
		_, err := global.OpenRepository(ctx, gopts, printer)
		if errors.Is(err, global.ErrNoRepository) {
			if _, err := global.CreateRepository(ctx, gopts, vaultic.StableRepoVersion, nil, printer); err != nil {
				return errors.Fatalf("initialize repository: %v", err)
			}
		} else if err != nil {
			return err
		}
	}

	deferredActive := opts.AllowDeferredCommit && opts.DeferredMode != "auto"
	metadataBypassed := opts.DeferredMode == "data-plane-only"
	var repo *repository.Repository
	var unlock func()
	if opts.DeferredMode == "data-plane-only" {
		repo, err = global.OpenDataPlaneRepository(ctx, gopts, printer)
		if err == nil {
			unlock = func() { _ = repo.Close() }
		}
	} else {
		ctx, repo, unlock, err = openWithAppendLock(ctx, gopts, opts.DryRun, printer)
		if shouldUseDataPlaneFallback(err, opts) {
			repo, err = global.OpenDataPlaneRepository(ctx, gopts, printer)
			if err == nil {
				deferredActive = true
				metadataBypassed = true
				unlock = func() { _ = repo.Close() }
			}
		} else if err == nil && opts.DeferredMode == "auto" {
			if engine, ok := repo.Engine().(*enginepkg.DaemonEngine); ok {
				status, statusErr := engine.Client().WriterStatus(ctx)
				if statusErr != nil || status.Role != "read-write" {
					deferredActive = true
				}
			}
		}
	}
	if err != nil {
		return err
	}
	defer unlock()
	if deferredActive {
		severity := observability.Notice
		message := "deferred backup mode selected"
		if metadataBypassed {
			severity = observability.Critical
			message = "metadata bypassed for data-plane-only deferred crawl"
		}
		_ = observability.Emit(ctx, observability.Event{Severity: severity, Category: observability.CategoryLifecycle, Component: "backup", Message: message, Fields: map[string]any{"mode": opts.DeferredMode}})
	}

	progressReporter := backup.NewProgress(printer, gopts.Quiet, gopts.JSON, term.CanUpdateStatus())
	defer progressReporter.Done()

	// rejectByNameFuncs collect functions that can reject items from the backup based on path only
	rejectByNameFuncs, err := collectRejectByNameFuncs(opts, repo, printer.E)
	if err != nil {
		return err
	}

	var parentSnapshot *data.Snapshot
	if !opts.Stdin && !deferredActive {
		parentSnapshot, err = findParentSnapshot(ctx, repo, opts, targets, timeStamp)
		if err != nil {
			return err
		}

		if !gopts.JSON {
			if parentSnapshot != nil {
				printer.P("using parent snapshot %v\n", parentSnapshot.ID().Str())
			} else {
				printer.P("no parent snapshot found, will read all files\n")
			}
		}
	}

	if !gopts.JSON && !deferredActive {
		printer.V("load index files")
	}
	if !deferredActive {
		err = repo.LoadIndex(ctx, printer)
		if err != nil {
			return err
		}
	}

	targetFS := fs.NewLocal()
	if runtime.GOOS == "windows" && opts.UseFsSnapshot {
		if err = fs.HasSufficientPrivilegesForVSS(); err != nil {
			return err
		}

		errorHandler := func(item string, err error) {
			_ = progressReporter.Error(item, err)
		}

		messageHandler := func(msg string, args ...any) {
			if !gopts.JSON {
				printer.P(msg, args...)
			}
		}

		localVss := fs.NewLocalVss(errorHandler, messageHandler, vsscfg)
		defer localVss.DeleteSnapshots()
		targetFS = localVss
	}

	if opts.Stdin || opts.StdinCommand {
		if !gopts.JSON {
			printer.V("read data from stdin")
		}
		filename := path.Join("/", opts.StdinFilename)
		source := term.InputRaw()
		if opts.StdinCommand {
			source, err = fs.NewCommandReader(ctx, args, printer.E)
			if err != nil {
				return err
			}
		}
		targetFS, err = fs.NewReader(filename, source, fs.ReaderOptions{
			ModTime: timeStamp,
			Mode:    0644,
		})
		if err != nil {
			return fmt.Errorf("failed to backup from stdin: %w", err)
		}
		targets = []string{filename}
	}

	if backupFSTestHook != nil {
		targetFS = backupFSTestHook(targetFS)
	}

	pathdiffPlan := crawl.Plan{Reason: "pathdiff is disabled"}
	if opts.UsePathdiff {
		if !fs.IsLocal(targetFS) {
			pathdiffPlan.Reason = "source does not use the plain local filesystem"
		} else if parentSnapshot == nil {
			pathdiffPlan.Reason = "no parent snapshot is available"
		} else {
			topology, topologyErr := crawl.LoadTopology(opts.PathdiffSVMMap)
			if topologyErr != nil {
				pathdiffPlan.Reason = topologyErr.Error()
			} else {
				service := crawl.NewPathdiffService(uppathdiff.NewClient(opts.PathdiffEndpoint))
				pathdiffPlan, err = crawl.BuildPathdiffPlan(ctx, service, topology, targets, parentSnapshot.Time, backupStart)
				if err != nil {
					return fmt.Errorf("build pathdiff crawl plan: %w", err)
				}
			}
		}
		if !pathdiffPlan.Selective {
			if opts.PathdiffRequireCoverage {
				return errors.Fatalf("pathdiff coverage is required: %s", pathdiffPlan.Reason)
			}
			if !gopts.JSON {
				printer.V("pathdiff coverage unverified, using full cwalk traversal: %s", pathdiffPlan.Reason)
			}
		} else if !gopts.JSON {
			printer.V("pathdiff coverage verified; crawling %d changed subtrees", len(pathdiffPlan.ChangedDirs))
		}
	}

	// rejectFuncs collect functions that can reject items from the backup based on path and file info
	rejectFuncs, err := collectRejectFuncs(opts, targets, targetFS, printer.E)
	if err != nil {
		return err
	}
	var authoritativeEngine *enginepkg.DaemonEngine
	var mandatorySelect archiver.SelectFunc
	if engine, ok := repo.Engine().(*enginepkg.DaemonEngine); ok && !deferredActive {
		authoritativeEngine = engine
		excludedUIDs, err := analytics.ExcludedUIDs(ctx, engine.SchemaStore())
		if err != nil {
			return fmt.Errorf("load UID exclusion policy: %w", err)
		}
		if len(excludedUIDs) > 0 {
			uidPolicy := archiver.RejectUIDs(excludedUIDs)
			rejectFuncs = append(rejectFuncs, uidPolicy)
			mandatorySelect = archiver.CombineRejects([]archiver.RejectFunc{uidPolicy})
		}
	}

	selectByNameFilter := archiver.CombineRejectByNames(rejectByNameFuncs)
	selectFilter := archiver.CombineRejects(rejectFuncs)

	wg, wgCtx := errgroup.WithContext(ctx)
	cancelCtx, cancel := context.WithCancel(wgCtx)
	defer cancel()

	if !opts.NoScan && !pathdiffPlan.Selective && !opts.UseCWalk {
		sc := archiver.NewScanner(targetFS)
		sc.SelectByName = selectByNameFilter
		sc.Select = selectFilter
		sc.Error = printer.ScannerError
		sc.Result = progressReporter.ReportTotal

		if !gopts.JSON {
			printer.V("start scan on %v", targets)
		}
		wg.Go(func() error { return sc.Scan(cancelCtx, targets) })
	}

	archiverOptions := archiver.Options{ReadConcurrency: opts.ReadConcurrency}
	if opts.UseCWalk && (!pathdiffPlan.Selective || len(pathdiffPlan.ChangedDirs) > 0) {
		archiverOptions.CWalkConcurrency = opts.CWalkConcurrency
		archiverOptions.CWalkQueue = 4096
		if pathdiffPlan.Selective {
			archiverOptions.CWalkRoots = pathdiffPlan.ChangedDirs
		}
	}
	archiverRepository := repo.AppendTransaction()
	if deferredActive {
		archiverRepository = repo.DeferredTransaction()
	}
	arch := archiver.New(archiverRepository, targetFS, archiverOptions)
	arch.SelectByName = selectByNameFilter
	arch.Select = selectFilter
	if pathdiffPlan.Selective {
		arch.ReuseSubtree = func(_ string, sourcePath string, _ *data.Node) bool {
			return pathdiffPlan.ReuseSubtree(sourcePath)
		}
	}
	if mandatorySelect != nil {
		arch.MandatorySelect = mandatorySelect
	}
	arch.WithAtime = opts.WithAtime

	arch.Error = func(item string, err error) error {
		success = false
		reterr := progressReporter.Error(item, err)
		// If we receive a fatal error during the execution of the snapshot,
		// we abort the snapshot.
		if reterr == nil && errors.IsFatal(err) {
			reterr = err
		}
		return reterr
	}
	arch.CompleteItem = progressReporter.CompleteItem
	arch.StartFile = progressReporter.StartFile
	arch.CompleteBlob = progressReporter.CompleteBlob
	arch.ExcludedItem = progressReporter.ExcludedItem

	var reconciler *reconcile.Reconciler
	var deferredCapture *reconcile.DeferredCapture
	if authoritativeEngine != nil {
		reconciler, err = reconcile.New(cancelCtx, targetFS, authoritativeEngine.SchemaStore(), reconcile.Options{PathIndexPaths: repo.Config().PathIndexPaths})
		if err != nil {
			return fmt.Errorf("start authoritative metadata reconciliation: %w", err)
		}
		reconcile.Attach(arch, reconciler)
		arch.BeforeSnapshot = func() error {
			if err := reconciler.Close(); err != nil {
				return err
			}
			authoritativeEngine.SetNextSnapshotRoot(reconciler.RootKey())
			return nil
		}
	}

	if opts.IgnoreInode {
		// --ignore-inode implies --ignore-ctime: on FUSE, the ctime is not
		// reliable either.
		arch.ChangeIgnoreFlags |= archiver.ChangeIgnoreCtime | archiver.ChangeIgnoreInode
	}
	if opts.IgnoreCtime {
		arch.ChangeIgnoreFlags |= archiver.ChangeIgnoreCtime
	}

	snapshotOpts := archiver.SnapshotOptions{
		Excludes:        opts.Excludes,
		Tags:            opts.Tags.Flatten(),
		BackupStart:     backupStart,
		Time:            timeStamp,
		Hostname:        opts.Host,
		Label:           opts.Label,
		ParentSnapshot:  parentSnapshot,
		ProgramVersion:  "vaultic " + global.Version,
		SkipIfUnchanged: opts.SkipIfUnchanged,
	}
	var deferredResult repository.DeferredUploadResult
	var deferredStore staging.Store
	if deferredActive {
		deferredCapture = reconcile.NewDeferredCapture(targetFS)
		previousReconcileNode := arch.ReconcileNode
		arch.ReconcileNode = func(snapshotPath, sourcePath string, node *data.Node) {
			previousReconcileNode(snapshotPath, sourcePath, node)
			deferredCapture.Observe(snapshotPath, sourcePath, node)
		}
		uploadOptions, store, err := repo.DeferredUploadPlan()
		if err != nil {
			return err
		}
		quotaConfig := repo.Config().StagingQuota
		quota := staging.Quota{MaxBytes: quotaConfig.MaxBytes, MaxJobs: quotaConfig.MaxJobs, MaxAge: time.Duration(quotaConfig.MaxAgeSeconds) * time.Second}
		usage, err := store.ActiveUsage(ctx, repo.Config().ID)
		if err != nil {
			return fmt.Errorf("inspect deferred staging quota: %w", err)
		}
		if err := staging.CheckQuota(quota, usage.Jobs, 0, usage.Bytes, usage.OldestJobAt, 0, time.Now().UTC()); err != nil {
			_ = observability.Emit(ctx, observability.Event{Severity: observability.Error, Category: observability.CategoryLifecycle, Component: "backup", Message: "deferred staging quota refused upload"})
			return err
		}
		if quota.MaxBytes > 0 {
			uploadOptions.MaxAdditionalBytes = quota.MaxBytes - usage.Bytes
		}
		deferredStore = store
		snapshotOpts.DeferredUploader = func(ctx context.Context, fn func(context.Context, vaultic.BlobSaverWithAsync) error) error {
			var uploadErr error
			deferredResult, uploadErr = repo.WithDeferredBlobUploader(ctx, uploadOptions, fn)
			return uploadErr
		}
	}

	// resolve description (--description-from overrides --description)
	if opts.DescriptionFrom != "" {
		data, err := textfile.Read(opts.DescriptionFrom)
		if err != nil {
			return errors.Fatalf("unable to read description from %q: %v", opts.DescriptionFrom, err)
		}
		snapshotOpts.Description = strings.TrimSpace(string(data))
	} else {
		snapshotOpts.Description = opts.Description
	}

	// resolve delete protection (--delete-never wins over --delete-after)
	if opts.DeleteNever {
		snapshotOpts.Delete = &data.DeleteOption{Never: true}
	} else if opts.DeleteAfter != "" {
		dur, err := data.ParseDuration(opts.DeleteAfter)
		if err != nil || dur.Zero() {
			return errors.Fatalf("invalid --delete-after duration %q: %v", opts.DeleteAfter, err)
		}
		until := timeStamp.AddDate(dur.Years, dur.Months, dur.Days).Add(time.Duration(dur.Hours) * time.Hour)
		snapshotOpts.Delete = &data.DeleteOption{After: &until}
	}

	if !gopts.JSON {
		printer.V("start backup on %v", targets)
	}
	snapshot, id, summary, err := arch.Snapshot(ctx, targets, snapshotOpts)
	var deferredJobID string
	var deferredSeal staging.Seal
	if err == nil && deferredActive {
		observations, captureErr := deferredCapture.Close()
		if captureErr != nil {
			err = captureErr
		}
		deferredJobID = vaultic.NewRandomID().String()
		snapshotPayload, marshalErr := json.Marshal(snapshot)
		if marshalErr != nil {
			err = marshalErr
		} else if err == nil {
			sourceDigest := sha256.Sum256([]byte(strings.Join(targets, "\x00")))
			header := staging.Header{
				Format: 1, RepositoryID: repo.Config().ID, JobID: deferredJobID, IdempotencyKey: deferredJobID,
				CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(opts.DeferredExpiry),
				CapsuleGeneration: 1, RepositoryKeyVersion: 1, ChunkerVersion: "rabin-v1",
				CompressionVersion: fmt.Sprintf("repository-v%d", repo.Config().Version), PlacementPolicyVersion: 1,
				SourceIdentitySHA256: hex.EncodeToString(sourceDigest[:]), ConsistencyEvidence: "full-crawl",
			}
			records := append(deferredResult.Records, staging.Record{Kind: "prospective-snapshot-v1", Payload: snapshotPayload})
			for _, observation := range observations {
				payload, marshalErr := json.Marshal(observation)
				if marshalErr != nil {
					err = marshalErr
					break
				}
				records = append(records, staging.Record{Kind: reconcile.DeferredObservationKind, Payload: payload})
			}
			if err != nil {
				return err
			}
			deferredSeal, _, _, err = deferredStore.PublishJob(ctx, header, deferredResult.Packs, records)
			if err == nil {
				_ = observability.Emit(ctx, observability.Event{Severity: observability.Info, Category: observability.CategoryIntegrity, Component: "backup", Message: "deferred pack durability verified", Fields: map[string]any{"job_id": deferredJobID, "pack_count": deferredSeal.PackCount, "protected_bytes": deferredSeal.ProtectedBytes}})
				_ = observability.Emit(ctx, observability.Event{Severity: observability.Notice, Category: observability.CategoryLifecycle, Component: "staging", Message: "deferred ingest journal sealed", Fields: map[string]any{"job_id": deferredJobID, "expires_at": deferredSeal.Header.ExpiresAt}})
			}
		}
	}
	if reconciler != nil {
		if reconcileErr := reconciler.Close(); reconcileErr != nil && err == nil {
			err = fmt.Errorf("reconcile authoritative snapshot metadata: %w", reconcileErr)
		} else if err == nil && !id.IsNull() {
			rootKey := reconciler.RootKey()
			if len(rootKey) == 0 {
				err = fmt.Errorf("reconcile authoritative snapshot metadata: missing snapshot root")
			} else if publishErr := authoritativeEngine.PublishSnapshotScope(ctx, id, rootKey); publishErr != nil {
				err = fmt.Errorf("publish authoritative snapshot scope: %w", publishErr)
			}
		}
	}

	// cleanly shutdown all running goroutines
	cancel()

	// let's see if one returned an error
	werr := wg.Wait()

	// return original error
	if err != nil {
		return errors.Fatalf("unable to save snapshot: %v", err)
	}
	if deferredActive {
		if werr != nil {
			return werr
		}
		if !success {
			return ErrInvalidSourceData
		}
		placements := make(map[string]uint64)
		for _, pack := range deferredResult.Packs {
			for _, placement := range pack.Placements {
				placements[placement.BackendID]++
			}
		}
		result := map[string]any{"state": "data_durable_metadata_pending", "job_id": deferredJobID, "packs": len(deferredResult.Packs), "protected_bytes": deferredSeal.ProtectedBytes, "placements": placements, "expires_at": deferredSeal.Header.ExpiresAt, "reason": opts.DeferredMode}
		if gopts.JSON {
			encoded, _ := json.Marshal(result)
			term.Print(string(encoded))
		} else {
			printer.P("data durable; metadata pending (job %s, %d packs, expires in %s)\n", deferredJobID, len(deferredResult.Packs), opts.DeferredExpiry)
		}
		return nil
	}

	// Report finished execution
	progressReporter.Finish(id, summary, opts.DryRun)
	if !success {
		return ErrInvalidSourceData
	}

	// Return errors before publishing telemetry: metrics represent only fully
	// successful backups with a durable snapshot.
	if werr != nil {
		return werr
	}
	if authoritativeEngine != nil && !opts.DryRun {
		model, placementErr := indexMaintenancePlacementModel(repo)
		if placementErr == nil {
			_, placementErr = maintenance.PlanPlacement(ctx, authoritativeEngine.SchemaStore(), maintenance.PlacementSchedulerOptions{Model: model, Now: time.Now()})
		}
		if placementErr == nil {
			_, placementErr = maintenance.ExecutePlacement(ctx, authoritativeEngine.SchemaStore(), repositoryPlacementActions{repo: repo, printer: printer}, maintenance.PlacementWorkerOptions{
				Model: model, Now: time.Now(), MaxRequests: 1,
			})
		}
		if placementErr != nil {
			printer.E("placement scheduler tick failed: %v\n", placementErr)
		}
	}
	if err := telemetry.Publish(ctx, telemetry.Config{
		PrometheusURL:  gopts.PrometheusURL,
		PrometheusUser: gopts.PrometheusUser,
		PrometheusPass: gopts.PrometheusPass,
		InfluxURL:      gopts.InfluxURL,
		InfluxToken:    gopts.InfluxToken,
		InfluxOrg:      gopts.InfluxOrg,
		InfluxBucket:   gopts.InfluxBucket,
	}, telemetry.Backup{Repository: gopts.Repo, SnapshotID: id.String(), Label: snapshotOpts.Label, Summary: summary}); err != nil {
		// The snapshot is already durable. Observability outages must not turn a
		// completed backup into a failed one.
		printer.E("telemetry publish failed: %v\n", err)
	}
	if opts.List {
		if err := runLs(ctx, LsOptions{}, gopts, []string{id.String()}, term); err != nil {
			// The snapshot is already durable. Keep the backup successful even if
			// the optional post-backup listing cannot be rendered.
			printer.E("listing created snapshot failed: %v\n", err)
		}
	}
	return nil
}

func shouldUseDataPlaneFallback(openErr error, opts BackupOptions) bool {
	return openErr != nil && opts.DeferredMode == "auto" && (opts.AcknowledgeMetadataBypass || errors.Is(openErr, enginepkg.ErrUnavailable))
}
