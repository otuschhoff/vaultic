package backupcmd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/cmd/vaultic/querycmd"
	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/configfile"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/env"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/filter"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/hooks"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/textfile"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func NewCommand(globalOptions *global.Options) *cobra.Command {
	var options backupOptions

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
			return options.Finalize()
		},
		GroupID:           "default",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 && globalOptions.Profile != nil && len(globalOptions.Profile.Snapshots) != 0 {
				return runProfileBackupJobs(cmd.Context(), options, *globalOptions, globalOptions.Term, cmd.Flags())
			}
			return runBackup(cmd.Context(), options, *globalOptions, globalOptions.Term, args)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

// backupOptions bundles all options for the backup command.
type backupOptions struct {
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
	FSTestHook          func(fs.FS) fs.FS
}

func (options *backupOptions) AddFlags(f *pflag.FlagSet) {
	f.StringVar(
		&options.Parent,
		"parent",
		"",
		"use this parent `snapshot` (default: latest snapshot in the group determined by --group-by and not newer than the timestamp determined by --time)",
	)
	options.GroupBy = data.SnapshotGroupByOptions{Host: true, Path: true}
	f.VarP(&options.GroupBy, "group-by", "g", "`group` snapshots by host, paths and/or tags, separated by comma (disable grouping with '')")
	f.BoolVarP(&options.Force, "force", "f", false, `force re-reading the source files/directories (overrides the "parent" flag)`)

	options.ExcludePatternOptions.Add(f)

	f.BoolVarP(&options.ExcludeOtherFS, "one-file-system", "x", false, "exclude other file systems, don't cross filesystem boundaries and subvolumes")
	f.StringArrayVar(
		&options.ExcludeIfPresent,
		"exclude-if-present",
		nil,
		"takes `filename[:header]`, exclude contents of directories containing filename (except filename itself) "+
			"if header of that file is as provided (can be specified multiple times)",
	)
	f.BoolVar(
		&options.ExcludeCaches,
		"exclude-caches",
		false,
		`excludes cache directories that are marked with a CACHEDIR.TAG file. See https://bford.info/cachedir/ for the Cache Directory Tagging Standard`,
	)
	f.StringVar(&options.ExcludeLargerThan, "exclude-larger-than", "", "max `size` of the files to be backed up (allowed suffixes: k/K, m/M, g/G, t/T)")
	f.BoolVar(&options.Stdin, "stdin", false, "read backup from stdin")
	f.StringVar(&options.StdinFilename, "stdin-filename", "stdin", "`filename` to use when reading from stdin")
	f.BoolVar(&options.StdinCommand, "stdin-from-command", false, "interpret arguments as command to execute and store its stdout")
	f.Var(&options.Tags, "tag", "add `tags` for the new snapshot in the format `tag[,tag,...]` (can be specified multiple times)")
	f.StringVar(&options.Label, "label", "", "set a `label` for the new snapshot (for grouping/filtering)")
	f.StringVar(&options.Description, "description", "", "add a `description` to the new snapshot")
	f.StringVar(&options.DescriptionFrom, "description-from", "", "read the snapshot description from `file`")
	f.BoolVar(&options.DeleteNever, "delete-never", false, "mark the snapshot as never deletable by forget (delete protection)")
	f.StringVar(&options.DeleteAfter, "delete-after", "", "mark the snapshot as not deletable before a `duration` from now (e.g. 10d; delete protection)")
	f.UintVar(&options.ReadConcurrency, "read-concurrency", 0, "read `n` files concurrently (default: $VAULTIC_READ_CONCURRENCY or 2)")
	f.StringVarP(
		&options.Host,
		"host",
		"H",
		"",
		"set the `hostname` for the snapshot manually (default: $VAULTIC_HOST). To prevent an expensive rescan use the \"parent\" flag",
	)
	f.StringVar(&options.Host, "hostname", "", "set the `hostname` for the snapshot manually")
	err := f.MarkDeprecated("hostname", "use --host")
	if err != nil {
		// MarkDeprecated only returns an error when the flag could not be found
		// Flag registration is a construction-time invariant.
		panic(err) //nolint:forbidigo // A missing flag here is a command-construction defect.
	}
	f.StringArrayVar(
		&options.FilesFrom,
		"files-from",
		nil,
		"read the files to backup from `file` (can be combined with file args; can be specified multiple times)",
	)
	f.StringArrayVar(
		&options.FilesFromVerbatim,
		"files-from-verbatim",
		nil,
		"read the files to backup from `file` (can be combined with file args; can be specified multiple times)",
	)
	f.StringArrayVar(
		&options.FilesFromRaw,
		"files-from-raw",
		nil,
		"read the files to backup from `file` (can be combined with file args; can be specified multiple times)",
	)
	options.addTraversalFlags(f)
	f.BoolVar(&options.SkipIfUnchanged, "skip-if-unchanged", false, "skip snapshot creation if identical to parent snapshot")
	f.BoolVar(
		&options.AllowDeferredCommit,
		"allow-deferred-commit",
		false,
		"acknowledge that durable data remains unavailable as a snapshot until metadata reconciliation",
	)
	f.StringVar(&options.DeferredMode, "deferred-mode", "", "deferred ingest mode (auto, read-only-assisted, or data-plane-only)")
	f.DurationVar(&options.DeferredExpiry, "deferred-expiry", 72*time.Hour, "staging journal expiry for deferred ingest")
	f.BoolVar(
		&options.AcknowledgeMetadataBypass,
		"acknowledge-metadata-bypass",
		false,
		"acknowledge that data-plane-only mode ignores unavailable or corrupt metadata",
	)
	f.StringSliceVar(&options.ProfileNames, "name", nil, "run named [[backup.snapshots]] profile job (repeatable)")
	f.BoolVar(&options.Init, "init", false, "initialize the repository if it does not exist")
	f.BoolVar(&options.List, "ls", false, "list the contents of the created snapshot")

	options.readConcurrencyFlag = f.Lookup("read-concurrency")

	// parse read inode and ctime from env, on error the default value will be used
	options.IgnoreInode = parseOptionalBool(env.Get("IGNORE_INODE"))
	options.IgnoreCtime = parseOptionalBool(env.Get("IGNORE_CTIME"))

	// parse host from env, if not exists or empty the default value will be used
	if host := env.Get("HOST"); host != "" {
		options.Host = host
	}
}

func (options *backupOptions) addTraversalFlags(f *pflag.FlagSet) {
	f.StringVar(&options.TimeStamp, "time", "", "`time` of the backup (ex. '2012-11-01 22:08:41') (default: now)")
	f.BoolVar(&options.WithAtime, "with-atime", false, "store the atime for all files and directories")
	f.BoolVar(
		&options.IgnoreInode,
		"ignore-inode",
		false,
		"ignore inode number and ctime changes when checking for modified files (default: $VAULTIC_IGNORE_INODE or false)",
	)
	f.BoolVar(&options.IgnoreCtime, "ignore-ctime", false, "ignore ctime changes when checking for modified files (default: $VAULTIC_IGNORE_CTIME or false)")
	f.BoolVarP(&options.DryRun, "dry-run", "n", false, "do not upload or write any data, just show what would be done")
	f.BoolVar(&options.NoScan, "no-scan", false, "do not run scanner to estimate size of backup")
	f.BoolVar(&options.UseCWalk, "use-cwalk", false, "use parallel cwalk traversal for the backup scanner")
	f.IntVar(&options.CWalkConcurrency, "cwalk-concurrency", runtime.GOMAXPROCS(0), "run `n` concurrent cwalk workers")
	f.BoolVar(&options.UsePathdiff, "use-pathdiff", false, "use verified pathdiff events to skip unchanged subtrees")
	f.StringVar(&options.PathdiffEndpoint, "pathdiff-endpoint", "", "pathdiff control socket `path`")
	f.BoolVar(
		&options.PathdiffRequireCoverage,
		"pathdiff-require-coverage",
		false,
		"fail instead of performing a full crawl when pathdiff coverage is unverified",
	)
	f.StringVar(&options.PathdiffSVMMap, "pathdiff-svm-map", "", "pathdiff source-to-LIF/SVM/volume topology JSON `file`")
	if runtime.GOOS == "windows" {
		f.BoolVar(&options.UseFsSnapshot, "use-fs-snapshot", false, "use filesystem snapshot where possible (currently only Windows VSS)")
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		f.BoolVar(&options.ExcludeCloudFiles, "exclude-cloud-files", false, "excludes online-only cloud files (such as OneDrive, iCloud drive, …)")
	}
}

func runProfileBackupJobs(ctx context.Context, base backupOptions, globalOptions global.Options, term ui.Terminal, commandFlags *pflag.FlagSet) error {
	profile := globalOptions.Profile
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
		if err := runProfileBackupJob(ctx, job, profile.Sections["backup"], globalOptions, term, commandFlags); err != nil {
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

func runProfileBackupJob(
	ctx context.Context,
	job configfile.SnapshotJob,
	backupValues map[string]any,
	globalOptions global.Options,
	term ui.Terminal,
	commandFlags *pflag.FlagSet,
) error {
	var jobOptions backupOptions
	jobFlags := pflag.NewFlagSet("profile backup", pflag.ContinueOnError)
	jobFlags.SetInterspersed(true)
	jobOptions.AddFlags(jobFlags)
	envOverrides := func(name string) bool {
		_, ok := env.Lookup(strings.ToUpper(strings.ReplaceAll(name, "-", "_")))
		return ok
	}
	if err := configfile.ApplyValues(backupValues, jobFlags, envOverrides); err != nil {
		return err
	}
	if err := configfile.ApplyValues(job.Values, jobFlags, envOverrides); err != nil {
		return err
	}
	var setFlagErr error
	commandFlags.Visit(func(flag *pflag.Flag) {
		if jobFlags.Lookup(flag.Name) != nil && flag.Value.Type() != "stringArray" {
			setFlagErr = errors.Join(setFlagErr, jobFlags.Set(flag.Name, flag.Value.String()))
		}
	})
	if setFlagErr != nil {
		return fmt.Errorf("apply command-line profile overrides: %w", setFlagErr)
	}
	if err := jobOptions.Finalize(); err != nil {
		return err
	}

	values := hooks.Context{
		Action: "backup", BackupLabel: jobOptions.Label,
		BackupSources: job.Sources, BackupTags: jobOptions.Tags.Flatten(),
	}
	runner := hooks.Runner{
		Stdout: term.OutputWriter(), Stderr: term.OutputWriter(),
		Warn: func(format string, args ...any) { term.Error(fmt.Sprintf(format, args...)) },
	}
	if err := runner.Run(ctx, hooks.Before, []configfile.Hooks{job.Hooks}, values); err != nil {
		return err
	}
	err := runBackup(ctx, jobOptions, globalOptions, term, job.Sources)
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
	return err
}

func parseOptionalBool(value string) bool {
	parsed, _ := strconv.ParseBool(value) // Invalid optional environment values retain the false default.
	return parsed
}

func (options *backupOptions) Finalize() error {
	if envVal := env.Get("READ_CONCURRENCY"); envVal != "" && !options.readConcurrencyFlag.Changed {
		n, err := strconv.ParseUint(envVal, 10, 32)
		if err != nil {
			return errors.Fatalf("invalid value for VAULTIC_READ_CONCURRENCY (legacy: RESTIC_READ_CONCURRENCY) %q: %v", envVal, err)
		}
		options.ReadConcurrency = uint(n)
	}
	if options.Host == "" {
		hostname, err := os.Hostname()
		if err != nil {
			debug.Log("os.Hostname() returned err: %v", err)
			return nil
		}
		options.Host = hostname
	}
	return nil
}

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
func (options backupOptions) Check(globalOptions global.Options, args []string) error {
	if err := options.validateDeferred(); err != nil {
		return err
	}
	if err := options.validateParent(); err != nil {
		return err
	}
	return options.validateStdin(globalOptions, args)
}

func (options backupOptions) validateDeferred() error {
	if options.DeferredMode != "" && !options.AllowDeferredCommit {
		return errors.Fatal("--deferred-mode requires --allow-deferred-commit")
	}
	if options.AllowDeferredCommit && options.DeferredMode != "auto" &&
		options.DeferredMode != "read-only-assisted" && options.DeferredMode != "data-plane-only" {
		return errors.Fatal("--allow-deferred-commit requires --deferred-mode=auto, read-only-assisted, or data-plane-only")
	}
	if options.DeferredMode == "data-plane-only" && !options.AcknowledgeMetadataBypass {
		return errors.Fatal("--deferred-mode=data-plane-only requires --acknowledge-metadata-bypass")
	}
	if options.AllowDeferredCommit && (options.Parent != "" || options.SkipIfUnchanged || options.DryRun || options.Init || options.List) {
		return errors.Fatal("deferred ingest cannot use --parent, --skip-if-unchanged, --dry-run, --init, or --list")
	}
	if options.AllowDeferredCommit && options.DeferredExpiry <= 0 {
		return errors.Fatal("--deferred-expiry must be positive")
	}
	return nil
}

func (options backupOptions) validateParent() error {
	if options.UseCWalk && options.CWalkConcurrency < 1 {
		return errors.Fatal("--cwalk-concurrency must be at least 1")
	}
	if options.UsePathdiff && !options.UseCWalk {
		return errors.Fatal("--use-pathdiff requires --use-cwalk")
	}
	if options.UsePathdiff && options.PathdiffEndpoint == "" {
		return errors.Fatal("--use-pathdiff requires --pathdiff-endpoint")
	}
	if options.UsePathdiff && options.PathdiffSVMMap == "" {
		return errors.Fatal("--use-pathdiff requires --pathdiff-svm-map")
	}
	if options.PathdiffRequireCoverage && !options.UsePathdiff {
		return errors.Fatal("--pathdiff-require-coverage requires --use-pathdiff")
	}
	return nil
}

func (options backupOptions) validateStdin(globalOptions global.Options, args []string) error {
	if globalOptions.Password == "" && !globalOptions.InsecureNoPassword {
		if options.Stdin {
			return errors.Fatal("cannot read both password and data from stdin")
		}

		filesFrom := append(append(options.FilesFrom, options.FilesFromVerbatim...), options.FilesFromRaw...)
		if slices.Contains(filesFrom, "-") {
			return errors.Fatal("unable to read password from stdin when data is to be read from stdin, use --password-file or $VAULTIC_PASSWORD")
		}
	}

	if options.Stdin || options.StdinCommand {
		if len(options.FilesFrom) > 0 {
			return errors.Fatal("--stdin and --files-from cannot be used together")
		}
		if len(options.FilesFromVerbatim) > 0 {
			return errors.Fatal("--stdin and --files-from-verbatim cannot be used together")
		}
		if len(options.FilesFromRaw) > 0 {
			return errors.Fatal("--stdin and --files-from-raw cannot be used together")
		}

		if len(args) > 0 && !options.StdinCommand {
			return errors.Fatal("--stdin was specified and files/dirs were listed as arguments")
		}
	}

	return nil
}

// collectRejectByNameFuncs returns a list of all functions which may reject data
// from being saved in a snapshot based on path only
func collectRejectByNameFuncs(
	options backupOptions,
	repo *repository.Repository,
	warnf func(msg string, args ...any),
) (fs []archiver.RejectByNameFunc, err error) {
	// exclude vaultic cache
	if repo.Cache() != nil {
		f, err := querycmd.RejectResticCache(repo)
		if err != nil {
			return nil, err
		}

		fs = append(fs, f)
	}

	fsPatterns, err := options.ExcludePatternOptions.CollectPatterns(warnf)
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
func collectRejectFuncs(options backupOptions, targets []string, fs fs.FS, warnf func(msg string, args ...any)) (funcs []archiver.RejectFunc, err error) {
	// allowed devices
	if options.ExcludeOtherFS && !options.Stdin && !options.StdinCommand {
		f, err := archiver.RejectByDevice(targets, fs)
		if err != nil {
			return nil, err
		}
		funcs = append(funcs, f)
	}

	if len(options.ExcludeLargerThan) != 0 && !options.Stdin && !options.StdinCommand {
		maxSize, err := ui.ParseBytes(options.ExcludeLargerThan)
		if err != nil {
			return nil, err
		}

		f, err := archiver.RejectBySize(maxSize)
		if err != nil {
			return nil, err
		}
		funcs = append(funcs, f)
	}

	if options.ExcludeCloudFiles && !options.Stdin && !options.StdinCommand {
		f, err := archiver.RejectCloudFiles(warnf)
		if err != nil {
			return nil, err
		}
		funcs = append(funcs, f)
	}

	if options.ExcludeCaches {
		options.ExcludeIfPresent = append(options.ExcludeIfPresent, "CACHEDIR.TAG:Signature: 8a477f597d28d172789f06886806bc55")
	}

	for _, spec := range options.ExcludeIfPresent {
		f, err := archiver.RejectIfPresent(spec, warnf)
		if err != nil {
			return nil, err
		}

		funcs = append(funcs, f)
	}

	return funcs, nil
}

// collectTargets returns a list of target files/dirs from several sources.
func collectTargets(options backupOptions, args []string, warnf func(msg string, args ...any), stdin io.ReadCloser) (targets []string, err error) {
	if options.Stdin || options.StdinCommand {
		return nil, nil
	}

	for _, file := range options.FilesFrom {
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

	for _, file := range options.FilesFromVerbatim {
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

	for _, file := range options.FilesFromRaw {
		fromfile, err := readFilenamesFromFileRaw(file, stdin)
		if err != nil {
			return nil, err
		}
		targets = append(targets, fromfile...)
	}

	// Merge args into files-from so we can reuse the normal args checks
	// and have the ability to use both files-from and args at the same time.
	targets = append(targets, args...)
	if len(targets) == 0 && !options.Stdin {
		return nil, errors.Fatal("nothing to backup, please specify source files/dirs")
	}

	return filterExisting(targets, warnf)
}

// parent returns the ID of the parent snapshot. If there is none, nil is
// returned.
func findParentSnapshot(
	ctx context.Context,
	repo vaultic.ListerLoaderUnpacked,
	options backupOptions,
	targets []string,
	timeStampLimit time.Time,
) (*data.Snapshot, error) {
	if options.Force {
		return nil, nil
	}

	snName := options.Parent
	if snName == "" {
		snName = "latest"
	}
	f := data.SnapshotFilter{TimestampLimit: timeStampLimit}
	if options.GroupBy.Host {
		f.Hosts = []string{options.Host}
	}
	if options.GroupBy.Path {
		f.Paths = targets
	}
	if options.GroupBy.Tag {
		f.Tags = []data.TagList{options.Tags.Flatten()}
	}

	sn, _, err := f.FindLatest(ctx, repo, repo, snName)
	// Snapshot not found is ok if no explicit parent was set
	if options.Parent == "" && errors.Is(err, data.ErrNoSnapshotFound) {
		err = nil
	}
	return sn, err
}

func runBackup(ctx context.Context, options backupOptions, globalOptions global.Options, term ui.Terminal, args []string) error {
	return runBackupPipeline(ctx, options, globalOptions, term, args)
}

func shouldUseDataPlaneFallback(openErr error, options backupOptions) bool {
	return openErr != nil && options.DeferredMode == "auto" && (options.AcknowledgeMetadataBypass || errors.Is(openErr, enginepkg.ErrUnavailable))
}
