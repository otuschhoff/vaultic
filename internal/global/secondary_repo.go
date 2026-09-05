package global

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/env"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/spf13/pflag"
)

type SecondaryRepoOptions struct {
	Password string
	// from-repo options
	Repo               string
	RepositoryFile     string
	PasswordFile       string
	PasswordCommand    string
	KeyHint            string
	InsecureNoPassword bool
	// repo2 options
	LegacyRepo            string
	LegacyRepositoryFile  string
	LegacyPasswordFile    string
	LegacyPasswordCommand string
	LegacyKeyHint         string
}

func (options *SecondaryRepoOptions) AddFlags(f *pflag.FlagSet, repoPrefix string, repoUsage string) {
	f.StringVarP(&options.LegacyRepo, "repo2", "", "", repoPrefix+" `repository` "+repoUsage+" (default: $VAULTIC_REPOSITORY2)")
	f.StringVarP(
		&options.LegacyRepositoryFile,
		"repository-file2",
		"",
		"",
		"`file` from which to read the "+repoPrefix+" repository location "+repoUsage+" (default: $VAULTIC_REPOSITORY_FILE2)",
	)
	f.StringVarP(
		&options.LegacyPasswordFile,
		"password-file2",
		"",
		"",
		"`file` to read the "+repoPrefix+" repository password from (default: $VAULTIC_PASSWORD_FILE2)",
	)
	f.StringVarP(
		&options.LegacyKeyHint, "key-hint2", "", "",
		"key ID of key to try decrypting the "+repoPrefix+" repository first (default: $VAULTIC_KEY_HINT2)",
	)
	f.StringVarP(
		&options.LegacyPasswordCommand,
		"password-command2",
		"",
		"",
		"shell `command` to obtain the "+repoPrefix+" repository password from (default: $VAULTIC_PASSWORD_COMMAND2)",
	)

	// hide repo2 options
	mustConfigureSecondaryFlag(f.MarkDeprecated("repo2", "use --repo or --from-repo instead"))
	mustConfigureSecondaryFlag(f.MarkDeprecated("repository-file2", "use --repository-file or --from-repository-file instead"))
	mustConfigureSecondaryFlag(f.MarkHidden("password-file2"))
	mustConfigureSecondaryFlag(f.MarkHidden("key-hint2"))
	mustConfigureSecondaryFlag(f.MarkHidden("password-command2"))

	options.LegacyRepo = env.Get("REPOSITORY2")
	options.LegacyRepositoryFile = env.Get("REPOSITORY_FILE2")
	options.LegacyPasswordFile = env.Get("PASSWORD_FILE2")
	options.LegacyKeyHint = env.Get("KEY_HINT2")
	options.LegacyPasswordCommand = env.Get("PASSWORD_COMMAND2")

	f.StringVarP(&options.Repo, "from-repo", "", "", "source `repository` "+repoUsage+" (default: $VAULTIC_FROM_REPOSITORY)")
	f.StringVarP(
		&options.RepositoryFile,
		"from-repository-file",
		"",
		"",
		"`file` from which to read the source repository location "+repoUsage+" (default: $VAULTIC_FROM_REPOSITORY_FILE)",
	)
	f.StringVarP(
		&options.PasswordFile, "from-password-file", "", "",
		"`file` to read the source repository password from (default: $VAULTIC_FROM_PASSWORD_FILE)",
	)
	f.StringVarP(&options.KeyHint, "from-key-hint", "", "", "key ID of key to try decrypting the source repository first (default: $VAULTIC_FROM_KEY_HINT)")
	f.StringVarP(
		&options.PasswordCommand,
		"from-password-command",
		"",
		"",
		"shell `command` to obtain the source repository password from (default: $VAULTIC_FROM_PASSWORD_COMMAND)",
	)
	f.BoolVar(&options.InsecureNoPassword, "from-insecure-no-password", false, "use an empty password for the source repository (insecure)")

	options.Repo = env.Get("FROM_REPOSITORY")
	options.RepositoryFile = env.Get("FROM_REPOSITORY_FILE")
	options.PasswordFile = env.Get("FROM_PASSWORD_FILE")
	options.KeyHint = env.Get("FROM_KEY_HINT")
	options.PasswordCommand = env.Get("FROM_PASSWORD_COMMAND")
}

func (options *SecondaryRepoOptions) FillGlobalOpts(ctx context.Context, globalOptions Options, repoPrefix string) (Options, bool, error) {
	if options.Repo == "" && options.RepositoryFile == "" && options.LegacyRepo == "" && options.LegacyRepositoryFile == "" {
		return Options{}, false, errors.Fatal("Please specify a source repository location (--from-repo or --from-repository-file)")
	}

	hasFromRepo := options.Repo != "" || options.RepositoryFile != "" || options.PasswordFile != "" ||
		options.KeyHint != "" || options.PasswordCommand != "" || options.InsecureNoPassword
	hasRepo2 := options.LegacyRepo != "" || options.LegacyRepositoryFile != "" || options.LegacyPasswordFile != "" ||
		options.LegacyKeyHint != "" || options.LegacyPasswordCommand != ""

	if hasFromRepo && hasRepo2 {
		return Options{}, false, errors.Fatal("Option groups repo2 and from-repo are mutually exclusive, please specify only one")
	}

	var err error
	secondaryGlobalOptions := globalOptions
	var pwdEnv string

	if hasFromRepo {
		if options.Repo != "" && options.RepositoryFile != "" {
			return Options{}, false, errors.Fatal("Options --from-repo and --from-repository-file are mutually exclusive, please specify only one")
		}

		secondaryGlobalOptions.Repo = options.Repo
		secondaryGlobalOptions.RepositoryFile = options.RepositoryFile
		secondaryGlobalOptions.PasswordFile = options.PasswordFile
		secondaryGlobalOptions.PasswordCommand = options.PasswordCommand
		secondaryGlobalOptions.KeyHint = options.KeyHint
		secondaryGlobalOptions.InsecureNoPassword = options.InsecureNoPassword

		pwdEnv = "VAULTIC_FROM_PASSWORD"
		repoPrefix = "source"
	} else {
		if options.LegacyRepo != "" && options.LegacyRepositoryFile != "" {
			return Options{}, false, errors.Fatal("Options --repo2 and --repository-file2 are mutually exclusive, please specify only one")
		}

		secondaryGlobalOptions.Repo = options.LegacyRepo
		secondaryGlobalOptions.RepositoryFile = options.LegacyRepositoryFile
		secondaryGlobalOptions.PasswordFile = options.LegacyPasswordFile
		secondaryGlobalOptions.PasswordCommand = options.LegacyPasswordCommand
		secondaryGlobalOptions.KeyHint = options.LegacyKeyHint
		// keep existing behavior for legacy options
		secondaryGlobalOptions.InsecureNoPassword = false

		pwdEnv = "VAULTIC_PASSWORD2"
	}

	if options.Password != "" {
		secondaryGlobalOptions.Password = options.Password
	} else {
		secondaryGlobalOptions.Password, err = resolvePassword(ctx, &secondaryGlobalOptions, pwdEnv)
		if err != nil {
			return Options{}, false, err
		}
	}
	secondaryGlobalOptions.Password, err = readPassword(ctx, secondaryGlobalOptions, "enter password for "+repoPrefix+" repository: ")
	if err != nil {
		return Options{}, false, err
	}
	return secondaryGlobalOptions, hasFromRepo, nil
}

func mustConfigureSecondaryFlag(err error) {
	if err != nil {
		// Legacy flag registration is a construction-time invariant.
		panic(err) //nolint:forbidigo // Legacy flag registration is a construction-time invariant.
	}
}
