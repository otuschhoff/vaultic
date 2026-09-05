package main

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testRunKeyListOtherIDs(t testing.TB, globalOptions global.Options) []string {
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyList(ctx, globalOptions, []string{}, globalOptions.Term)
	})
	rtest.OK(t, err)

	scanner := bufio.NewScanner(buf)
	exp := regexp.MustCompile(`^ ([a-f0-9]+) `)

	IDs := []string{}
	for scanner.Scan() {
		if id := exp.FindStringSubmatch(scanner.Text()); id != nil {
			IDs = append(IDs, id[1])
		}
	}

	return IDs
}

func testRunKeyAddNewKey(t testing.TB, newPassword string, globalOptions global.Options) {
	err := withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyAddWithPassword(ctx, globalOptions, keyAddOptions{}, []string{}, globalOptions.Term, func() (string, error) {
			return newPassword, nil
		})
	})
	rtest.OK(t, err)
}

func testRunKeyAddNewKeyUserHost(t testing.TB, globalOptions global.Options) {
	newPassword := "john's geheimnis"

	t.Log("adding key for john@example.com")
	err := withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyAddWithPassword(ctx, globalOptions, keyAddOptions{
			Username: "john",
			Hostname: "example.com",
		}, []string{}, globalOptions.Term, func() (string, error) { return newPassword, nil })
	})
	rtest.OK(t, err)

	_ = withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		repo, err := global.OpenRepository(ctx, globalOptions, vaultic.NewNoopPrinter())
		rtest.OK(t, err)
		err = repo.SearchKey(ctx, newPassword, 2, "")
		rtest.OK(t, err)

		key, err := repository.LoadKey(ctx, repo, repo.KeyID())
		rtest.OK(t, err)
		rtest.Equals(t, "john", key.Username)
		rtest.Equals(t, "example.com", key.Hostname)
		return nil
	})
}

func testRunKeyPasswd(t testing.TB, newPassword string, globalOptions global.Options) {
	err := withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyPasswdWithPassword(ctx, globalOptions, keyPasswdOptions{}, []string{}, globalOptions.Term, func() (string, error) {
			return newPassword, nil
		})
	})
	rtest.OK(t, err)
}

func testRunKeyPasswdUserHost(t testing.TB, newPassword string, globalOptions global.Options) {
	t.Log("changing password and setting key for john@example.com")
	options := keyPasswdOptions{}
	options.Username = "john"
	options.Hostname = "example.com"
	err := withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyPasswdWithPassword(ctx, globalOptions, options, []string{}, globalOptions.Term, func() (string, error) { return newPassword, nil })
	})
	rtest.OK(t, err)

	globalOptions.Password = newPassword
	_ = withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		repo, err := global.OpenRepository(ctx, globalOptions, vaultic.NewNoopPrinter())
		rtest.OK(t, err)
		err = repo.SearchKey(ctx, newPassword, 1, "")
		rtest.OK(t, err)

		key, err := repository.LoadKey(ctx, repo, repo.KeyID())
		rtest.OK(t, err)
		rtest.Equals(t, "john", key.Username)
		rtest.Equals(t, "example.com", key.Hostname)
		return nil
	})
}

func testRunKeyRemove(t testing.TB, globalOptions global.Options, IDs []string) {
	t.Logf("remove %d keys: %q\n", len(IDs), IDs)
	for _, id := range IDs {
		err := withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runKeyRemove(ctx, globalOptions, []string{id}, globalOptions.Term)
		})
		rtest.OK(t, err)
	}
}

func TestKeyAddRemove(t *testing.T) {
	passwordList := []string{
		"OnnyiasyatvodsEvVodyawit",
		"raicneirvOjEfEigonOmLasOd",
	}

	env, cleanup := withTestEnvironment(t)
	// must list keys more than once
	env.globalOptions.BackendTestHook = nil
	defer cleanup()

	testRunInit(t, env.globalOptions)

	testRunKeyPasswd(t, "geheim2", env.globalOptions)
	env.globalOptions.Password = "geheim2"
	testRunKeyPasswdUserHost(t, "geheim3", env.globalOptions)
	env.globalOptions.Password = "geheim3"
	t.Logf("changed password to %q", env.globalOptions.Password)

	for _, newPassword := range passwordList {
		testRunKeyAddNewKey(t, newPassword, env.globalOptions)
		t.Logf("added new password %q", newPassword)
		env.globalOptions.Password = newPassword
		testRunKeyRemove(t, env.globalOptions, testRunKeyListOtherIDs(t, env.globalOptions))
	}

	env.globalOptions.Password = passwordList[len(passwordList)-1]
	t.Logf("testing access with last password %q\n", env.globalOptions.Password)
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyList(ctx, globalOptions, []string{}, globalOptions.Term)
	})
	rtest.OK(t, err)
	testRunCheck(t, env.globalOptions)

	testRunKeyAddNewKeyUserHost(t, env.globalOptions)
}

func TestKeyAddInvalid(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)

	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyAdd(ctx, globalOptions, keyAddOptions{
			NewPasswordFile:    "some-file",
			InsecureNoPassword: true,
		}, []string{}, globalOptions.Term)
	})
	rtest.Assert(t, strings.Contains(err.Error(), "only either"), "unexpected error message, got %q", err)

	pwfile := filepath.Join(t.TempDir(), "pwfile")
	rtest.OK(t, os.WriteFile(pwfile, []byte{}, 0o666))

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyAdd(ctx, globalOptions, keyAddOptions{
			NewPasswordFile: pwfile,
		}, []string{}, globalOptions.Term)
	})
	rtest.Assert(t, strings.Contains(err.Error(), "an empty password is not allowed by default"), "unexpected error message, got %q", err)
}

func TestKeyAddEmpty(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	// must list keys more than once
	env.globalOptions.BackendTestHook = nil
	defer cleanup()
	testRunInit(t, env.globalOptions)

	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyAdd(ctx, globalOptions, keyAddOptions{
			InsecureNoPassword: true,
		}, []string{}, globalOptions.Term)
	})
	rtest.OK(t, err)

	env.globalOptions.Password = ""
	env.globalOptions.InsecureNoPassword = true

	testRunCheck(t, env.globalOptions)
}

type emptySaveBackend struct {
	backend.Backend
}

func (b *emptySaveBackend) Save(ctx context.Context, h backend.Handle, _ backend.RewindReader) error {
	return b.Backend.Save(ctx, h, backend.NewByteReader([]byte{}, nil))
}

func TestKeyProblems(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)
	env.globalOptions.BackendTestHook = func(r backend.Backend) (backend.Backend, error) {
		return &emptySaveBackend{r}, nil
	}

	readPassword := func() (string, error) { return "geheim2", nil }

	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyPasswdWithPassword(ctx, globalOptions, keyPasswdOptions{}, []string{}, globalOptions.Term, readPassword)
	})
	t.Log(err)
	rtest.Assert(t, err != nil, "expected passwd change to fail")

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyAddWithPassword(ctx, globalOptions, keyAddOptions{}, []string{}, globalOptions.Term, readPassword)
	})
	t.Log(err)
	rtest.Assert(t, err != nil, "expected key adding to fail")

	t.Logf("testing access with initial password %q\n", env.globalOptions.Password)
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyList(ctx, globalOptions, []string{}, globalOptions.Term)
	})
	rtest.OK(t, err)
	testRunCheck(t, env.globalOptions)
}

func TestKeyCommandInvalidArguments(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)
	env.globalOptions.BackendTestHook = func(r backend.Backend) (backend.Backend, error) {
		return &emptySaveBackend{r}, nil
	}

	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyAdd(ctx, globalOptions, keyAddOptions{}, []string{"johndoe"}, globalOptions.Term)
	})
	t.Log(err)
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "no arguments"), "unexpected error for key add: %v", err)

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyPasswd(ctx, globalOptions, keyPasswdOptions{}, []string{"johndoe"}, globalOptions.Term)
	})
	t.Log(err)
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "no arguments"), "unexpected error for key passwd: %v", err)

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyList(ctx, globalOptions, []string{"johndoe"}, globalOptions.Term)
	})
	t.Log(err)
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "no arguments"), "unexpected error for key list: %v", err)

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyRemove(ctx, globalOptions, []string{}, globalOptions.Term)
	})
	t.Log(err)
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "one argument"), "unexpected error for key remove: %v", err)

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runKeyRemove(ctx, globalOptions, []string{"john", "doe"}, globalOptions.Term)
	})
	t.Log(err)
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "one argument"), "unexpected error for key remove: %v", err)
}
