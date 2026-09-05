package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/pflag"
)

var options = struct {
	Version string

	IgnoreBranchName           bool
	IgnoreUncommittedChanges   bool
	IgnoreChangelogVersion     bool
	IgnoreChangelogReleaseDate bool
	IgnoreChangelogCurrent     bool
	IgnoreDockerBuildGoVersion bool

	OutputDir string
}{}

var versionRegex = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

func init() {
	pflag.BoolVar(&options.IgnoreBranchName, "ignore-branch-name", false, "allow releasing from other branches than 'master'")
	pflag.BoolVar(&options.IgnoreUncommittedChanges, "ignore-uncommitted-changes", false, "allow uncommitted changes")
	pflag.BoolVar(&options.IgnoreChangelogVersion, "ignore-changelog-version", false, "ignore missing entry in CHANGELOG.md")
	pflag.BoolVar(&options.IgnoreChangelogReleaseDate, "ignore-changelog-release-date", false, "ignore missing subdir with date in changelog/")
	pflag.BoolVar(&options.IgnoreChangelogCurrent, "ignore-changelog-current", false, "ignore check if CHANGELOG.md is up to date")
	pflag.BoolVar(&options.IgnoreDockerBuildGoVersion, "ignore-docker-build-go-version", false, "ignore check if docker builder go version is up to date")

	pflag.StringVar(&options.OutputDir, "output-dir", "", "use `dir` as output directory")

	pflag.Parse()
}

func die(f string, args ...any) {
	if !strings.HasSuffix(f, "\n") {
		f += "\n"
	}
	f = "\x1b[31m" + f + "\x1b[0m"
	fmt.Fprintf(os.Stderr, f, args...)
	os.Exit(1)
}

func msg(f string, args ...any) {
	if !strings.HasSuffix(f, "\n") {
		f += "\n"
	}
	f = "\x1b[32m" + f + "\x1b[0m"
	fmt.Printf(f, args...)
}

func run(cmd string, args ...string) {
	c := exec.Command(cmd, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	err := c.Run()
	if err != nil {
		die("error running %s %s: %v", cmd, args, err)
	}
}

func replace(filename, from, to string) {
	reg := regexp.MustCompile(from)

	buf, err := os.ReadFile(filename)
	if err != nil {
		die("error reading file %v: %v", filename, err)
	}

	buf = reg.ReplaceAll(buf, []byte(to))
	err = os.WriteFile(filename, buf, 0644)
	if err != nil {
		die("error writing file %v: %v", filename, err)
	}
}

func rm(file string) {
	err := os.Remove(file)
	if err != nil {
		die("error removing %v: %v", file, err)
	}
}

func rmdir(dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		die("error removing %v: %v", dir, err)
	}
}

func mkdir(dir string) {
	err := os.Mkdir(dir, 0755)
	if err != nil {
		die("mkdir %v: %v", dir, err)
	}
}

func getwd() string {
	pwd, err := os.Getwd()
	if err != nil {
		die("Getwd(): %v", err)
	}
	return pwd
}

func uncommittedChanges(dirs ...string) string {
	args := []string{"status", "--porcelain", "--untracked-files=no"}
	if len(dirs) > 0 {
		args = append(args, dirs...)
	}

	changes, err := exec.Command("git", args...).Output()
	if err != nil {
		die("unable to run command: %v", err)
	}

	return string(changes)
}

func getBranchName() string {
	branch, err := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD").Output()
	if err != nil {
		die("error running 'git': %v", err)
	}

	return strings.TrimSpace(string(branch))
}

func preCheckBranchMaster() {
	if options.IgnoreBranchName {
		return
	}

	branch := getBranchName()
	if branch != "master" {
		die("wrong branch: %s", branch)
	}
}

func preCheckUncommittedChanges() {
	if options.IgnoreUncommittedChanges {
		return
	}

	changes := uncommittedChanges()
	if len(changes) > 0 {
		die("uncommitted changes found:\n%s\n", changes)
	}
}

func preCheckVersionExists() {
	buf, err := exec.Command("git", "tag", "-l").Output()
	if err != nil {
		die("error running 'git tag -l': %v", err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(buf))
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "v"+options.Version {
			die("tag v%v already exists", options.Version)
		}
	}
	if err := scanner.Err(); err != nil {
		die("error scanning version tags: %v", err)
	}
}

func preCheckChangelogCurrent() {
	if options.IgnoreChangelogCurrent {
		return
	}

	// regenerate changelog
	run("calens", "--output", "CHANGELOG.md")

	// check for uncommitted changes in changelog
	if len(uncommittedChanges("CHANGELOG.md")) > 0 {
		msg("committing file CHANGELOG.md")
		run("git", "commit", "-m", fmt.Sprintf("Generate CHANGELOG.md for %v", options.Version), "CHANGELOG.md")
	}
}

func preCheckChangelogRelease() bool {
	if options.IgnoreChangelogReleaseDate {
		return true
	}

	for _, name := range readdir("changelog") {
		if strings.HasPrefix(name, options.Version+"_") {
			return true
		}
	}

	return false
}

func createChangelogRelease() {
	date := time.Now().Format("2006-01-02")
	targetDir := filepath.Join("changelog", fmt.Sprintf("%s_%s", options.Version, date))
	unreleasedDir := filepath.Join("changelog", "unreleased")
	mkdir(targetDir)

	for _, name := range readdir(unreleasedDir) {
		if name == ".gitignore" {
			continue
		}

		src := filepath.Join("changelog", "unreleased", name)
		dest := filepath.Join(targetDir, name)

		err := os.Rename(src, dest)
		if err != nil {
			die("rename %v -> %v failed: %w", src, dest, err)
		}
	}

	run("git", "add", targetDir)
	run("git", "add", "-u", unreleasedDir)

	msg := fmt.Sprintf("Prepare changelog for %v", options.Version)
	run("git", "commit", "-m", msg, targetDir, unreleasedDir)
}

func preCheckChangelogVersion() {
	if options.IgnoreChangelogVersion {
		return
	}

	f, err := os.Open("CHANGELOG.md")
	if err != nil {
		die("unable to open CHANGELOG.md: %v", err)
	}
	defer func() {
		_ = f.Close() // Read-only changelog close cannot affect the completed scan.
	}()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if scanner.Err() != nil {
			die("error scanning: %v", scanner.Err())
		}

		if strings.Contains(strings.TrimSpace(scanner.Text()), fmt.Sprintf("Changelog for vaultic %v", options.Version)) {
			return
		}
	}

	die("CHANGELOG.md does not contain version %v", options.Version)
}

func preCheckDockerBuilderGoVersion() {
	if options.IgnoreDockerBuildGoVersion {
		return
	}

	buf, err := exec.Command("go", "version").Output()
	if err != nil {
		die("unable to check local Go version: %v", err)
	}
	localVersion := strings.TrimSpace(string(buf))

	msg("update docker container vaultic/builder")
	run("docker", "pull", "vaultic/builder")
	buf, err = exec.Command("docker", "run", "--rm", "vaultic/builder", "go", "version").Output()
	if err != nil {
		die("unable to check Go version in docker image: %v", err)
	}
	containerVersion := strings.TrimSpace(string(buf))

	if localVersion != containerVersion {
		die("version in docker container vaultic/builder is different:\n  local:     %v\n  container: %v\n",
			localVersion, containerVersion)
	}
}

func generateFiles() {
	msg("generate files")
	run("go", "run", "build.go", "-o", "vaultic-generate.temp")

	mandir := filepath.Join("doc", "man")
	rmdir(mandir)
	mkdir(mandir)
	run("./vaultic-generate.temp", "generate",
		"--man", "doc/man",
		"--zsh-completion", "doc/zsh-completion.zsh",
		"--powershell-completion", "doc/powershell-completion.ps1",
		"--fish-completion", "doc/fish-completion.fish",
		"--bash-completion", "doc/bash-completion.sh")
	rm("vaultic-generate.temp")

	run("git", "add", "doc")
	changes := uncommittedChanges("doc")
	if len(changes) > 0 {
		msg("committing manpages and auto-completion")
		run("git", "commit", "-m", "Update manpages and auto-completion", "doc")
	}
}

var versionPattern = `const Version = ".*"`

const versionCodeFile = "internal/global/global.go"

func updateVersion() {
	err := os.WriteFile("VERSION", []byte(options.Version+"\n"), 0644)
	if err != nil {
		die("unable to write version to file: %v", err)
	}

	newVersion := fmt.Sprintf("const Version = %q", options.Version)
	replace(versionCodeFile, versionPattern, newVersion)

	if len(uncommittedChanges("VERSION")) > 0 || len(uncommittedChanges(versionCodeFile)) > 0 {
		msg("committing version files")
		run("git", "commit", "-m", fmt.Sprintf("Add version for %v", options.Version), "VERSION", versionCodeFile)
	}
}

func updateVersionDev() {
	err := os.WriteFile("VERSION", []byte(options.Version+"-dev\n"), 0644)
	if err != nil {
		die("unable to write version to file: %v", err)
	}

	newVersion := fmt.Sprintf(`const Version = "%s-dev (compiled manually)"`, options.Version)
	replace(versionCodeFile, versionPattern, newVersion)

	msg("committing cmd/vaultic/global.go with dev version")
	run("git", "commit", "-m", fmt.Sprintf("Set development version for %v", options.Version), "VERSION", versionCodeFile)
}

func addTag() {
	tagname := "v" + options.Version
	msg("add tag %v", tagname)
	run("git", "tag", "-a", "-s", "-m", tagname, tagname)
}

func exportTar(version, tarFilename string) {
	cmd := fmt.Sprintf("git archive --format=tar --prefix=vaultic-%s/ v%s | gzip -n > %s",
		version, version, tarFilename)
	run("sh", "-c", cmd)
	msg("build vaultic-%s.tar.gz", version)
}

func extractTar(filename, outputDir string) {
	msg("extract tar into %v", outputDir)
	c := exec.Command("tar", "xz", "--strip-components=1", "-f", filename)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Dir = outputDir
	err := c.Run()
	if err != nil {
		die("error extracting tar: %v", err)
	}
}

func runBuild(sourceDir, outputDir, version string) {
	msg("building binaries...")
	run("docker", "run", "--rm",
		"--volume", sourceDir+":/vaultic",
		"--volume", outputDir+":/output",
		"vaultic/builder",
		"go", "run", "helpers/build-release-binaries/main.go",
		"--version", version)
}

func readdir(dir string) []string {
	fis, err := os.ReadDir(dir)
	if err != nil {
		die("readdir %v failed: %v", dir, err)
	}

	filenames := make([]string, 0, len(fis))
	for _, fi := range fis {
		filenames = append(filenames, fi.Name())
	}
	return filenames
}

func sha256sums(inputDir, outputFile string) {
	msg("running sha256sum in %v", inputDir)

	filenames := readdir(inputDir)

	f, err := os.Create(outputFile)
	if err != nil {
		die("unable to create %v: %v", outputFile, err)
	}

	c := exec.Command("sha256sum", filenames...)
	c.Stdout = f
	c.Stderr = os.Stderr
	c.Dir = inputDir

	err = c.Run()
	if err != nil {
		die("error running sha256sums: %v", err)
	}

	err = f.Close()
	if err != nil {
		die("close %v: %v", outputFile, err)
	}
}

func signFiles(filenames ...string) {
	for _, filename := range filenames {
		run("gpg", "--armor", "--detach-sign", filename)
	}
}

func updateDocker(sourceDir, version string) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	builderName := fmt.Sprintf("vaultic-release-builder-%d", r.Int())
	run("docker", "buildx", "create", "--name", builderName, "--driver", "docker-container", "--bootstrap")

	buildCmd := fmt.Sprintf(
		"docker buildx build --builder %s --platform linux/386,linux/amd64,linux/arm,linux/arm64 --pull -f docker/Dockerfile.release %q",
		builderName,
		sourceDir,
	)
	run("sh", "-c", buildCmd+" --no-cache")

	var publishCmds strings.Builder
	for _, tag := range []string{"otuschhoff/vaultic:latest", "otuschhoff/vaultic:" + version} {
		publishCmds.WriteString(buildCmd + fmt.Sprintf(" --tag %q --push\n", tag))
	}
	return publishCmds.String() + "\ndocker buildx rm " + builderName
}

func tempdir(prefix string) string {
	dir, err := os.MkdirTemp(getwd(), prefix)
	if err != nil {
		die("unable to create temp dir %q: %v", prefix, err)
	}
	return dir
}

func main() {
	if len(pflag.Args()) == 0 {
		die("USAGE: release-version [OPTIONS] VERSION")
	}

	options.Version = pflag.Args()[0]
	if !versionRegex.MatchString(options.Version) {
		die("invalid new version")
	}

	preCheckBranchMaster()
	branch := getBranchName()
	preCheckUncommittedChanges()
	preCheckVersionExists()
	preCheckDockerBuilderGoVersion()
	if !preCheckChangelogRelease() {
		createChangelogRelease()
	}
	preCheckChangelogCurrent()
	preCheckChangelogVersion()

	if options.OutputDir == "" {
		options.OutputDir = tempdir("build-output-")
	}
	sourceDir := tempdir("source-")

	msg("using output dir %v", options.OutputDir)
	msg("using source dir %v", sourceDir)

	generateFiles()
	updateVersion()
	addTag()
	updateVersionDev()

	tarFilename := filepath.Join(options.OutputDir, fmt.Sprintf("vaultic-%s.tar.gz", options.Version))
	exportTar(options.Version, tarFilename)

	extractTar(tarFilename, sourceDir)
	runBuild(sourceDir, options.OutputDir, options.Version)

	sha256sums(options.OutputDir, filepath.Join(options.OutputDir, "SHA256SUMS"))

	signFiles(filepath.Join(options.OutputDir, "SHA256SUMS"), tarFilename)

	dockerCmds := updateDocker(sourceDir, options.Version)

	msg("done, output dir is %v", options.OutputDir)

	msg("now run:\n\ngit push --tags origin %s\n%s\n\nrm -rf %q", branch, dockerCmds, sourceDir)
}
