package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const installStateFilename = "install.json"

type InstallState struct {
	SourcePath  string `json:"source_path,omitempty"`
	BinaryPath  string `json:"binary_path,omitempty"`
	InstallDir  string `json:"install_dir,omitempty"`
	InstalledAt string `json:"installed_at,omitempty"`
	PathUpdated bool   `json:"path_updated,omitempty"`
	Environment string `json:"environment,omitempty"`
	CommandName string `json:"command_name,omitempty"`
}

type InstallOptions struct {
	InstallDir string
	Force      bool
}

type InstallResult struct {
	SourcePath  string
	BinaryPath  string
	InstallDir  string
	PathUpdated bool
	Noop        bool
}

func runInstall(args []string) error {
	return runInstallWithWriter(args, os.Stdout)
}

func runInstallWithWriter(args []string, w io.Writer) error {
	flags := flag.NewFlagSet(appCommandName, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	dir := flags.String("dir", "", "Install directory (defaults to ~/.local/bin)")
	force := flags.Bool("force", false, "Overwrite an existing binary")
	if err := flags.Parse(args); err != nil {
		return err
	}

	result, err := InstallRhizomeBot(InstallOptions{
		InstallDir: strings.TrimSpace(*dir),
		Force:      *force,
	})
	if err != nil {
		return err
	}

	if result.Noop {
		fmt.Fprintf(w, "%s already installed at %s\n", appCommandName, result.BinaryPath)
	} else {
		fmt.Fprintf(w, "installed %s to %s\n", appCommandName, result.BinaryPath)
	}
	if result.PathUpdated {
		fmt.Fprintln(w, "updated PATH for the current user")
	}
	fmt.Fprintf(w, "install dir: %s\n", result.InstallDir)
	fmt.Fprintf(w, "source: %s\n", result.SourcePath)
	return nil
}

func InstallRhizomeBot(opts InstallOptions) (InstallResult, error) {
	installDir := firstNonEmpty(strings.TrimSpace(opts.InstallDir), defaultInstallDir())
	if installDir == "" {
		return InstallResult{}, fmt.Errorf("could not resolve install directory")
	}

	sourcePath, err := os.Executable()
	if err != nil {
		return InstallResult{}, fmt.Errorf("resolve executable: %w", err)
	}
	sourcePath, _ = filepath.Abs(sourcePath)
	installDir, _ = filepath.Abs(installDir)
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		return InstallResult{}, fmt.Errorf("create install dir: %w", err)
	}

	binaryName := appCommandName
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binaryPath := filepath.Join(installDir, binaryName)

	noop := false
	if samePath(sourcePath, binaryPath) {
		noop = true
	} else {
		if sameFileExists(binaryPath) {
			if !opts.Force {
				return InstallResult{}, fmt.Errorf("binary already exists at %s; rerun with --force to replace it", binaryPath)
			}
			if err := os.Remove(binaryPath); err != nil {
				return InstallResult{}, fmt.Errorf("remove existing binary: %w", err)
			}
		}
		if err := copyExecutable(sourcePath, binaryPath); err != nil {
			return InstallResult{}, err
		}
	}

	pathUpdated, err := ensureInstallDirInPath(installDir)
	if err != nil {
		return InstallResult{}, err
	}
	if err := persistInstallState(InstallState{
		SourcePath:  sourcePath,
		BinaryPath:  binaryPath,
		InstallDir:  installDir,
		InstalledAt: time.Now().UTC().Format(time.RFC3339Nano),
		PathUpdated: pathUpdated,
		Environment: runtime.GOOS,
		CommandName: appCommandName,
	}); err != nil {
		return InstallResult{}, err
	}

	return InstallResult{
		SourcePath:  sourcePath,
		BinaryPath:  binaryPath,
		InstallDir:  installDir,
		PathUpdated: pathUpdated,
		Noop:        noop,
	}, nil
}

func defaultInstallDir() string {
	home, _ := os.UserHomeDir()
	if home == "" {
		return ""
	}
	return filepath.Join(home, ".local", "bin")
}

func installStatePath() string {
	return agentRuntimeConfigPath(installStateFilename)
}

func persistInstallState(state InstallState) error {
	path := installStatePath()
	if path == "" {
		return nil
	}
	return writeManagerStateJSON("persist_install_state", path, state)
}

func copyExecutable(sourcePath, targetPath string) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}
	in, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open source executable: %w", err)
	}
	defer in.Close()

	tmpPath := targetPath + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return fmt.Errorf("create target executable: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("copy executable: %w", err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Chmod(tmpPath, 0o755); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func samePath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	a = filepath.Clean(a)
	b = filepath.Clean(b)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

func sameFileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
