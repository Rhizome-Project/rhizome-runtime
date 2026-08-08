package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"
)

// RuntimeBuildInfo captures non-secret build/runtime identity for a running binary.
type RuntimeBuildInfo struct {
	BinaryPath       string `json:"binary_path,omitempty"`
	BinarySHA256     string `json:"binary_sha256,omitempty"`
	BinaryModTime    string `json:"binary_mod_time,omitempty"`
	BinarySizeBytes  int64  `json:"binary_size_bytes,omitempty"`
	WorkingDirectory string `json:"working_directory,omitempty"`
	RepoRoot         string `json:"repo_root,omitempty"`
	GoVersion        string `json:"go_version,omitempty"`
	VCSRevision      string `json:"vcs_revision,omitempty"`
	VCSTime          string `json:"vcs_time,omitempty"`
	VCSModified      bool   `json:"vcs_modified,omitempty"`
}

// GitCheckoutInfo captures the current git checkout state for a working tree.
type GitCheckoutInfo struct {
	RepoRoot string `json:"repo_root,omitempty"`
	Branch   string `json:"branch,omitempty"`
	Head     string `json:"head,omitempty"`
	Dirty    bool   `json:"dirty,omitempty"`
	Error    string `json:"error,omitempty"`
}

// CurrentRuntimeBuildInfo reads the running binary's identity and embedded VCS metadata.
func CurrentRuntimeBuildInfo() RuntimeBuildInfo {
	info := RuntimeBuildInfo{}

	if exe, err := os.Executable(); err == nil {
		info.BinaryPath = filepath.Clean(exe)
		if stat, statErr := os.Stat(info.BinaryPath); statErr == nil {
			info.BinaryModTime = stat.ModTime().UTC().Format(time.RFC3339Nano)
			info.BinarySizeBytes = stat.Size()
		}
		if digest, digestErr := fileSHA256(info.BinaryPath); digestErr == nil {
			info.BinarySHA256 = digest
		}
	}
	if cwd, err := os.Getwd(); err == nil {
		info.WorkingDirectory = filepath.Clean(cwd)
	}
	if info.WorkingDirectory != "" {
		if root, err := FindGitRepoRoot(info.WorkingDirectory); err == nil {
			info.RepoRoot = root
		}
	}
	if info.RepoRoot == "" && info.BinaryPath != "" {
		if root, err := FindGitRepoRoot(filepath.Dir(info.BinaryPath)); err == nil {
			info.RepoRoot = root
		}
	}

	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		info.GoVersion = buildInfo.GoVersion
		ApplyBuildSettings(&info, buildInfo.Settings)
	}

	return info
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ApplyBuildSettings maps Go build settings into RuntimeBuildInfo.
func ApplyBuildSettings(info *RuntimeBuildInfo, settings []debug.BuildSetting) {
	if info == nil {
		return
	}
	for _, setting := range settings {
		switch strings.TrimSpace(setting.Key) {
		case "vcs.revision":
			info.VCSRevision = strings.TrimSpace(setting.Value)
		case "vcs.time":
			info.VCSTime = strings.TrimSpace(setting.Value)
		case "vcs.modified":
			info.VCSModified = strings.EqualFold(strings.TrimSpace(setting.Value), "true")
		}
	}
}

// CurrentGitCheckoutInfo reads git checkout metadata from the current working tree.
func CurrentGitCheckoutInfo() GitCheckoutInfo {
	startDir, err := os.Getwd()
	if err != nil {
		return GitCheckoutInfo{Error: err.Error()}
	}
	return GitCheckoutInfoFromDir(startDir)
}

// GitCheckoutInfoFromDir reads git checkout metadata starting from the given directory.
func GitCheckoutInfoFromDir(startDir string) GitCheckoutInfo {
	root, err := FindGitRepoRoot(startDir)
	if err != nil {
		return GitCheckoutInfo{Error: err.Error()}
	}

	info := GitCheckoutInfo{RepoRoot: root}
	head, err := runGit(root, "rev-parse", "HEAD")
	if err != nil {
		info.Error = err.Error()
		return info
	}
	branch, err := runGit(root, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		info.Error = err.Error()
		return info
	}
	status, err := runGit(root, "status", "--porcelain")
	if err != nil {
		info.Error = err.Error()
		return info
	}

	info.Head = strings.TrimSpace(head)
	info.Branch = strings.TrimSpace(branch)
	info.Dirty = strings.TrimSpace(status) != ""
	return info
}

// FindGitRepoRoot walks upward until it finds a .git entry.
func FindGitRepoRoot(startDir string) (string, error) {
	startDir = strings.TrimSpace(startDir)
	if startDir == "" {
		return "", errors.New("start directory is empty")
	}

	current := filepath.Clean(startDir)
	for {
		gitPath := filepath.Join(current, ".git")
		if _, err := os.Stat(gitPath); err == nil {
			return current, nil
		}

		next := filepath.Dir(current)
		if next == current {
			break
		}
		current = next
	}
	return "", errors.New("git repo root not found")
}

func runGit(repoRoot string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdArgs := append([]string{"-C", repoRoot}, args...)
	cmd := exec.CommandContext(ctx, "git", cmdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", errors.New(strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}
