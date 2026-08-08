package fssecure

import (
	"errors"
	"fmt"
	"os"
	"runtime"
)

// EnsurePrivateDir creates a sensitive directory and removes group/other
// access on POSIX even when the directory already existed.
func EnsurePrivateDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	return os.Chmod(path, 0o700)
}

// EnsurePrivateParentDir creates a missing parent directory privately, but
// never changes permissions on an existing caller-selected directory. This is
// safe for paths whose parent may be a shared system directory such as /tmp.
func EnsurePrivateParentDir(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("path is not a directory: %s", path)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return EnsurePrivateDir(path)
}

// OpenPrivateFile opens a sensitive file and removes group/other access on
// POSIX even when an existing file had broader permissions.
func OpenPrivateFile(path string, flags int) (*os.File, error) {
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	if runtime.GOOS != "windows" {
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return file, nil
}

// RestrictExistingFile removes group/other access from a file when it exists.
func RestrictExistingFile(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if err := os.Chmod(path, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
