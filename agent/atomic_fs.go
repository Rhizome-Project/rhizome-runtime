package main

import (
	"os"
	"path/filepath"
)

// atomicWriteFile safely writes data to a temporary file, syncs it to disk (fsync),
// and then atomically renames it to the target path. This prevents data loss
// or corrupted files during power failures or crash panics.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(dir, "atomic-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()

	// Clean up the temp file if atomicWriteFile fails midway.
	// If it succeeds, os.Rename moves the temp file and this Remove does nothing.
	defer os.Remove(tmpName)

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}

	// Crucial step: flush to disk. Without this, os.Rename could be ordered
	// before the data is actually on persistent storage.
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return err
	}

	if err := tmpFile.Close(); err != nil {
		return err
	}

	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}

	if err := replaceFileAtomic(tmpName, path); err != nil {
		return err
	}
	return nil
}
