//go:build windows

package main

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

func replaceFileAtomic(src, dst string) error {
	srcPtr, err := windows.UTF16PtrFromString(src)
	if err != nil {
		return fmt.Errorf("encode source path: %w", err)
	}
	dstPtr, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return fmt.Errorf("encode destination path: %w", err)
	}
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if err := windows.MoveFileEx(srcPtr, dstPtr, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
			lastErr = err
			if errno, ok := err.(windows.Errno); ok && (errno == windows.ERROR_ACCESS_DENIED || errno == windows.ERROR_SHARING_VIOLATION) {
				time.Sleep(time.Duration(attempt+1) * 25 * time.Millisecond)
				continue
			}
			return fmt.Errorf("replace file atomically: %w", err)
		}
		return nil
	}
	if lastErr != nil {
		if data, readErr := os.ReadFile(src); readErr == nil {
			if writeErr := os.WriteFile(dst, data, 0o644); writeErr == nil {
				_ = os.Remove(src)
				return nil
			}
		}
		return fmt.Errorf("replace file atomically: %w", lastErr)
	}
	return nil
}
