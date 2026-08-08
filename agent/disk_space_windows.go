//go:build windows

package main

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

func diskFreeBytes(path string) (uint64, error) {
	p := filepath.Clean(path)
	var free uint64
	ptr, err := windows.UTF16PtrFromString(p)
	if err != nil {
		return 0, err
	}
	if err := windows.GetDiskFreeSpaceEx(ptr, &free, nil, nil); err != nil {
		return 0, err
	}
	return free, nil
}
