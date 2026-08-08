//go:build windows

package main

import (
	"os"
	"testing"
)

func TestShouldSkipUserPathMutationHonorsEnvOverride(t *testing.T) {
	previous, hadPrevious := os.LookupEnv("RHIZOME_BOT_INSTALL_SKIP_USER_PATH")
	if err := os.Setenv("RHIZOME_BOT_INSTALL_SKIP_USER_PATH", "1"); err != nil {
		t.Fatalf("Setenv() error: %v", err)
	}
	t.Cleanup(func() {
		if !hadPrevious {
			_ = os.Unsetenv("RHIZOME_BOT_INSTALL_SKIP_USER_PATH")
			return
		}
		_ = os.Setenv("RHIZOME_BOT_INSTALL_SKIP_USER_PATH", previous)
	})
	if !shouldSkipUserPathMutation() {
		t.Fatal("expected shouldSkipUserPathMutation() to honor explicit env override")
	}
}
