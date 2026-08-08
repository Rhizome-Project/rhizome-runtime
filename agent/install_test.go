package main

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestInstallRhizomeBotRespectsForce(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	installDir := filepath.Join(t.TempDir(), "bin")
	t.Setenv("PATH", installDir)
	if _, err := InstallRhizomeBot(InstallOptions{InstallDir: installDir}); err != nil {
		t.Fatalf("first InstallRhizomeBot() error: %v", err)
	}

	_, err := InstallRhizomeBot(InstallOptions{InstallDir: installDir})
	if err == nil {
		t.Fatal("expected second install without force to fail when binary already exists")
	}

	if _, err := InstallRhizomeBot(InstallOptions{InstallDir: installDir, Force: true}); err != nil {
		t.Fatalf("forced InstallRhizomeBot() error: %v", err)
	}
}

func TestDefaultInstallDirUsesUserLocalBin(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	got := defaultInstallDir()
	want := filepath.Join(home, ".local", "bin")
	if runtime.GOOS == "windows" {
		want = filepath.Join(home, ".local", "bin")
	}
	if got != want {
		t.Fatalf("defaultInstallDir() = %q, want %q", got, want)
	}
}
