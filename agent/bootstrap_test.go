package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBootstrapCreatesWorkspaceFilesAndPreservesExistingFiles(t *testing.T) {
	workdir := t.TempDir()
	soulPath := filepath.Join(workdir, "SOUL.md")

	if err := os.WriteFile(soulPath, []byte("custom soul"), 0o644); err != nil {
		t.Fatalf("seed SOUL.md: %v", err)
	}

	if err := Bootstrap(workdir); err != nil {
		t.Fatalf("Bootstrap() error = %v", err)
	}

	gotSoul, err := os.ReadFile(soulPath)
	if err != nil {
		t.Fatalf("read SOUL.md: %v", err)
	}
	if string(gotSoul) != "custom soul" {
		t.Fatalf("SOUL.md was overwritten: %q", string(gotSoul))
	}

	for _, name := range []string{"HEARTBEAT.md", "MEMORY.md"} {
		path := filepath.Join(workdir, name)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", name, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(data), "{{DATE}}") {
			t.Fatalf("%s still contains template placeholder", name)
		}
	}

	memData, err := os.ReadFile(filepath.Join(workdir, "MEMORY.md"))
	if err != nil {
		t.Fatalf("read MEMORY.md: %v", err)
	}
	if !strings.Contains(string(memData), time.Now().Format("2006-01-02")) {
		t.Fatalf("MEMORY.md does not contain today's date: %q", string(memData))
	}

	if fi, err := os.Stat(filepath.Join(workdir, "memory")); err != nil || !fi.IsDir() {
		t.Fatalf("expected memory/ directory to exist, got fi=%v err=%v", fi, err)
	}
}

func TestLoadMemoryContextIncludesRecentDailyNotes(t *testing.T) {
	workdir := t.TempDir()

	if err := os.WriteFile(filepath.Join(workdir, "MEMORY.md"), []byte("memory anchor"), 0o644); err != nil {
		t.Fatalf("seed MEMORY.md: %v", err)
	}

	now := time.Now()
	notes := []struct {
		offsetDays int
		content    string
	}{
		{0, "today note"},
		{-1, "yesterday note"},
		{-2, "two days ago note"},
		{-3, "old note should be ignored"},
	}

	for _, note := range notes {
		day := now.AddDate(0, 0, note.offsetDays)
		path := filepath.Join(workdir, "memory", day.Format("200601"), day.Format("20060102")+".md")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir note dir: %v", err)
		}
		if err := os.WriteFile(path, []byte(note.content), 0o644); err != nil {
			t.Fatalf("write note: %v", err)
		}
	}

	got := LoadMemoryContext(workdir)

	for _, want := range []string{"memory anchor", "today note", "yesterday note", "two days ago note"} {
		if !strings.Contains(got, want) {
			t.Fatalf("LoadMemoryContext() missing %q in %q", want, got)
		}
	}
	if strings.Contains(got, "old note should be ignored") {
		t.Fatalf("LoadMemoryContext() included a note older than the 3-day window: %q", got)
	}
}

func TestLoadMemoryContextUsesDeterministicRecentEpisodeTail(t *testing.T) {
	workdir := t.TempDir()

	if err := os.WriteFile(filepath.Join(workdir, "MEMORY.md"), []byte("memory anchor"), 0o644); err != nil {
		t.Fatalf("seed MEMORY.md: %v", err)
	}

	now := time.Now()
	notes := []struct {
		offsetDays int
		content    string
	}{
		{0, "today episode"},
		{-1, "yesterday episode"},
		{-2, "two days episode"},
		{-3, "old episode"},
	}

	for _, note := range notes {
		day := now.AddDate(0, 0, note.offsetDays)
		path := filepath.Join(workdir, "memory", day.Format("200601"), day.Format("20060102")+".md")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir note dir: %v", err)
		}
		if err := os.WriteFile(path, []byte(note.content), 0o644); err != nil {
			t.Fatalf("write note: %v", err)
		}
	}

	first := LoadMemoryContext(workdir)
	second := LoadMemoryContext(workdir)
	if first != second {
		t.Fatalf("expected deterministic LoadMemoryContext output, got:\nfirst:\n%s\nsecond:\n%s", first, second)
	}

	for _, want := range []string{"memory anchor", "today episode", "yesterday episode", "two days episode"} {
		if !strings.Contains(first, want) {
			t.Fatalf("LoadMemoryContext() missing %q in %q", want, first)
		}
	}
	if strings.Contains(first, "old episode") {
		t.Fatalf("LoadMemoryContext() included an episode older than the 3-day window: %q", first)
	}

	todayIdx := strings.Index(first, "today episode")
	yesterdayIdx := strings.Index(first, "yesterday episode")
	twoDaysIdx := strings.Index(first, "two days episode")
	if todayIdx < 0 || yesterdayIdx < 0 || twoDaysIdx < 0 {
		t.Fatalf("expected all recent episodes to be present, got:\n%s", first)
	}
	if !(todayIdx < yesterdayIdx && yesterdayIdx < twoDaysIdx) {
		t.Fatalf("expected deterministic newest-to-oldest episodic ordering, got:\n%s", first)
	}
}
