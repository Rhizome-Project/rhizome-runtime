package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func writeBackupArchiveForTest(t *testing.T, path string, manifest backupManifest, entries map[string]string, duplicateEntries []string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir backup archive dir: %v", err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create backup archive: %v", err)
	}
	defer func() { _ = file.Close() }()

	writer := zip.NewWriter(file)
	for name, payload := range entries {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatalf("create zip entry %s: %v", name, err)
		}
		if _, err := entry.Write([]byte(payload)); err != nil {
			t.Fatalf("write zip entry %s: %v", name, err)
		}
	}
	for _, name := range duplicateEntries {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatalf("create duplicate zip entry %s: %v", name, err)
		}
		if _, err := entry.Write([]byte("duplicate")); err != nil {
			t.Fatalf("write duplicate zip entry %s: %v", name, err)
		}
	}

	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	entry, err := writer.Create(backupManifestPath)
	if err != nil {
		t.Fatalf("create manifest entry: %v", err)
	}
	if _, err := entry.Write(manifestRaw); err != nil {
		t.Fatalf("write manifest entry: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close archive writer: %v", err)
	}
}

func setBackupPathsForTest(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	t.Setenv("RHIZOME_DB", filepath.Join(root, "data", "rhizome.db"))
	t.Setenv("RHIZOME_WORKSPACE_ROOT", filepath.Join(root, "data", "workspace"))
	t.Setenv("RHIZOME_METRICS_PATH", filepath.Join(root, "data", "metrics.jsonl"))
}

func TestRunBackupRestoreRejectsUnexpectedArchiveEntry(t *testing.T) {
	setBackupPathsForTest(t)
	backupPath := filepath.Join(t.TempDir(), "unexpected.zip")
	manifest := backupManifest{
		Version: backupVersion,
		Files: []backupFileMeta{
			{ArchivePath: backupDBMainPath, SizeBytes: int64(len("db"))},
		},
	}
	writeBackupArchiveForTest(t, backupPath, manifest, map[string]string{
		backupDBMainPath:           "db",
		"workspace/unexpected.txt": "oops",
	}, nil)

	err := runBackupRestore([]string{"--input", backupPath})
	if err == nil || !strings.Contains(err.Error(), "unexpected archive entry") {
		t.Fatalf("expected unexpected archive entry error, got %v", err)
	}
}

func TestRunBackupRestoreRejectsMissingManifestEntry(t *testing.T) {
	setBackupPathsForTest(t)
	backupPath := filepath.Join(t.TempDir(), "missing.zip")
	manifest := backupManifest{
		Version: backupVersion,
		Files: []backupFileMeta{
			{ArchivePath: backupDBMainPath, SizeBytes: int64(len("db"))},
			{ArchivePath: backupMetricsPath, SizeBytes: int64(len("metrics"))},
		},
	}
	writeBackupArchiveForTest(t, backupPath, manifest, map[string]string{
		backupDBMainPath: "db",
	}, nil)

	err := runBackupRestore([]string{"--input", backupPath})
	if err == nil || !strings.Contains(err.Error(), "backup archive missing manifest entry") {
		t.Fatalf("expected missing manifest entry error, got %v", err)
	}
}

func TestRunBackupRestoreRejectsDuplicateArchiveEntry(t *testing.T) {
	setBackupPathsForTest(t)
	backupPath := filepath.Join(t.TempDir(), "duplicate.zip")
	manifest := backupManifest{
		Version: backupVersion,
		Files: []backupFileMeta{
			{ArchivePath: backupDBMainPath, SizeBytes: int64(len("db"))},
		},
	}
	writeBackupArchiveForTest(t, backupPath, manifest, map[string]string{
		backupDBMainPath: "db",
	}, []string{backupDBMainPath})

	err := runBackupRestore([]string{"--input", backupPath})
	if err == nil || !strings.Contains(err.Error(), "duplicate archive entry") {
		t.Fatalf("expected duplicate archive entry error, got %v", err)
	}
}

func TestRunBackupVerifySucceedsOnValidManifestArchive(t *testing.T) {
	backupPath := filepath.Join(t.TempDir(), "valid.zip")
	manifest := backupManifest{
		Version: backupVersion,
		Files: []backupFileMeta{
			{ArchivePath: backupDBMainPath, SizeBytes: int64(len("db"))},
			{ArchivePath: backupMetricsPath, SizeBytes: int64(len("metrics"))},
		},
	}
	writeBackupArchiveForTest(t, backupPath, manifest, map[string]string{
		backupDBMainPath:  "db",
		backupMetricsPath: "metrics",
	}, nil)

	var stdout bytes.Buffer
	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r)
		done <- buf.String()
	}()
	err = runBackupVerify([]string{"--input", backupPath})
	_ = w.Close()
	os.Stdout = origStdout
	stdout.WriteString(<-done)
	_ = r.Close()
	if err != nil {
		t.Fatalf("runBackupVerify failed: %v", err)
	}
	if !strings.Contains(stdout.String(), `"status": "VERIFIED"`) {
		t.Fatalf("expected VERIFIED output, got %q", stdout.String())
	}
}

func TestRunBackupVerifyRejectsDuplicateManifestEntry(t *testing.T) {
	backupPath := filepath.Join(t.TempDir(), "duplicate-manifest.zip")
	manifest := backupManifest{
		Version: backupVersion,
		Files: []backupFileMeta{
			{ArchivePath: backupDBMainPath, SizeBytes: int64(len("db"))},
		},
	}
	writeBackupArchiveForTest(t, backupPath, manifest, map[string]string{
		backupDBMainPath: "db",
	}, []string{backupManifestPath})

	err := runBackupVerify([]string{"--input", backupPath})
	if err == nil || !strings.Contains(err.Error(), "backup manifest appears more than once") {
		t.Fatalf("expected duplicate manifest error, got %v", err)
	}
}

func TestRunBackupVerifyRestoreRestoresIntoSandboxWithoutTouchingConfiguredTargets(t *testing.T) {
	setBackupPathsForTest(t)

	liveCfg := app.LoadConfig()
	liveDataRoot := filepath.Dir(liveCfg.DBPath)
	if err := os.MkdirAll(liveCfg.WorkspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir live workspace root: %v", err)
	}
	if err := os.MkdirAll(liveDataRoot, 0o755); err != nil {
		t.Fatalf("mkdir live data root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(liveCfg.WorkspaceRoot, "live.txt"), []byte("live-state"), 0o644); err != nil {
		t.Fatalf("write live workspace marker: %v", err)
	}

	sourceRoot := filepath.Join(t.TempDir(), "source")
	sourceDBPath := filepath.Join(sourceRoot, "data", "rhizome.db")
	if err := os.MkdirAll(filepath.Dir(sourceDBPath), 0o755); err != nil {
		t.Fatalf("mkdir source data root: %v", err)
	}
	store, err := sqlite.NewStore(sourceDBPath)
	if err != nil {
		t.Fatalf("create source sqlite store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source sqlite store: %v", err)
	}
	sourceDBRaw, err := os.ReadFile(sourceDBPath)
	if err != nil {
		t.Fatalf("read source sqlite db: %v", err)
	}

	metricsSnapshotRaw, err := json.Marshal(runtimeMetricsSnapshot{
		SchemaVersion: "1.0",
		Timestamp:     "2026-04-12T00:00:00Z",
		Profiles: map[string]runtimeProfileMetrics{
			"DEFAULT": {
				TotalRuns:    3,
				SuccessCount: 3,
				FailureCount: 0,
				TimeoutCount: 0,
				FailureRate:  0,
				AvgDuration:  1.25,
				AvgStartupMS: 120,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal metrics snapshot: %v", err)
	}
	metricsRaw := append(metricsSnapshotRaw, '\n')
	artifactRaw := []byte("artifact")
	stateRaw := []byte("{}")
	backupPath := filepath.Join(t.TempDir(), "verify-restore.zip")
	manifest := backupManifest{
		Version: backupVersion,
		Files: []backupFileMeta{
			{ArchivePath: backupDBMainPath, SizeBytes: int64(len(sourceDBRaw))},
			{ArchivePath: backupMetricsPath, SizeBytes: int64(len(metricsRaw))},
			{ArchivePath: "workspace/shared/artifact.txt", SizeBytes: int64(len(artifactRaw))},
			{ArchivePath: "workspace/state/state.json", SizeBytes: int64(len(stateRaw))},
		},
	}
	writeBackupArchiveForTest(t, backupPath, manifest, map[string]string{
		backupDBMainPath:                string(sourceDBRaw),
		backupMetricsPath:               string(metricsRaw),
		"workspace/shared/artifact.txt": string(artifactRaw),
		"workspace/state/state.json":    string(stateRaw),
	}, nil)

	sandboxRoot := filepath.Join(t.TempDir(), "sandbox")
	out, err := captureStdout(t, func() error {
		return runBackupVerifyRestore([]string{"--input", backupPath, "--sandbox-root", sandboxRoot})
	})
	if err != nil {
		t.Fatalf("runBackupVerifyRestore failed: %v", err)
	}
	if !strings.Contains(out, `"status": "VERIFIED_RESTORED"`) {
		t.Fatalf("expected VERIFIED_RESTORED output, got %q", out)
	}

	sandboxArtifact := filepath.Join(sandboxRoot, "data", "workspace", "shared", "artifact.txt")
	raw, err := os.ReadFile(sandboxArtifact)
	if err != nil {
		t.Fatalf("read sandbox artifact: %v", err)
	}
	if string(raw) != "artifact" {
		t.Fatalf("expected sandbox artifact restored, got %q", string(raw))
	}

	liveRaw, err := os.ReadFile(filepath.Join(liveCfg.WorkspaceRoot, "live.txt"))
	if err != nil {
		t.Fatalf("read live workspace marker: %v", err)
	}
	if string(liveRaw) != "live-state" {
		t.Fatalf("expected live workspace marker preserved, got %q", string(liveRaw))
	}
}
