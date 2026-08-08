package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/fssecure"
)

const (
	backupManifestPath = "manifest.json"
	backupDBMainPath   = "db/main.db"
	backupDBWALPath    = "db/main.db-wal"
	backupDBSHMPath    = "db/main.db-shm"
	backupMetricsPath  = "metrics/metrics.jsonl"
	backupWorkspaceDir = "workspace/"
	backupVersion      = "rhizome-backup-v1"
)

type backupManifest struct {
	Version           string           `json:"version"`
	CreatedAt         string           `json:"created_at"`
	WorkspaceRoot     string           `json:"workspace_root"`
	DBPath            string           `json:"db_path"`
	MetricsPath       string           `json:"metrics_path"`
	IncludesWorkspace bool             `json:"includes_workspace"`
	IncludesMetrics   bool             `json:"includes_metrics"`
	Files             []backupFileMeta `json:"files"`
}

type backupFileMeta struct {
	ArchivePath string `json:"archive_path"`
	SizeBytes   int64  `json:"size_bytes"`
}

func runBackup(args []string) error {
	if len(args) < 1 {
		printBackupUsage(os.Stderr)
		return errors.New("missing backup subcommand")
	}

	switch args[0] {
	case "create":
		return runBackupCreate(args[1:])
	case "restore":
		return runBackupRestore(args[1:])
	case "verify":
		return runBackupVerify(args[1:])
	case "verify-restore":
		return runBackupVerifyRestore(args[1:])
	default:
		printBackupUsage(os.Stderr)
		return fmt.Errorf("unknown backup subcommand: %s", args[0])
	}
}

func runBackupCreate(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("backup create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	output := fs.String("output", "", "Path to backup zip file")
	includeWorkspace := fs.Bool("include-workspace", true, "Include workspace files")
	includeMetrics := fs.Bool("include-metrics", true, "Include runtime metrics JSONL")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := app.LoadConfig()
	dbPath := strings.TrimSpace(cfg.DBPath)
	if dbPath == "" {
		return errors.New("RHIZOME_DB is empty")
	}
	if _, err := os.Stat(dbPath); err != nil {
		return fmt.Errorf("db file not found for backup: %s", dbPath)
	}

	targetOutput := strings.TrimSpace(*output)
	if targetOutput == "" {
		targetOutput = filepath.Join(
			"data",
			"backups",
			fmt.Sprintf("rhizome-backup-%s.zip", time.Now().UTC().Format("20060102T150405Z")),
		)
	}
	if err := ensurePrivateParentDir(targetOutput); err != nil {
		return fmt.Errorf("create backup output directory: %w", err)
	}

	file, err := fssecure.OpenPrivateFile(targetOutput, os.O_CREATE|os.O_TRUNC|os.O_WRONLY)
	if err != nil {
		return fmt.Errorf("create backup archive: %w", err)
	}
	defer func() { _ = file.Close() }()

	writer := zip.NewWriter(file)
	defer func() { _ = writer.Close() }()

	manifest := backupManifest{
		Version:           backupVersion,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		WorkspaceRoot:     cfg.WorkspaceRoot,
		DBPath:            cfg.DBPath,
		MetricsPath:       cfg.MetricsPath,
		IncludesWorkspace: *includeWorkspace,
		IncludesMetrics:   *includeMetrics,
		Files:             []backupFileMeta{},
	}

	archiveEntries := [][2]string{
		{backupDBMainPath, dbPath},
		{backupDBWALPath, dbPath + "-wal"},
		{backupDBSHMPath, dbPath + "-shm"},
	}
	for _, entry := range archiveEntries {
		if err := addExistingFileToZip(writer, &manifest, entry[0], entry[1]); err != nil {
			return err
		}
	}

	if *includeMetrics {
		if err := addExistingFileToZip(writer, &manifest, backupMetricsPath, cfg.MetricsPath); err != nil {
			return err
		}
	}

	if *includeWorkspace {
		files, err := listFilesUnderRoot(cfg.WorkspaceRoot)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("list workspace files: %w", err)
			}
		}
		for _, filePath := range files {
			rel, err := filepath.Rel(cfg.WorkspaceRoot, filePath)
			if err != nil {
				return fmt.Errorf("build workspace relative path: %w", err)
			}
			archivePath := backupWorkspaceDir + filepath.ToSlash(rel)
			if err := addExistingFileToZip(writer, &manifest, archivePath, filePath); err != nil {
				return err
			}
		}
	}

	manifestRaw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal backup manifest: %w", err)
	}
	if err := addBytesToZip(writer, backupManifestPath, manifestRaw); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close backup archive: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"status":   "CREATED",
		"output":   targetOutput,
		"manifest": manifest,
	})
}

func runBackupRestore(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("backup restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	input := fs.String("input", "", "Path to backup zip file")
	force := fs.Bool("force", false, "Overwrite existing DB, metrics, and workspace files")
	if err := fs.Parse(args); err != nil {
		return err
	}

	archivePath := strings.TrimSpace(*input)
	if archivePath == "" {
		return errors.New("--input is required")
	}

	cfg := app.LoadConfig()
	manifest, reader, err := openValidatedBackupArchive(archivePath)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	if err := restoreBackupIntoConfiguredTargets(cfg, reader.File, *force); err != nil {
		return err
	}
	if err := validateRestoredDatabase(); err != nil {
		return err
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"status":   "RESTORED",
		"input":    archivePath,
		"manifest": manifest,
	})
}

func runBackupVerify(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("backup verify", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	input := fs.String("input", "", "Path to backup zip file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	archivePath := strings.TrimSpace(*input)
	if archivePath == "" {
		return errors.New("--input is required")
	}

	manifest, reader, err := openValidatedBackupArchive(archivePath)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"status":   "VERIFIED",
		"input":    archivePath,
		"manifest": manifest,
	})
}

func runBackupVerifyRestore(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("backup verify-restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	input := fs.String("input", "", "Path to backup zip file")
	sandboxRoot := fs.String("sandbox-root", "", "Optional sandbox root for restore verification")
	if err := fs.Parse(args); err != nil {
		return err
	}

	archivePath := strings.TrimSpace(*input)
	if archivePath == "" {
		return errors.New("--input is required")
	}

	manifest, reader, err := openValidatedBackupArchive(archivePath)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	root := strings.TrimSpace(*sandboxRoot)
	cleanup := false
	if root == "" {
		tempRoot, err := os.MkdirTemp("", "rhizome-backup-verify-restore-*")
		if err != nil {
			return fmt.Errorf("create verify-restore sandbox: %w", err)
		}
		root = tempRoot
		cleanup = true
	}
	root = filepath.Clean(root)
	if err := fssecure.EnsurePrivateParentDir(root); err != nil {
		return fmt.Errorf("create sandbox root: %w", err)
	}
	if cleanup {
		defer func() { _ = os.RemoveAll(root) }()
	}

	dataRoot := filepath.Join(root, "data")
	workspaceRoot := filepath.Join(dataRoot, "workspace")
	overrides := map[string]string{
		"RHIZOME_DB":             filepath.Join(dataRoot, "rhizome.db"),
		"RHIZOME_WORKSPACE_ROOT": workspaceRoot,
		"RHIZOME_METRICS_PATH":   filepath.Join(dataRoot, "metrics.jsonl"),
		"RHIZOME_JOURNAL_PATH":   filepath.Join(dataRoot, "execution_journal.jsonl"),
	}

	var doctorOut map[string]any
	if err := withEnvOverrides(overrides, func() error {
		cfg := app.LoadConfig()
		if err := restoreBackupIntoConfiguredTargets(cfg, reader.File, true); err != nil {
			return err
		}
		if err := verifyRestoredTargetsMatchManifest(cfg, manifest); err != nil {
			return err
		}
		if err := validateRestoredDatabase(); err != nil {
			return err
		}

		out, err := captureStdoutForBackup(func() error {
			return runDoctor([]string{"--format", "json"})
		})
		if err != nil {
			return fmt.Errorf("doctor failed after verify-restore: %w", err)
		}
		if err := json.Unmarshal([]byte(out), &doctorOut); err != nil {
			return fmt.Errorf("decode doctor output after verify-restore: %w", err)
		}
		verdict, _ := doctorOut["verdict"].(string)
		if verdict != doctorStatusPass && verdict != "pass" {
			doctorRaw, _ := json.Marshal(doctorOut)
			return fmt.Errorf("doctor verdict after verify-restore is %q: %s", verdict, string(doctorRaw))
		}
		return nil
	}); err != nil {
		return err
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"status":       "VERIFIED_RESTORED",
		"input":        archivePath,
		"sandbox_root": root,
		"manifest":     manifest,
		"doctor":       doctorOut,
	})
}

func printBackupUsage(out *os.File) {
	fmt.Fprintln(out, "Backup commands:")
	fmt.Fprintln(out, "  rhizome backup create [--output path.zip] [--include-workspace=true] [--include-metrics=true]")
	fmt.Fprintln(out, "  rhizome backup restore --input path.zip [--force]")
	fmt.Fprintln(out, "  rhizome backup verify --input path.zip")
	fmt.Fprintln(out, "  rhizome backup verify-restore --input path.zip [--sandbox-root dir]")
}

func addExistingFileToZip(writer *zip.Writer, manifest *backupManifest, archivePath, sourcePath string) error {
	info, err := os.Stat(sourcePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat source file %s: %w", sourcePath, err)
	}
	if info.IsDir() {
		return nil
	}

	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open source file %s: %w", sourcePath, err)
	}
	defer func() { _ = source.Close() }()

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("build zip header for %s: %w", sourcePath, err)
	}
	header.Name = archivePath
	header.Method = zip.Deflate

	target, err := writer.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("create zip entry %s: %w", archivePath, err)
	}
	if _, err := io.Copy(target, source); err != nil {
		return fmt.Errorf("copy file %s into backup: %w", sourcePath, err)
	}

	manifest.Files = append(manifest.Files, backupFileMeta{
		ArchivePath: archivePath,
		SizeBytes:   info.Size(),
	})
	return nil
}

func addBytesToZip(writer *zip.Writer, archivePath string, payload []byte) error {
	entry, err := writer.Create(archivePath)
	if err != nil {
		return fmt.Errorf("create zip entry %s: %w", archivePath, err)
	}
	if _, err := entry.Write(payload); err != nil {
		return fmt.Errorf("write zip entry %s: %w", archivePath, err)
	}
	return nil
}

func listFilesUnderRoot(root string) ([]string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("root is empty")
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("root is not a directory: %s", root)
	}

	files := []string{}
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func readBackupManifest(files []*zip.File) (backupManifest, error) {
	var manifestFile *zip.File
	manifestCount := 0
	for _, file := range files {
		if file.Name != backupManifestPath {
			continue
		}
		manifestCount++
		if manifestCount == 1 {
			manifestFile = file
		}
	}
	if manifestCount == 0 || manifestFile == nil {
		return backupManifest{}, errors.New("backup manifest not found")
	}
	if manifestCount > 1 {
		return backupManifest{}, errors.New("backup manifest appears more than once")
	}

	reader, err := manifestFile.Open()
	if err != nil {
		return backupManifest{}, fmt.Errorf("open backup manifest: %w", err)
	}
	defer func() { _ = reader.Close() }()

	raw, err := io.ReadAll(reader)
	if err != nil {
		return backupManifest{}, fmt.Errorf("read backup manifest: %w", err)
	}
	var manifest backupManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return backupManifest{}, fmt.Errorf("decode backup manifest: %w", err)
	}
	return manifest, nil
}

func validateBackupArchive(files []*zip.File, manifest backupManifest) error {
	expected := make(map[string]int64, len(manifest.Files))
	for _, meta := range manifest.Files {
		name := strings.TrimSpace(meta.ArchivePath)
		if name == "" {
			return errors.New("backup manifest contains empty archive_path")
		}
		if name == backupManifestPath {
			return fmt.Errorf("backup manifest entry must not shadow %s", backupManifestPath)
		}
		if _, exists := expected[name]; exists {
			return fmt.Errorf("backup manifest contains duplicate archive_path: %s", name)
		}
		expected[name] = meta.SizeBytes
	}

	seen := make(map[string]struct{}, len(expected))
	manifestSeen := 0
	for _, file := range files {
		name := strings.TrimSpace(file.Name)
		if name == "" {
			return errors.New("backup archive contains empty entry name")
		}
		if name == backupManifestPath {
			manifestSeen++
			continue
		}
		wantSize, ok := expected[name]
		if !ok {
			return fmt.Errorf("unexpected archive entry: %s", name)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate archive entry: %s", name)
		}
		if int64(file.UncompressedSize64) != wantSize {
			return fmt.Errorf("archive entry size mismatch for %s: got %d want %d", name, file.UncompressedSize64, wantSize)
		}
		seen[name] = struct{}{}
	}

	if manifestSeen != 1 {
		return errors.New("backup archive must contain exactly one manifest.json entry")
	}
	for name := range expected {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("backup archive missing manifest entry: %s", name)
		}
	}
	return nil
}

func openValidatedBackupArchive(archivePath string) (backupManifest, *zip.ReadCloser, error) {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return backupManifest{}, nil, fmt.Errorf("open backup archive: %w", err)
	}
	manifest, err := readBackupManifest(reader.File)
	if err != nil {
		_ = reader.Close()
		return backupManifest{}, nil, err
	}
	if manifest.Version != backupVersion {
		_ = reader.Close()
		return backupManifest{}, nil, fmt.Errorf("unsupported backup manifest version: %s", manifest.Version)
	}
	if err := validateBackupArchive(reader.File, manifest); err != nil {
		_ = reader.Close()
		return backupManifest{}, nil, err
	}
	return manifest, reader, nil
}

func restoreBackupIntoConfiguredTargets(cfg app.Config, files []*zip.File, force bool) error {
	if err := ensureRestoreTargetsSafe(cfg, force); err != nil {
		return err
	}
	if force {
		if err := clearRestoreTargets(cfg); err != nil {
			return err
		}
	}
	if err := ensurePrivateParentDir(cfg.DBPath); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}
	if err := ensurePrivateParentDir(cfg.MetricsPath); err != nil {
		return fmt.Errorf("create metrics directory: %w", err)
	}

	for _, file := range files {
		targetPath, skip, err := mapBackupArchivePath(cfg, file.Name)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		if err := extractZipFile(file, targetPath); err != nil {
			return err
		}
	}
	return nil
}

func validateRestoredDatabase() error {
	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("validate restored database migrations: %w", err)
	}
	return nil
}

func verifyRestoredTargetsMatchManifest(cfg app.Config, manifest backupManifest) error {
	for _, meta := range manifest.Files {
		targetPath, skip, err := mapBackupArchivePath(cfg, meta.ArchivePath)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		info, err := os.Stat(targetPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("restored target missing for %s", meta.ArchivePath)
			}
			return fmt.Errorf("stat restored target for %s: %w", meta.ArchivePath, err)
		}
		if info.Size() != meta.SizeBytes {
			return fmt.Errorf("restored target size mismatch for %s: got %d want %d", meta.ArchivePath, info.Size(), meta.SizeBytes)
		}
	}
	return nil
}

func withEnvOverrides(overrides map[string]string, fn func() error) error {
	previous := make(map[string]*string, len(overrides))
	for key, value := range overrides {
		if existing, ok := os.LookupEnv(key); ok {
			copyValue := existing
			previous[key] = &copyValue
		} else {
			previous[key] = nil
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set %s override: %w", key, err)
		}
	}
	defer func() {
		for key, value := range previous {
			if value == nil {
				_ = os.Unsetenv(key)
				continue
			}
			_ = os.Setenv(key, *value)
		}
	}()
	return fn()
}

func captureStdoutForBackup(fn func() error) (string, error) {
	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		return "", fmt.Errorf("create stdout pipe: %w", err)
	}
	os.Stdout = writer

	done := make(chan struct {
		out string
		err error
	}, 1)
	go func() {
		var buf strings.Builder
		_, readErr := io.Copy(&buf, reader)
		done <- struct {
			out string
			err error
		}{out: buf.String(), err: readErr}
	}()

	runErr := fn()
	_ = writer.Close()
	os.Stdout = oldStdout
	result := <-done
	_ = reader.Close()
	if runErr != nil {
		return "", runErr
	}
	if result.err != nil {
		return "", result.err
	}
	return result.out, nil
}

func ensureRestoreTargetsSafe(cfg app.Config, force bool) error {
	targets := []string{
		cfg.DBPath,
		cfg.DBPath + "-wal",
		cfg.DBPath + "-shm",
		cfg.MetricsPath,
	}
	for _, target := range targets {
		if strings.TrimSpace(target) == "" {
			continue
		}
		if _, err := os.Stat(target); err == nil && !force {
			return fmt.Errorf("restore target already exists: %s (use --force to overwrite)", target)
		}
	}

	if info, err := os.Stat(cfg.WorkspaceRoot); err == nil && info.IsDir() && !force {
		entries, readErr := os.ReadDir(cfg.WorkspaceRoot)
		if readErr != nil {
			return fmt.Errorf("read workspace root before restore: %w", readErr)
		}
		if len(entries) > 0 {
			return fmt.Errorf("workspace root already contains files: %s (use --force to overwrite)", cfg.WorkspaceRoot)
		}
	}
	return nil
}

func clearRestoreTargets(cfg app.Config) error {
	targets := []string{
		cfg.DBPath,
		cfg.DBPath + "-wal",
		cfg.DBPath + "-shm",
		cfg.MetricsPath,
	}
	for _, target := range targets {
		if strings.TrimSpace(target) == "" {
			continue
		}
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove restore target %s: %w", target, err)
		}
	}
	if strings.TrimSpace(cfg.WorkspaceRoot) != "" {
		if err := os.RemoveAll(cfg.WorkspaceRoot); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove workspace root %s: %w", cfg.WorkspaceRoot, err)
		}
	}
	return nil
}

func mapBackupArchivePath(cfg app.Config, archivePath string) (string, bool, error) {
	switch archivePath {
	case backupManifestPath:
		return "", true, nil
	case backupDBMainPath:
		return cfg.DBPath, false, nil
	case backupDBWALPath:
		return cfg.DBPath + "-wal", false, nil
	case backupDBSHMPath:
		return cfg.DBPath + "-shm", false, nil
	case backupMetricsPath:
		return cfg.MetricsPath, false, nil
	}

	if strings.HasPrefix(archivePath, backupWorkspaceDir) {
		rel := strings.TrimPrefix(archivePath, backupWorkspaceDir)
		rel = filepath.Clean(filepath.FromSlash(rel))
		if rel == "." || rel == "" {
			return "", true, nil
		}
		if strings.HasPrefix(rel, "..") {
			return "", false, fmt.Errorf("unsafe workspace archive path: %s", archivePath)
		}
		return filepath.Join(cfg.WorkspaceRoot, rel), false, nil
	}
	return "", false, fmt.Errorf("unexpected archive entry: %s", archivePath)
}

func extractZipFile(file *zip.File, targetPath string) error {
	if err := ensurePrivateParentDir(targetPath); err != nil {
		return fmt.Errorf("create restore parent dir for %s: %w", targetPath, err)
	}

	reader, err := file.Open()
	if err != nil {
		return fmt.Errorf("open zip entry %s: %w", file.Name, err)
	}
	defer func() { _ = reader.Close() }()

	out, err := fssecure.OpenPrivateFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY)
	if err != nil {
		return fmt.Errorf("create restore target %s: %w", targetPath, err)
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, reader); err != nil {
		return fmt.Errorf("extract zip entry %s: %w", file.Name, err)
	}
	return nil
}

func ensurePrivateParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return fssecure.EnsurePrivateParentDir(dir)
}
