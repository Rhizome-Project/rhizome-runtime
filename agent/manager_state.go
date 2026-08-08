package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	managerStateLockFilename  = ".manager_state.lock"
	managerStateTxnFilename   = ".manager_state.txn.json"
	managerStateMatFilename   = ".manager_state.materialize.json"
	managerStateTxnStageReady = "prepared"
)

var (
	managerStateLockPoll       = 50 * time.Millisecond
	managerStateLockTimeout    = 5 * time.Second
	managerStateLockStaleAfter = 5 * time.Minute
	managerStateProcessMu      sync.Mutex
	errManagerStateBusy        = errors.New("manager state is busy")
)

type managerStateLockInfo struct {
	PID       int    `json:"pid,omitempty"`
	StartedAt string `json:"started_at,omitempty"`
}

type managerStateTxn struct {
	Operation  string `json:"operation,omitempty"`
	TargetPath string `json:"target_path,omitempty"`
	TempPath   string `json:"temp_path,omitempty"`
	Stage      string `json:"stage,omitempty"`
	PID        int    `json:"pid,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type managerStateMaterialization struct {
	Operation string                         `json:"operation,omitempty"`
	Entries   []managerStateMaterializeEntry `json:"entries,omitempty"`
	PID       int                            `json:"pid,omitempty"`
	StartedAt string                         `json:"started_at,omitempty"`
	UpdatedAt string                         `json:"updated_at,omitempty"`
}

type managerStateMaterializeEntry struct {
	TargetPath    string `json:"target_path,omitempty"`
	PayloadPath   string `json:"payload_path,omitempty"`
	BackupPath    string `json:"backup_path,omitempty"`
	BackupPerm    uint32 `json:"backup_perm,omitempty"`
	TargetMissing bool   `json:"target_missing,omitempty"`
	Perm          uint32 `json:"perm,omitempty"`
}

func repairManagerStateOnStartup() error {
	root := agentRuntimeConfigRoot()
	if root == "" {
		return nil
	}
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	ownedByCurrentProcess, err := managerStateLockOwnedByCurrentProcess(root)
	if err != nil {
		return err
	}
	if ownedByCurrentProcess {
		return nil
	}
	err = withManagerStateLock(root, false, func() error {
		if err := recoverManagerStateTransactionLocked(root); err != nil {
			return err
		}
		return recoverManagerStateMaterializationLocked(root)
	})
	if errors.Is(err, errManagerStateBusy) {
		return nil
	}
	return err
}

func writeManagerStateJSON(operation, path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeManagerStateBytes(operation, path, raw, 0o600)
}

func writeManagerStateBytes(operation, path string, data []byte, perm os.FileMode) error {
	root := agentRuntimeConfigRoot()
	if root == "" {
		return fmt.Errorf("agent config root is unavailable")
	}
	return withManagerStateLock(root, true, func() error {
		return writeManagerStateBytesLocked(root, path, operation, data, perm)
	})
}

func withManagerStateLock(root string, wait bool, fn func() error) error {
	managerStateProcessMu.Lock()
	defer managerStateProcessMu.Unlock()

	release, acquired, err := acquireManagerStateLock(root, wait)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	defer release()
	if err := recoverManagerStateTransactionLocked(root); err != nil {
		return err
	}
	if err := recoverManagerStateMaterializationLocked(root); err != nil {
		return err
	}
	return fn()
}

func acquireManagerStateLock(root string, wait bool) (func(), bool, error) {
	root = cleanManagedRuntimeRoot(root)
	if root == "" {
		return nil, false, fmt.Errorf("agent config root is unavailable")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, false, err
	}

	deadline := time.Now().Add(managerStateLockTimeout)
	for {
		if err := createManagerStateLock(root); err == nil {
			return func() {
				_ = os.Remove(managerStateLockPath(root))
			}, true, nil
		} else if !os.IsExist(err) {
			return nil, false, err
		}

		stale, staleErr := managerStateLockIsStale(root)
		if staleErr != nil {
			return nil, false, staleErr
		}
		if stale {
			if err := os.Remove(managerStateLockPath(root)); err != nil && !os.IsNotExist(err) {
				return nil, false, err
			}
			continue
		}
		if !wait || time.Now().After(deadline) {
			return nil, false, errManagerStateBusy
		}
		time.Sleep(managerStateLockPoll)
	}
}

func createManagerStateLock(root string) error {
	lock := managerStateLockInfo{
		PID:       os.Getpid(),
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.MarshalIndent(lock, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(managerStateLockPath(root), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		file.Close()
		_ = os.Remove(managerStateLockPath(root))
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		_ = os.Remove(managerStateLockPath(root))
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(managerStateLockPath(root))
		return err
	}
	return nil
}

func managerStateLockIsStale(root string) (bool, error) {
	path := managerStateLockPath(root)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	var lock managerStateLockInfo
	if err := json.Unmarshal(data, &lock); err != nil {
		info, statErr := os.Stat(path)
		if statErr == nil && time.Since(info.ModTime()) > managerStateLockStaleAfter {
			return true, nil
		}
		return false, fmt.Errorf("read manager state lock: %w", err)
	}
	if lock.PID <= 0 {
		return true, nil
	}
	if startedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(lock.StartedAt)); err == nil && time.Since(startedAt) > managerStateLockStaleAfter {
		return true, nil
	}
	ok, err := processExists(lock.PID)
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func managerStateLockOwnedByCurrentProcess(root string) (bool, error) {
	data, err := os.ReadFile(managerStateLockPath(root))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	var lock managerStateLockInfo
	if err := json.Unmarshal(data, &lock); err != nil {
		return false, nil
	}
	return lock.PID > 0 && lock.PID == os.Getpid(), nil
}

func writeManagerStateBytesLocked(root, path, operation string, data []byte, perm os.FileMode) error {
	if !managerStatePathWithinRoot(root, path) {
		return fmt.Errorf("manager state path %q is outside config root %q", path, root)
	}
	tempPath, err := prepareManagerStateTempFile(path, data, perm)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tempPath)
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := saveManagerStateTxnLocked(root, managerStateTxn{
		Operation:  strings.TrimSpace(operation),
		TargetPath: path,
		TempPath:   tempPath,
		Stage:      managerStateTxnStageReady,
		PID:        os.Getpid(),
		StartedAt:  now,
		UpdatedAt:  now,
	}); err != nil {
		return err
	}

	if err := replaceFileAtomic(tempPath, path); err != nil {
		return err
	}
	committed = true
	return clearManagerStateTxnLocked(root)
}

func prepareManagerStateTempFile(path string, data []byte, perm os.FileMode) (string, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	tmpFile, err := os.CreateTemp(dir, "manager-state-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	return tmpName, nil
}

func prepareManagerStatePayloadFile(root, prefix string, data []byte, perm os.FileMode) (string, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	tmpFile, err := os.CreateTemp(root, prefix)
	if err != nil {
		return "", err
	}
	tmpName := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	return tmpName, nil
}

func recoverManagerStateTransactionLocked(root string) error {
	txn, ok, err := loadManagerStateTxnLocked(root)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if txn.Stage != managerStateTxnStageReady {
		if err := archiveBrokenManagerStateTxn(root, txn, "unsupported journal stage"); err != nil {
			return err
		}
		return nil
	}
	if !managerStatePathWithinRoot(root, txn.TargetPath) {
		if err := archiveBrokenManagerStateTxn(root, txn, "journal target escaped config root"); err != nil {
			return err
		}
		return nil
	}
	if !managerStatePathWithinRoot(root, txn.TempPath) {
		if err := archiveBrokenManagerStateTxn(root, txn, "journal temp path escaped config root"); err != nil {
			return err
		}
		return nil
	}
	if strings.TrimSpace(txn.TempPath) == "" || strings.TrimSpace(txn.TargetPath) == "" {
		if err := archiveBrokenManagerStateTxn(root, txn, "journal missing temp or target path"); err != nil {
			return err
		}
		return nil
	}

	tempExists := pathExists(txn.TempPath)
	targetExists := pathExists(txn.TargetPath)
	switch {
	case tempExists:
		if err := os.MkdirAll(filepath.Dir(txn.TargetPath), 0o700); err != nil {
			return err
		}
		if err := replaceFileAtomic(txn.TempPath, txn.TargetPath); err != nil {
			return err
		}
	case !targetExists:
		if err := archiveBrokenManagerStateTxn(root, txn, "journal missing both temp and target"); err != nil {
			return err
		}
		return nil
	}
	return clearManagerStateTxnLocked(root)
}

func loadManagerStateTxnLocked(root string) (managerStateTxn, bool, error) {
	data, err := os.ReadFile(managerStateTxnPath(root))
	if err != nil {
		if os.IsNotExist(err) {
			return managerStateTxn{}, false, nil
		}
		return managerStateTxn{}, false, err
	}
	var txn managerStateTxn
	if err := json.Unmarshal(data, &txn); err != nil {
		if archiveErr := archiveBrokenManagerStateJournalPath(managerStateTxnPath(root), "corrupt manager state journal"); archiveErr != nil {
			return managerStateTxn{}, false, archiveErr
		}
		return managerStateTxn{}, false, nil
	}
	return txn, true, nil
}

func materializeManagerStateEntriesLocked(root, operation string, entries []managerStateMaterializeEntry) error {
	if len(entries) == 0 {
		return nil
	}
	mat := managerStateMaterialization{
		Operation: strings.TrimSpace(operation),
		Entries:   append([]managerStateMaterializeEntry(nil), entries...),
		PID:       os.Getpid(),
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := saveManagerStateMaterializationLocked(root, mat); err != nil {
		return err
	}
	mat, discardable, err := executeManagerStateMaterializationLocked(root, mat)
	if err != nil {
		if discardable {
			if clearErr := clearManagerStateMaterializationLocked(root); clearErr != nil {
				return clearErr
			}
			if cleanupErr := cleanupManagerStateMaterializationFiles(mat); cleanupErr != nil {
				return cleanupErr
			}
		}
		return err
	}
	if err := clearManagerStateMaterializationLocked(root); err != nil {
		return err
	}
	return cleanupManagerStateMaterializationFiles(mat)
}

func recoverManagerStateMaterializationLocked(root string) error {
	mat, ok, err := loadManagerStateMaterializationLocked(root)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	mat, discardable, err := executeManagerStateMaterializationLocked(root, mat)
	if err != nil {
		if !discardable {
			return err
		}
		if archiveErr := archiveBrokenManagerStateMaterialization(root, mat, err.Error()); archiveErr != nil {
			return archiveErr
		}
		return cleanupManagerStateMaterializationFiles(mat)
	}
	if err := clearManagerStateMaterializationLocked(root); err != nil {
		return err
	}
	return cleanupManagerStateMaterializationFiles(mat)
}

func executeManagerStateMaterializationLocked(root string, mat managerStateMaterialization) (managerStateMaterialization, bool, error) {
	mat, err := ensureManagerStateMaterializationBackupsLocked(root, mat)
	if err != nil {
		return mat, true, err
	}
	applied := 0
	for _, entry := range mat.Entries {
		if err := applyManagerStateMaterializationEntry(root, &mat, entry); err != nil {
			if rollbackErr := rollbackManagerStateMaterializationLocked(root, mat, applied); rollbackErr != nil {
				return mat, false, fmt.Errorf("apply manager state materialization entry %q: %w (rollback failed: %v)", entry.TargetPath, err, rollbackErr)
			}
			return mat, true, fmt.Errorf("apply manager state materialization entry %q: %w", entry.TargetPath, err)
		}
		applied++
	}
	return mat, false, nil
}

func ensureManagerStateMaterializationBackupsLocked(root string, mat managerStateMaterialization) (managerStateMaterialization, error) {
	updated := false
	for i := range mat.Entries {
		entry := &mat.Entries[i]
		if !managerStatePathWithinRoot(root, entry.PayloadPath) {
			return mat, fmt.Errorf("manager state payload %q is outside config root %q", entry.PayloadPath, root)
		}
		if !managerStateTargetPathAllowed(root, *entry) {
			return mat, fmt.Errorf("manager state materialization target %q is not allowed", entry.TargetPath)
		}
		if entry.TargetMissing {
			continue
		}
		if strings.TrimSpace(entry.BackupPath) != "" {
			if !managerStatePathWithinRoot(root, entry.BackupPath) {
				return mat, fmt.Errorf("manager state backup %q is outside config root %q", entry.BackupPath, root)
			}
			info, err := os.Stat(entry.BackupPath)
			if err != nil {
				return mat, fmt.Errorf("manager state backup %q is unavailable: %w", entry.BackupPath, err)
			}
			if info.IsDir() {
				return mat, fmt.Errorf("manager state backup %q is a directory", entry.BackupPath)
			}
			continue
		}
		info, err := os.Stat(entry.TargetPath)
		switch {
		case err == nil:
			if info.IsDir() {
				return mat, fmt.Errorf("manager state target %q is a directory", entry.TargetPath)
			}
			data, err := os.ReadFile(entry.TargetPath)
			if err != nil {
				return mat, err
			}
			backupPath, err := prepareManagerStatePayloadFile(root, "mat-backup-", data, info.Mode().Perm())
			if err != nil {
				return mat, err
			}
			entry.BackupPath = backupPath
			entry.BackupPerm = uint32(info.Mode().Perm())
			updated = true
		case os.IsNotExist(err):
			entry.TargetMissing = true
			updated = true
		default:
			return mat, err
		}
	}
	if updated {
		if err := saveManagerStateMaterializationLocked(root, mat); err != nil {
			return mat, err
		}
	}
	return mat, nil
}

func applyManagerStateMaterializationEntry(root string, mat *managerStateMaterialization, entry managerStateMaterializeEntry) error {
	if !managerStatePathWithinRoot(root, entry.PayloadPath) {
		return fmt.Errorf("manager state payload %q is outside config root %q", entry.PayloadPath, root)
	}
	if !managerStateTargetPathAllowed(root, entry) {
		return fmt.Errorf("manager state materialization target %q is not allowed", entry.TargetPath)
	}
	data, err := os.ReadFile(entry.PayloadPath)
	if err != nil {
		return err
	}
	return atomicWriteFile(entry.TargetPath, data, os.FileMode(entry.Perm))
}

func rollbackManagerStateMaterializationLocked(root string, mat managerStateMaterialization, applied int) error {
	if applied <= 0 {
		return nil
	}
	if applied > len(mat.Entries) {
		applied = len(mat.Entries)
	}
	for i := applied - 1; i >= 0; i-- {
		entry := mat.Entries[i]
		if !managerStateTargetPathAllowed(root, entry) {
			return fmt.Errorf("manager state materialization target %q is not allowed during rollback", entry.TargetPath)
		}
		if entry.TargetMissing {
			if err := os.Remove(entry.TargetPath); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		if !managerStatePathWithinRoot(root, entry.BackupPath) {
			return fmt.Errorf("manager state backup %q is outside config root %q", entry.BackupPath, root)
		}
		data, err := os.ReadFile(entry.BackupPath)
		if err != nil {
			return err
		}
		perm := os.FileMode(entry.BackupPerm)
		if perm == 0 {
			perm = 0o600
		}
		if err := atomicWriteFile(entry.TargetPath, data, perm); err != nil {
			return err
		}
	}
	return nil
}

func cleanupManagerStateMaterializationFiles(mat managerStateMaterialization) error {
	for _, entry := range mat.Entries {
		if err := cleanupManagerStateMaterializationPath(entry.PayloadPath); err != nil {
			return err
		}
		if err := cleanupManagerStateMaterializationPath(entry.BackupPath); err != nil {
			return err
		}
		if err := cleanupManagerStateMaterializationPath(localRuntimeMaterializationMarkerPath(entry.TargetPath)); err != nil {
			return err
		}
	}
	return nil
}

func cleanupManagerStateMaterializationPath(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func loadManagerStateMaterializationLocked(root string) (managerStateMaterialization, bool, error) {
	data, err := os.ReadFile(managerStateMaterializationPath(root))
	if err != nil {
		if os.IsNotExist(err) {
			return managerStateMaterialization{}, false, nil
		}
		return managerStateMaterialization{}, false, err
	}
	var mat managerStateMaterialization
	if err := json.Unmarshal(data, &mat); err != nil {
		if archiveErr := archiveBrokenManagerStateJournalPath(managerStateMaterializationPath(root), "corrupt manager state materialization journal"); archiveErr != nil {
			return managerStateMaterialization{}, false, archiveErr
		}
		return managerStateMaterialization{}, false, nil
	}
	return mat, true, nil
}

func saveManagerStateMaterializationLocked(root string, mat managerStateMaterialization) error {
	mat.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(mat, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(managerStateMaterializationPath(root), raw, 0o600)
}

func clearManagerStateMaterializationLocked(root string) error {
	if err := os.Remove(managerStateMaterializationPath(root)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func saveManagerStateTxnLocked(root string, txn managerStateTxn) error {
	txn.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(txn, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(managerStateTxnPath(root), raw, 0o600)
}

func clearManagerStateTxnLocked(root string) error {
	if err := os.Remove(managerStateTxnPath(root)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func archiveBrokenManagerStateTxn(root string, txn managerStateTxn, reason string) error {
	return archiveBrokenManagerStateJournalPath(managerStateTxnPath(root), fmt.Sprintf("%s for operation %q", reason, txn.Operation))
}

func archiveBrokenManagerStateMaterialization(root string, mat managerStateMaterialization, reason string) error {
	return archiveBrokenManagerStateJournalPath(managerStateMaterializationPath(root), fmt.Sprintf("%s for operation %q", reason, mat.Operation))
}

func managerStateLockPath(root string) string {
	return filepath.Join(root, managerStateLockFilename)
}

func managerStateTxnPath(root string) string {
	return filepath.Join(root, managerStateTxnFilename)
}

func managerStateMaterializationPath(root string) string {
	return filepath.Join(root, managerStateMatFilename)
}

func managerStatePathWithinRoot(root, path string) bool {
	root = cleanManagedRuntimeRoot(root)
	path = cleanManagedRuntimeRoot(path)
	if root == "" || path == "" {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	rel = filepath.Clean(rel)
	if rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func managerStateTargetPathAllowed(root string, entry managerStateMaterializeEntry) bool {
	path := entry.TargetPath
	if managerStatePathWithinRoot(root, path) {
		return true
	}
	path = cleanManagedRuntimeRoot(path)
	if path == "" {
		return false
	}
	base := filepath.Base(path)
	if base != localRuntimeProfileFilename && base != agentAnatomyFilename {
		return false
	}
	workdir := filepath.Dir(path)
	switch base {
	case localRuntimeProfileFilename:
		if !samePath(localRuntimeProfilePath(workdir), path) {
			return false
		}
	case agentAnatomyFilename:
		if !samePath(agentAnatomyPath(workdir), path) {
			return false
		}
	default:
		return false
	}
	if !managerStateExternalMarkerMatchesTarget(path) {
		return false
	}
	payloadDigest, err := managerStatePayloadDigest(entry.PayloadPath)
	if err != nil {
		return false
	}
	return localRuntimeMaterializationMarkerValid(path, payloadDigest)
}

func managerStatePayloadDigest(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type localRuntimeMaterializationMarker struct {
	TargetPath    string `json:"target_path,omitempty"`
	PayloadSHA256 string `json:"payload_sha256,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
}

func localRuntimeMaterializationMarkerPath(targetPath string) string {
	targetPath = cleanManagedRuntimeRoot(targetPath)
	if targetPath == "" {
		return ""
	}
	switch filepath.Base(targetPath) {
	case localRuntimeProfileFilename:
		return filepath.Join(filepath.Dir(targetPath), ".agent.runtime_materialize.json")
	case agentAnatomyFilename:
		return filepath.Join(filepath.Dir(targetPath), ".agent.anatomy_materialize.json")
	default:
		return ""
	}
}

func writeLocalRuntimeMaterializationMarker(targetPath string, payload []byte) error {
	targetPath = cleanManagedRuntimeRoot(targetPath)
	markerPath := localRuntimeMaterializationMarkerPath(targetPath)
	if targetPath == "" || markerPath == "" {
		return fmt.Errorf("local runtime materialization target is invalid")
	}
	marker := localRuntimeMaterializationMarker{
		TargetPath:    targetPath,
		PayloadSHA256: payloadSHA256(payload),
		CreatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(markerPath, raw, 0o600)
}

func localRuntimeMaterializationMarkerValid(targetPath, expectedDigest string) bool {
	targetPath = cleanManagedRuntimeRoot(targetPath)
	markerPath := localRuntimeMaterializationMarkerPath(targetPath)
	if targetPath == "" || markerPath == "" || strings.TrimSpace(expectedDigest) == "" {
		return false
	}
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return false
	}
	var marker localRuntimeMaterializationMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return false
	}
	if !samePath(marker.TargetPath, targetPath) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(marker.PayloadSHA256), strings.TrimSpace(expectedDigest))
}

func managerStateExternalMarkerMatchesTarget(targetPath string) bool {
	targetPath = cleanManagedRuntimeRoot(targetPath)
	markerPath := localRuntimeMaterializationMarkerPath(targetPath)
	if targetPath == "" || markerPath == "" {
		return false
	}
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return false
	}
	var marker localRuntimeMaterializationMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return false
	}
	return samePath(marker.TargetPath, targetPath)
}

func payloadSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func archiveBrokenManagerStateJournalPath(path, reason string) error {
	archived := path + ".broken-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	if err := replaceFileAtomic(path, archived); err != nil {
		return fmt.Errorf("%s: %w", reason, err)
	}
	return nil
}
