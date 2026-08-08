package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRepairManagerStateOnStartupCommitsPreparedJournal(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	target := filepath.Join(root, botRegistryFilename)
	tempPath, err := prepareManagerStateTempFile(target, []byte(`{"defaults":{"host_url":"https://rhizome.test"}}`), 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStateTempFile() error: %v", err)
	}
	if err := saveManagerStateTxnLocked(root, managerStateTxn{
		Operation:  "save_bot_registry",
		TargetPath: target,
		TempPath:   tempPath,
		Stage:      managerStateTxnStageReady,
		PID:        2147483647,
		StartedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("saveManagerStateTxnLocked() error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}

	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("expected recovered target file: %v", err)
	}
	if !strings.Contains(string(data), "https://rhizome.test") {
		t.Fatalf("unexpected recovered payload: %s", string(data))
	}
	if pathExists(managerStateTxnPath(root)) {
		t.Fatalf("expected journal to be removed from %s", managerStateTxnPath(root))
	}
}

func TestWithManagerStateLockReclaimsStaleLock(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	if err := os.WriteFile(managerStateLockPath(root), []byte(`{"pid":2147483647,"started_at":"2026-04-12T00:00:00Z"}`), 0o600); err != nil {
		t.Fatalf("WriteFile(lock) error: %v", err)
	}

	called := false
	if err := withManagerStateLock(root, true, func() error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("withManagerStateLock() error: %v", err)
	}
	if !called {
		t.Fatal("expected callback to run after stale lock recovery")
	}
	if pathExists(managerStateLockPath(root)) {
		t.Fatalf("expected manager state lock to be removed from %s", managerStateLockPath(root))
	}
}

func TestWithManagerStateLockSerializesSameProcessWaiters(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	origTimeout := managerStateLockTimeout
	origPoll := managerStateLockPoll
	managerStateLockTimeout = 120 * time.Millisecond
	managerStateLockPoll = 5 * time.Millisecond
	defer func() {
		managerStateLockTimeout = origTimeout
		managerStateLockPoll = origPoll
	}()

	root := agentRuntimeConfigRoot()
	var active int32
	var maxActive int32
	errs := make(chan error, 5)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := withManagerStateLock(root, true, func() error {
				now := atomic.AddInt32(&active, 1)
				for {
					prev := atomic.LoadInt32(&maxActive)
					if now <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, now) {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
				atomic.AddInt32(&active, -1)
				return nil
			})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("withManagerStateLock() concurrent waiter error: %v", err)
		}
	}
	if got := atomic.LoadInt32(&maxActive); got != 1 {
		t.Fatalf("expected same-process manager state critical section to be serialized, max active = %d", got)
	}
	if pathExists(managerStateLockPath(root)) {
		t.Fatalf("expected manager state lock to be removed from %s", managerStateLockPath(root))
	}
}

func TestManagerStateLockIsStaleWhenAgeExceedsThreshold(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	old := time.Now().Add(-managerStateLockStaleAfter - time.Minute).UTC().Format(time.RFC3339Nano)
	if err := os.WriteFile(managerStateLockPath(root), []byte(fmt.Sprintf("{\"pid\":%d,\"started_at\":%q}", os.Getpid(), old)), 0o600); err != nil {
		t.Fatalf("WriteFile(lock) error: %v", err)
	}

	stale, err := managerStateLockIsStale(root)
	if err != nil {
		t.Fatalf("managerStateLockIsStale() error: %v", err)
	}
	if !stale {
		t.Fatal("expected old lock to be considered stale even when the pid is live")
	}
}

func TestRepairManagerStateOnStartupArchivesCorruptJournal(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	if err := os.WriteFile(managerStateTxnPath(root), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("WriteFile(journal) error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}
	if pathExists(managerStateTxnPath(root)) {
		t.Fatalf("expected corrupt journal to be archived from %s", managerStateTxnPath(root))
	}
	matches, err := filepath.Glob(managerStateTxnPath(root) + ".broken-*")
	if err != nil {
		t.Fatalf("Glob() error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived journal, got %v", matches)
	}
}

func TestRepairManagerStateOnStartupRejectsExternalTempPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	target := filepath.Join(root, botRegistryFilename)
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("secret-data"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside) error: %v", err)
	}
	if err := saveManagerStateTxnLocked(root, managerStateTxn{
		Operation:  "save_bot_registry",
		TargetPath: target,
		TempPath:   outside,
		Stage:      managerStateTxnStageReady,
		PID:        2147483647,
		StartedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("saveManagerStateTxnLocked() error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}
	data, err := os.ReadFile(outside)
	if err != nil {
		t.Fatalf("expected outside temp file to remain untouched: %v", err)
	}
	if string(data) != "secret-data" {
		t.Fatalf("expected outside temp file contents to survive, got %q", string(data))
	}
	if pathExists(target) {
		t.Fatalf("expected target %s to stay absent when temp path escapes root", target)
	}
	matches, err := filepath.Glob(managerStateTxnPath(root) + ".broken-*")
	if err != nil {
		t.Fatalf("Glob() error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived journal, got %v", matches)
	}
}

func TestRepairManagerStateOnStartupSkipsLiveLock(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	if err := os.WriteFile(managerStateLockPath(root), []byte(fmt.Sprintf("{\"pid\":%d,\"started_at\":\"2026-04-12T00:00:00Z\"}", os.Getpid())), 0o600); err != nil {
		t.Fatalf("WriteFile(lock) error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}
	if !pathExists(managerStateLockPath(root)) {
		t.Fatalf("expected live lock at %s to remain in place", managerStateLockPath(root))
	}
}

func TestRepairManagerStateOnStartupAppliesPendingMaterialization(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	workdir := t.TempDir()
	localRaw, _, err := marshalLocalRuntimeProfileForWrite(LocalRuntimeProfile{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		WorkspaceID: "ws-1",
		LLMBackend:  llmBackendCodex,
		Model:       defaultModel,
	})
	if err != nil {
		t.Fatalf("marshalLocalRuntimeProfileForWrite() error: %v", err)
	}
	registryRaw, _, err := marshalBotRegistryForWrite(BotRegistry{
		Agents: []ManagedAgentRecord{{
			AgentID:     "agent-1",
			DisplayName: "Agent One",
			Workdir:     workdir,
			WorkspaceID: "ws-1",
			LLMBackend:  llmBackendCodex,
			Model:       defaultModel,
		}},
	})
	if err != nil {
		t.Fatalf("marshalBotRegistryForWrite() error: %v", err)
	}
	registryPayload, err := prepareManagerStatePayloadFile(root, "mat-registry-", registryRaw, 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStatePayloadFile(registry) error: %v", err)
	}
	payloadPath, err := prepareManagerStatePayloadFile(root, "mat-local-runtime-", localRaw, 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStatePayloadFile() error: %v", err)
	}
	if err := writeLocalRuntimeMaterializationMarker(localRuntimeProfilePath(workdir), localRaw); err != nil {
		t.Fatalf("writeLocalRuntimeMaterializationMarker() error: %v", err)
	}
	if err := saveManagerStateMaterializationLocked(root, managerStateMaterialization{
		Operation: "persist_runtime_profiles",
		Entries: []managerStateMaterializeEntry{
			{
				TargetPath:  botRegistryPath(),
				PayloadPath: registryPayload,
				Perm:        0o600,
			},
			{
				TargetPath:  localRuntimeProfilePath(workdir),
				PayloadPath: payloadPath,
				Perm:        0o600,
			},
		},
		PID:       2147483647,
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("saveManagerStateMaterializationLocked() error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}
	got := LoadLocalRuntimeProfile(workdir)
	if got.AgentID != "agent-1" || got.WorkspaceID != "ws-1" {
		t.Fatalf("expected local runtime materialization to recover, got %+v", got)
	}
	if pathExists(managerStateMaterializationPath(root)) {
		t.Fatalf("expected materialization journal to be removed from %s", managerStateMaterializationPath(root))
	}
	if pathExists(payloadPath) {
		t.Fatalf("expected payload %s to be removed after recovery", payloadPath)
	}
	if pathExists(registryPayload) {
		t.Fatalf("expected registry payload %s to be removed after recovery", registryPayload)
	}
	if pathExists(localRuntimeMaterializationMarkerPath(localRuntimeProfilePath(workdir))) {
		t.Fatalf("expected local runtime materialization marker to be removed after recovery")
	}
}

func TestRepairManagerStateOnStartupRejectsUntrackedExternalLocalRuntimeTarget(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	workdir := t.TempDir()
	localRaw, _, err := marshalLocalRuntimeProfileForWrite(LocalRuntimeProfile{
		AgentID:     "agent-rogue",
		WorkspaceID: "ws-rogue",
	})
	if err != nil {
		t.Fatalf("marshalLocalRuntimeProfileForWrite() error: %v", err)
	}
	payloadPath, err := prepareManagerStatePayloadFile(root, "mat-local-runtime-", localRaw, 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStatePayloadFile() error: %v", err)
	}
	target := localRuntimeProfilePath(workdir)
	if err := saveManagerStateMaterializationLocked(root, managerStateMaterialization{
		Operation: "persist_runtime_profiles",
		Entries: []managerStateMaterializeEntry{{
			TargetPath:  target,
			PayloadPath: payloadPath,
			Perm:        0o600,
		}},
		PID:       2147483647,
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("saveManagerStateMaterializationLocked() error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}
	if pathExists(target) {
		t.Fatalf("expected untracked local runtime target %s to stay absent", target)
	}
	if pathExists(payloadPath) {
		t.Fatalf("expected payload %s to be cleaned up after rejection", payloadPath)
	}
	if pathExists(managerStateMaterializationPath(root)) {
		t.Fatalf("expected materialization journal to be archived from %s", managerStateMaterializationPath(root))
	}
	matches, err := filepath.Glob(managerStateMaterializationPath(root) + ".broken-*")
	if err != nil {
		t.Fatalf("Glob() error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived materialization journal, got %v", matches)
	}
}

func TestRepairManagerStateOnStartupRejectsExternalLocalRuntimeWithoutMarkerEvenWithRegistryPayload(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	workdir := t.TempDir()
	localRaw, _, err := marshalLocalRuntimeProfileForWrite(LocalRuntimeProfile{
		AgentID:     "agent-1",
		WorkspaceID: "ws-1",
	})
	if err != nil {
		t.Fatalf("marshalLocalRuntimeProfileForWrite() error: %v", err)
	}
	registryRaw, _, err := marshalBotRegistryForWrite(BotRegistry{
		Agents: []ManagedAgentRecord{{
			AgentID: "agent-1",
			Workdir: workdir,
		}},
	})
	if err != nil {
		t.Fatalf("marshalBotRegistryForWrite() error: %v", err)
	}
	registryPayload, err := prepareManagerStatePayloadFile(root, "mat-registry-", registryRaw, 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStatePayloadFile(registry) error: %v", err)
	}
	localPayload, err := prepareManagerStatePayloadFile(root, "mat-local-runtime-", localRaw, 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStatePayloadFile(local) error: %v", err)
	}
	target := localRuntimeProfilePath(workdir)
	if err := saveManagerStateMaterializationLocked(root, managerStateMaterialization{
		Operation: "persist_runtime_profiles",
		Entries: []managerStateMaterializeEntry{
			{
				TargetPath:  botRegistryPath(),
				PayloadPath: registryPayload,
				Perm:        0o600,
			},
			{
				TargetPath:  target,
				PayloadPath: localPayload,
				Perm:        0o600,
			},
		},
		PID:       2147483647,
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("saveManagerStateMaterializationLocked() error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}
	if pathExists(target) {
		t.Fatalf("expected local runtime target %s to stay absent without marker", target)
	}
	if pathExists(registryPayload) || pathExists(localPayload) {
		t.Fatalf("expected payloads to be cleaned up after rejection")
	}
	matches, err := filepath.Glob(managerStateMaterializationPath(root) + ".broken-*")
	if err != nil {
		t.Fatalf("Glob() error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived materialization journal, got %v", matches)
	}
}

func TestRepairManagerStateOnStartupRollsBackPartialMaterialization(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll(root) error: %v", err)
	}
	originalRhizome := []byte(`{"workspace_id":"ws-original"}`)
	if err := os.WriteFile(rhizomeProfilePath(), originalRhizome, 0o600); err != nil {
		t.Fatalf("WriteFile(rhizome) error: %v", err)
	}
	rhizomePayload, err := prepareManagerStatePayloadFile(root, "mat-rhizome-", []byte(`{"workspace_id":"ws-updated"}`), 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStatePayloadFile(rhizome) error: %v", err)
	}
	registryPayload, err := prepareManagerStatePayloadFile(root, "mat-registry-", []byte(`{"agents":[]}`), 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStatePayloadFile(registry) error: %v", err)
	}
	if err := os.MkdirAll(botRegistryPath(), 0o700); err != nil {
		t.Fatalf("MkdirAll(botRegistryPath) error: %v", err)
	}
	if err := saveManagerStateMaterializationLocked(root, managerStateMaterialization{
		Operation: "persist_runtime_profiles",
		Entries: []managerStateMaterializeEntry{
			{
				TargetPath:  rhizomeProfilePath(),
				PayloadPath: rhizomePayload,
				Perm:        0o600,
			},
			{
				TargetPath:  botRegistryPath(),
				PayloadPath: registryPayload,
				Perm:        0o600,
			},
		},
		PID:       2147483647,
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("saveManagerStateMaterializationLocked() error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}
	gotRhizome, err := os.ReadFile(rhizomeProfilePath())
	if err != nil {
		t.Fatalf("ReadFile(rhizome) error: %v", err)
	}
	if string(gotRhizome) != string(originalRhizome) {
		t.Fatalf("expected rhizome profile rollback, got %s", string(gotRhizome))
	}
	if pathExists(rhizomePayload) {
		t.Fatalf("expected rhizome payload %s to be cleaned up", rhizomePayload)
	}
	if pathExists(registryPayload) {
		t.Fatalf("expected registry payload %s to be cleaned up", registryPayload)
	}
	if pathExists(managerStateMaterializationPath(root)) {
		t.Fatalf("expected materialization journal to be archived from %s", managerStateMaterializationPath(root))
	}
	matches, err := filepath.Glob(managerStateMaterializationPath(root) + ".broken-*")
	if err != nil {
		t.Fatalf("Glob() error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived materialization journal, got %v", matches)
	}
}

func TestRepairManagerStateOnStartupArchivesMaterializationWithMissingBackupBeforeApply(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	root := agentRuntimeConfigRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll(root) error: %v", err)
	}
	originalRhizome := []byte(`{"workspace_id":"ws-original"}`)
	if err := os.WriteFile(rhizomeProfilePath(), originalRhizome, 0o600); err != nil {
		t.Fatalf("WriteFile(rhizome) error: %v", err)
	}
	rhizomePayload, err := prepareManagerStatePayloadFile(root, "mat-rhizome-", []byte(`{"workspace_id":"ws-updated"}`), 0o600)
	if err != nil {
		t.Fatalf("prepareManagerStatePayloadFile(rhizome) error: %v", err)
	}
	missingPayload := filepath.Join(root, "missing-payload.json")
	missingBackup := filepath.Join(root, "missing-backup.json")
	if err := saveManagerStateMaterializationLocked(root, managerStateMaterialization{
		Operation: "persist_runtime_profiles",
		Entries: []managerStateMaterializeEntry{
			{
				TargetPath:  rhizomeProfilePath(),
				PayloadPath: rhizomePayload,
				BackupPath:  missingBackup,
				BackupPerm:  0o600,
				Perm:        0o600,
			},
			{
				TargetPath:  botRegistryPath(),
				PayloadPath: missingPayload,
				Perm:        0o600,
			},
		},
		PID:       2147483647,
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("saveManagerStateMaterializationLocked() error: %v", err)
	}

	if err := repairManagerStateOnStartup(); err != nil {
		t.Fatalf("repairManagerStateOnStartup() error: %v", err)
	}
	gotRhizome, err := os.ReadFile(rhizomeProfilePath())
	if err != nil {
		t.Fatalf("ReadFile(rhizome) error: %v", err)
	}
	if string(gotRhizome) != string(originalRhizome) {
		t.Fatalf("expected rhizome profile to remain unchanged, got %s", string(gotRhizome))
	}
	if pathExists(rhizomePayload) {
		t.Fatalf("expected rhizome payload %s to be cleaned up", rhizomePayload)
	}
	if pathExists(managerStateMaterializationPath(root)) {
		t.Fatalf("expected materialization journal to be archived from %s", managerStateMaterializationPath(root))
	}
	matches, err := filepath.Glob(managerStateMaterializationPath(root) + ".broken-*")
	if err != nil {
		t.Fatalf("Glob() error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one archived materialization journal, got %v", matches)
	}
}
