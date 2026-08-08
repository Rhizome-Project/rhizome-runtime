package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/fssecure"
)

const agentProcessFilename = "agent.process.json"
const agentProcessLogDirname = ".rhizome-logs"

const (
	managedRealPilotDefaultBudgetHardLimitMicros = int64(100_000_000_000)
	managedRealPilotDefaultBudgetReserveMicros   = int64(200_000_000)
	managedRealPilotDefaultBudgetMicrosPerToken  = int64(1)
	managedRealPilotDefaultProviderCallTimeout   = realLLMPilotMaxProviderCallTimeout
)

var (
	managedAgentProcessExistsFunc   = processExists
	managedAgentKillProcessFunc     = killProcess
	managedAgentStartProcessFunc    = startDetachedProcess
	managedAgentSaveStateFunc       = SaveAgentProcessState
	managedAgentCleanupWorkdirFunc  = cleanupManagedAgentWorkdirProcesses
	managedAgentProcessMatchesFunc  = managedAgentProcessMatchesRecord
	managedAgentProcessSnapshotFunc = buildManagedAgentProcessSnapshot
	managedAgentStopExitTimeout     = 5 * time.Second
	managedAgentProcessExitPollGap  = 150 * time.Millisecond
	managedAgentStartProofTimeout   = 2 * time.Second
	managedAgentStartProofPollGap   = 100 * time.Millisecond
)

const managedAgentKnownProcessTreeGraceTimeout = 750 * time.Millisecond

type AgentProcessState struct {
	PID                 int      `json:"pid,omitempty"`
	Executable          string   `json:"executable,omitempty"`
	ExecutableSHA256    string   `json:"executable_sha256,omitempty"`
	Mode                string   `json:"mode,omitempty"`
	Workdir             string   `json:"workdir,omitempty"`
	Args                []string `json:"args,omitempty"`
	ArgsDigest          string   `json:"args_digest,omitempty"`
	RuntimeConfigDigest string   `json:"runtime_config_digest,omitempty"`
	BuildRevision       string   `json:"build_revision,omitempty"`
	LogOutPath          string   `json:"log_out_path,omitempty"`
	LogErrPath          string   `json:"log_err_path,omitempty"`
	StartedAt           string   `json:"started_at,omitempty"`
	UpdatedAt           string   `json:"updated_at,omitempty"`
}

type ManagedAgentProcessStatus struct {
	State               string   `json:"state"`
	Running             bool     `json:"running"`
	Stale               bool     `json:"stale"`
	PID                 int      `json:"pid,omitempty"`
	Executable          string   `json:"executable,omitempty"`
	ExecutableSHA256    string   `json:"executable_sha256,omitempty"`
	Workdir             string   `json:"workdir,omitempty"`
	Args                []string `json:"args,omitempty"`
	ArgsDigest          string   `json:"args_digest,omitempty"`
	RuntimeConfigDigest string   `json:"runtime_config_digest,omitempty"`
	BuildRevision       string   `json:"build_revision,omitempty"`
	DriftReasons        []string `json:"drift_reasons,omitempty"`
	LogOutPath          string   `json:"log_out_path,omitempty"`
	LogErrPath          string   `json:"log_err_path,omitempty"`
	StartedAt           string   `json:"started_at,omitempty"`
	UpdatedAt           string   `json:"updated_at,omitempty"`
}

type ManagedAgentLogTail struct {
	Stdout     []string `json:"stdout"`
	Stderr     []string `json:"stderr"`
	LogOutPath string   `json:"log_out_path"`
	LogErrPath string   `json:"log_err_path"`
	Error      string   `json:"error,omitempty"`
}

type ManagedAgentAnatomyMaterialization struct {
	Path                 string   `json:"path,omitempty"`
	Source               string   `json:"source,omitempty"`
	Preset               string   `json:"preset,omitempty"`
	ProfileID            string   `json:"profile_id,omitempty"`
	Digest               string   `json:"digest,omitempty"`
	InstalledToolBundles []string `json:"installed_tool_bundles,omitempty"`
	SkippedToolBundles   []string `json:"skipped_tool_bundles,omitempty"`
}

type managedAgentProcessProbe struct {
	PID            int
	Exists         bool
	LookupErr      error
	ExecutablePath string
	CommandLine    string
	CommandLineErr error
}

var errManagedAgentProcessIdentityUnavailable = errors.New("managed agent process identity evidence unavailable")

func agentProcessStatePath(workdir string) string {
	if workdir == "" {
		return ""
	}
	return filepath.Join(workdir, agentProcessFilename)
}

func LoadAgentProcessState(workdir string) AgentProcessState {
	path := agentProcessStatePath(workdir)
	if path == "" {
		return AgentProcessState{}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return AgentProcessState{}
	}
	var state AgentProcessState
	if err := json.Unmarshal(data, &state); err != nil {
		return AgentProcessState{}
	}
	return state
}

func SaveAgentProcessState(workdir string, state AgentProcessState) error {
	path := agentProcessStatePath(workdir)
	if path == "" {
		return nil
	}
	if state.Workdir == "" {
		state.Workdir = workdir
	}
	state.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

func RemoveAgentProcessState(workdir string) error {
	path := agentProcessStatePath(workdir)
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func ManagedAgentRuntimeStatus(record ManagedAgentRecord) string {
	return InspectManagedAgentProcess(record).State
}

func InspectManagedAgentProcess(record ManagedAgentRecord) ManagedAgentProcessStatus {
	return inspectManagedAgentProcess(record, nil)
}

func InspectManagedAgentProcessWithSnapshot(record ManagedAgentRecord, snapshot map[int]managedAgentProcessProbe) ManagedAgentProcessStatus {
	return inspectManagedAgentProcess(record, snapshot)
}

func ManagedAgentProcessStatusFromStartedState(record ManagedAgentRecord, state AgentProcessState) ManagedAgentProcessStatus {
	record = normalizeManagedAgentRecord(record)
	if state.Workdir == "" {
		state.Workdir = record.Workdir
	}
	status := ManagedAgentProcessStatus{
		State:               "running",
		Running:             state.PID > 0,
		PID:                 state.PID,
		Executable:          state.Executable,
		ExecutableSHA256:    state.ExecutableSHA256,
		Workdir:             firstNonEmpty(state.Workdir, record.Workdir),
		Args:                append([]string(nil), state.Args...),
		ArgsDigest:          state.ArgsDigest,
		RuntimeConfigDigest: state.RuntimeConfigDigest,
		BuildRevision:       state.BuildRevision,
		LogOutPath:          state.LogOutPath,
		LogErrPath:          state.LogErrPath,
		StartedAt:           state.StartedAt,
		UpdatedAt:           state.UpdatedAt,
	}
	if status.PID <= 0 {
		status.State = "unknown"
	}
	if status.LogOutPath == "" && status.Workdir != "" {
		status.LogOutPath, _ = resolvedManagedAgentLogPaths(status.Workdir)
	}
	if status.LogErrPath == "" && status.Workdir != "" {
		_, status.LogErrPath = resolvedManagedAgentLogPaths(status.Workdir)
	}
	return status
}

func inspectManagedAgentProcess(record ManagedAgentRecord, snapshot map[int]managedAgentProcessProbe) ManagedAgentProcessStatus {
	record = normalizeManagedAgentRecord(record)
	state := LoadAgentProcessState(record.Workdir)
	status := ManagedAgentProcessStatus{
		PID:                 state.PID,
		Executable:          state.Executable,
		ExecutableSHA256:    state.ExecutableSHA256,
		Workdir:             firstNonEmpty(state.Workdir, record.Workdir),
		Args:                append([]string(nil), state.Args...),
		ArgsDigest:          state.ArgsDigest,
		RuntimeConfigDigest: state.RuntimeConfigDigest,
		BuildRevision:       state.BuildRevision,
		LogOutPath:          state.LogOutPath,
		LogErrPath:          state.LogErrPath,
		StartedAt:           state.StartedAt,
		UpdatedAt:           state.UpdatedAt,
	}
	if status.LogOutPath == "" && status.Workdir != "" {
		status.LogOutPath, _ = resolvedManagedAgentLogPaths(status.Workdir)
	}
	if status.LogErrPath == "" && status.Workdir != "" {
		_, status.LogErrPath = resolvedManagedAgentLogPaths(status.Workdir)
	}
	if state.PID <= 0 {
		status.State = "stopped"
		return status
	}
	ok, err := managedAgentProbeProcessExists(state.PID, snapshot)
	if err != nil {
		status.State = "unknown"
		return status
	}
	if ok {
		matches, matchErr := managedAgentProbeProcessMatches(state.PID, state, record, snapshot)
		if matchErr != nil {
			status.State = "unknown"
			return status
		}
		if !matches {
			status.State = "stale"
			status.Stale = true
			return status
		}
		if reasons := managedAgentProcessProvenanceUnknownReasons(state); len(reasons) > 0 {
			status.State = "unknown"
			status.DriftReasons = reasons
			return status
		}
		if reasons := managedAgentProcessProvenanceDriftReasons(state, record); len(reasons) > 0 {
			status.State = "stale"
			status.Stale = true
			status.DriftReasons = reasons
			return status
		}
		status.State = "running"
		status.Running = true
		return status
	}
	status.State = "stale"
	status.Stale = true
	return status
}

func managedAgentProbeProcessExists(pid int, snapshot map[int]managedAgentProcessProbe) (bool, error) {
	if snapshot != nil {
		if probe, ok := snapshot[pid]; ok {
			return probe.Exists, probe.LookupErr
		}
	}
	return managedAgentProcessExistsFunc(pid)
}

func managedAgentProbeProcessMatches(pid int, state AgentProcessState, record ManagedAgentRecord, snapshot map[int]managedAgentProcessProbe) (bool, error) {
	if snapshot != nil {
		if probe, ok := snapshot[pid]; ok {
			if probe.CommandLineErr != nil {
				return false, probe.CommandLineErr
			}
			if strings.TrimSpace(probe.ExecutablePath) != "" &&
				strings.TrimSpace(state.Executable) != "" &&
				!sameFilesystemPath(probe.ExecutablePath, state.Executable) {
				return false, nil
			}
			if strings.TrimSpace(probe.CommandLine) == "" {
				return false, errManagedAgentProcessIdentityUnavailable
			}
			return managedAgentCommandLineMatchesRecord(probe.CommandLine, state, record), nil
		}
	}
	return managedAgentProcessMatchesFunc(pid, state, record)
}

func buildManagedAgentProcessSnapshotFallback(records []ManagedAgentRecord) map[int]managedAgentProcessProbe {
	out := map[int]managedAgentProcessProbe{}
	for _, record := range records {
		state := LoadAgentProcessState(normalizeManagedAgentRecord(record).Workdir)
		if state.PID <= 0 {
			continue
		}
		if _, ok := out[state.PID]; ok {
			continue
		}
		probe := managedAgentProcessProbe{PID: state.PID}
		probe.Exists, probe.LookupErr = managedAgentProcessExistsFunc(state.PID)
		if probe.LookupErr == nil && probe.Exists {
			probe.CommandLine, probe.CommandLineErr = processCommandLine(state.PID)
		}
		out[state.PID] = probe
	}
	return out
}

func CleanupStaleManagedAgentProcessState(record ManagedAgentRecord) (bool, error) {
	record = normalizeManagedAgentRecord(record)
	state := LoadAgentProcessState(record.Workdir)
	if state.PID <= 0 {
		if state.Workdir != "" {
			if err := RemoveAgentProcessState(record.Workdir); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil
	}
	ok, err := managedAgentProcessExistsFunc(state.PID)
	if err != nil {
		return false, err
	}
	if ok {
		if matches, err := managedAgentProcessMatchesFunc(state.PID, state, record); err != nil {
			return false, err
		} else if !matches {
			releaseManagedAgentProcessResources(state.PID)
			if err := RemoveAgentProcessState(record.Workdir); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil
	}
	releaseManagedAgentProcessResources(state.PID)
	if err := RemoveAgentProcessState(record.Workdir); err != nil {
		return false, err
	}
	return true, nil
}

func RestartManagedAgent(record ManagedAgentRecord) (AgentProcessState, error) {
	return RestartManagedAgentWithPreflight(record)
}

func restartManagedAgentWithoutPreflightForTest(record ManagedAgentRecord) (AgentProcessState, error) {
	record = normalizeManagedAgentRecord(record)
	if _, err := CleanupStaleManagedAgentProcessState(record); err != nil {
		return AgentProcessState{}, fmt.Errorf("restart agent %s: cleanup stale process state: %w", record.AgentID, err)
	}
	if err := StopManagedAgent(record); err != nil {
		return AgentProcessState{}, fmt.Errorf("restart agent %s: stop current process: %w", record.AgentID, err)
	}
	state, err := startManagedAgent(record, "")
	if err != nil {
		return AgentProcessState{}, fmt.Errorf("restart agent %s stopped current process but failed to start replacement: %w", record.AgentID, err)
	}
	return state, nil
}

func StartManagedAgent(record ManagedAgentRecord) (AgentProcessState, error) {
	return StartManagedAgentWithPreflight(record)
}

func startManagedAgentWithoutPreflightForTest(record ManagedAgentRecord) (AgentProcessState, error) {
	return startManagedAgent(record, "")
}

func StartManagedAgentWithPreflight(record ManagedAgentRecord) (AgentProcessState, error) {
	return StartManagedAgentWithOptions(record, managedRunPreflightOptions{})
}

func StartManagedAgentWithOptions(record ManagedAgentRecord, options managedRunPreflightOptions) (AgentProcessState, error) {
	record = normalizeManagedAgentRecord(record)
	preflight, err := admitManagedRunStartForProcess(record, options)
	if err != nil {
		return AgentProcessState{}, err
	}
	return startManagedAgent(record, preflight.ChildExecutablePath)
}

func RestartManagedAgentWithPreflight(record ManagedAgentRecord) (AgentProcessState, error) {
	return RestartManagedAgentWithOptions(record, managedRunPreflightOptions{})
}

func RestartManagedAgentWithOptions(record ManagedAgentRecord, options managedRunPreflightOptions) (AgentProcessState, error) {
	record = normalizeManagedAgentRecord(record)
	if _, err := CleanupStaleManagedAgentProcessState(record); err != nil {
		return AgentProcessState{}, fmt.Errorf("restart agent %s: cleanup stale process state: %w", record.AgentID, err)
	}
	if err := StopManagedAgent(record); err != nil {
		return AgentProcessState{}, fmt.Errorf("restart agent %s: stop current process: %w", record.AgentID, err)
	}
	preflight, err := admitManagedRunStartForProcess(record, options)
	if err != nil {
		return AgentProcessState{}, fmt.Errorf("restart agent %s stopped current process but preflight blocked replacement: %w", record.AgentID, err)
	}
	state, err := startManagedAgent(record, preflight.ChildExecutablePath)
	if err != nil {
		return AgentProcessState{}, fmt.Errorf("restart agent %s stopped current process but failed to start replacement: %w", record.AgentID, err)
	}
	return state, nil
}

func admitManagedRunStartForProcess(record ManagedAgentRecord, options managedRunPreflightOptions) (managedRunPreflightResult, error) {
	if options.hasOverrides() {
		return admitManagedRunStartWithOptionsFunc(record, options)
	}
	return admitManagedRunStartFunc(record)
}

func startManagedAgent(record ManagedAgentRecord, executablePathOverride string) (AgentProcessState, error) {
	record = normalizeManagedAgentRecord(record)
	if record.AgentID == "" || record.Workdir == "" {
		return AgentProcessState{}, fmt.Errorf("managed agent requires agent_id and workdir")
	}
	if err := validateProviderReference(record.ProviderID); err != nil {
		return AgentProcessState{}, err
	}
	status := InspectManagedAgentProcess(record)
	if status.Stale {
		if _, err := CleanupStaleManagedAgentProcessState(record); err != nil {
			return AgentProcessState{}, fmt.Errorf("cleanup stale process state: %w", err)
		}
		status = InspectManagedAgentProcess(record)
	}
	if status.Stale || strings.EqualFold(status.State, "unknown") {
		reason := strings.TrimSpace(strings.Join(status.DriftReasons, ","))
		if reason == "" {
			reason = firstNonEmpty(status.State, "untrusted_process_identity")
		}
		return AgentProcessState{}, fmt.Errorf("agent %s has untrusted managed process state %s: %s", record.AgentID, firstNonEmpty(status.State, "unknown"), reason)
	}
	if status.Running {
		return LoadAgentProcessState(record.Workdir), fmt.Errorf("agent %s is already running", record.AgentID)
	}
	if err := cleanupManagedAgentWorkdirProcessesAfterStop(record); err != nil {
		return AgentProcessState{}, fmt.Errorf("cleanup prior agent runtime resources: %w", err)
	}

	executablePath := strings.TrimSpace(executablePathOverride)
	if executablePath == "" {
		resolved, err := os.Executable()
		if err != nil {
			return AgentProcessState{}, fmt.Errorf("resolve executable: %w", err)
		}
		executablePath = resolved
	}
	executableHash, err := managerFileSHA256(executablePath)
	if err != nil {
		return AgentProcessState{}, fmt.Errorf("hash executable: %w", err)
	}
	logOutPath, logErrPath := privateManagedAgentLogPaths(record.Workdir)
	if err := os.MkdirAll(record.Workdir, 0o755); err != nil {
		return AgentProcessState{}, fmt.Errorf("create workdir: %w", err)
	}
	cfg := managedAgentEffectiveRuntimeConfig(record, managedAgentStartRuntimeConfig(record))
	if err := SaveLocalRuntimeProfile(record.Workdir, managedAgentStartLocalRuntimeProfile(record, cfg)); err != nil {
		return AgentProcessState{}, fmt.Errorf("materialize local runtime profile: %w", err)
	}
	if _, err := cleanupManagedAgentRuntimeIdentityLeaseBeforeStart(cfg); err != nil {
		return AgentProcessState{}, fmt.Errorf("cleanup prior runtime identity lease: %w", err)
	}
	if _, err := MaterializeManagedAgentAnatomy(record, cfg); err != nil {
		return AgentProcessState{}, fmt.Errorf("materialize agent anatomy: %w", err)
	}
	clearManagedAgentGracefulStop(record.Workdir)

	stdout, err := openProcessLog(logOutPath)
	if err != nil {
		return AgentProcessState{}, err
	}
	defer stdout.Close()
	stderr, err := openProcessLog(logErrPath)
	if err != nil {
		return AgentProcessState{}, err
	}
	defer stderr.Close()

	env, err := buildManagedAgentProcessEnv(record)
	if err != nil {
		return AgentProcessState{}, err
	}

	args := managedAgentDaemonArgs(record)
	pid, err := managedAgentStartProcessFunc(executablePath, args, record.Workdir, env, stdout, stderr)
	if err != nil {
		return AgentProcessState{}, err
	}
	buildInfo := managerCurrentRuntimeBuildIdentity()

	state := AgentProcessState{
		PID:                 pid,
		Executable:          executablePath,
		ExecutableSHA256:    executableHash,
		Mode:                string(RuntimeModeDaemon),
		Workdir:             record.Workdir,
		Args:                append([]string(nil), args...),
		ArgsDigest:          managedAgentArgsDigest(args),
		RuntimeConfigDigest: managedAgentRuntimeConfigDigest(cfg),
		BuildRevision:       buildInfo.VCSRevision,
		LogOutPath:          logOutPath,
		LogErrPath:          logErrPath,
		StartedAt:           time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := managedAgentSaveStateFunc(record.Workdir, state); err != nil {
		cleanupErr := rollbackStartedManagedAgentProcess(pid)
		if cleanupErr != nil {
			return AgentProcessState{}, fmt.Errorf("persist started agent %s state after starting pid %d: %v; rollback started process failed: %w", record.AgentID, pid, err, cleanupErr)
		}
		return AgentProcessState{}, fmt.Errorf("persist started agent %s state after starting pid %d: %w (rolled back started process)", record.AgentID, pid, err)
	}
	if err := proveManagedAgentStarted(record, state); err != nil {
		_ = RemoveAgentProcessState(record.Workdir)
		cleanupErr := rollbackStartedManagedAgentProcess(pid)
		if cleanupErr != nil {
			return AgentProcessState{}, fmt.Errorf("started agent %s pid %d failed readiness proof: %v; rollback started process failed: %w", record.AgentID, pid, err, cleanupErr)
		}
		return AgentProcessState{}, fmt.Errorf("started agent %s pid %d failed readiness proof: %w", record.AgentID, pid, err)
	}
	return state, nil
}

func cleanupManagedAgentRuntimeIdentityLeaseBeforeStart(cfg RuntimeConfig) (bool, error) {
	cfg.ApplyDefaults()
	if cfg.Mode != RuntimeModeDaemon {
		return false, nil
	}
	workspaceID := strings.TrimSpace(cfg.WorkspaceID)
	agentID := strings.TrimSpace(cfg.AgentID)
	if workspaceID == "" || agentID == "" {
		return false, nil
	}
	leaseRoot := managedAgentIdentityLeaseRootPath(cfg.Workdir)
	if strings.TrimSpace(leaseRoot) != "" {
		leaseRoot = filepath.Join(cleanManagedRuntimeRoot(leaseRoot), runtimeIdentityLeaseDirname)
	}
	path := runtimeIdentityLeasePathUnderRoot(leaseRoot, workspaceID, agentID)
	if strings.TrimSpace(path) == "" {
		return false, nil
	}
	existing, err := loadRuntimeIdentityLease(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, archiveBrokenRuntimeIdentityLease(path, "corrupt runtime identity lease")
	}
	if runtimeIdentityLeaseIsStale(existing, time.Now().UTC()) {
		if err := quarantineRuntimeIdentityLease(path); err != nil {
			return false, err
		}
		return true, nil
	}
	if existing.PID <= 0 {
		if err := quarantineRuntimeIdentityLease(path); err != nil {
			return false, err
		}
		return true, nil
	}
	live, err := managedAgentProcessExistsFunc(existing.PID)
	if err != nil {
		return false, err
	}
	if !live {
		if err := quarantineRuntimeIdentityLease(path); err != nil {
			return false, err
		}
		return true, nil
	}
	matchesLease, err := runtimeIdentityLeaseProcessMatchesFunc(existing)
	if err != nil {
		return false, err
	}
	if !matchesLease {
		if err := quarantineRuntimeIdentityLease(path); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func proveManagedAgentStarted(record ManagedAgentRecord, state AgentProcessState) error {
	record = normalizeManagedAgentRecord(record)
	if state.PID <= 0 {
		return fmt.Errorf("started process has no pid")
	}
	timeout := managedAgentStartProofTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	gap := managedAgentStartProofPollGap
	if gap <= 0 {
		gap = 100 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ok, existsErr := managedAgentProcessExistsFunc(state.PID)
		if existsErr != nil {
			lastErr = fmt.Errorf("process existence probe failed: %w", existsErr)
		} else if !ok {
			lastErr = fmt.Errorf("process exited before readiness proof")
		} else {
			matches, matchErr := managedAgentProcessMatchesFunc(state.PID, state, record)
			if matchErr != nil {
				lastErr = fmt.Errorf("process identity probe failed: %w", matchErr)
			} else if !matches {
				lastErr = fmt.Errorf("process identity does not match managed agent")
			} else if reasons := managedAgentProcessProvenanceUnknownReasons(state); len(reasons) > 0 {
				lastErr = fmt.Errorf("process provenance unknown: %s", strings.Join(reasons, ","))
			} else if reasons := managedAgentProcessProvenanceDriftReasons(state, record); len(reasons) > 0 {
				lastErr = fmt.Errorf("process provenance drift: %s", strings.Join(reasons, ","))
			} else {
				return nil
			}
		}
		if !time.Now().Before(deadline) {
			return lastErr
		}
		time.Sleep(gap)
	}
}

func managedAgentDaemonArgs(record ManagedAgentRecord) []string {
	record = normalizeManagedAgentRecord(record)
	args := []string{"daemon", "--workdir", record.Workdir}
	cfg := managedAgentEffectiveRuntimeConfig(record, managedAgentStartRuntimeConfig(record))
	args = appendManagedAgentRuntimeConfigArgs(args, cfg)
	if !managedAgentStartNeedsRealPilot(cfg) {
		return args
	}
	args = append(args, "--real-llm-pilot")
	args = append(args, "--provider-call-timeout-sec", fmt.Sprintf("%d", int(cfg.ProviderCallTimeout/time.Second)))
	args = append(args, "--planner-cycle-timeout-sec", fmt.Sprintf("%d", int(cfg.PlannerCycleTimeout/time.Second)))
	args = append(args, "--budget-account-id", cfg.BudgetAccountID)
	args = append(args, "--budget-hard-limit-micros", fmt.Sprintf("%d", cfg.BudgetHardLimitMicros))
	args = append(args, "--budget-reserve-micros", fmt.Sprintf("%d", cfg.BudgetReserveMicros))
	args = append(args, "--budget-micros-per-token", fmt.Sprintf("%d", cfg.BudgetMicrosPerToken))
	args = append(args, "--max-tool-loop-iterations", fmt.Sprintf("%d", cfg.MaxToolLoopIterations))
	args = append(args, "--max-provider-retry-attempts", fmt.Sprintf("%d", cfg.MaxProviderRetryAttempts))
	return args
}

func MaterializeManagedAgentAnatomy(record ManagedAgentRecord, cfg RuntimeConfig) (ManagedAgentAnatomyMaterialization, error) {
	record = normalizeManagedAgentRecord(record)
	cfg.ApplyDefaults()
	if strings.TrimSpace(cfg.Workdir) == "" {
		cfg.Workdir = record.Workdir
	}
	if strings.TrimSpace(cfg.AgentID) == "" {
		cfg.AgentID = record.AgentID
	}
	if strings.TrimSpace(cfg.DisplayName) == "" {
		cfg.DisplayName = record.DisplayName
	}
	if strings.TrimSpace(cfg.Role) == "" {
		cfg.Role = record.Role
	}
	if strings.TrimSpace(record.Workdir) == "" {
		return ManagedAgentAnatomyMaterialization{}, fmt.Errorf("managed agent workdir is required")
	}
	profile := runtimeProfileForAnatomy(cfg)
	defaults := LoadBotRegistry().Defaults
	anatomyPath := firstNonEmpty(record.AnatomyPath, defaults.AnatomyPath)
	anatomyPreset := firstNonEmpty(record.AnatomyPreset, defaults.AnatomyPreset)
	source := "role_default"
	var anatomy AgentAnatomyConfig
	var err error
	switch {
	case strings.TrimSpace(anatomyPath) != "":
		anatomy, err = ReadAgentAnatomyConfigFile(anatomyPath, profile)
		source = "path"
	case strings.TrimSpace(anatomyPreset) != "":
		anatomy = DefaultAgentAnatomyConfigForPreset(profile, anatomyPreset)
		source = "preset"
	case pathExists(agentAnatomyPath(record.Workdir)):
		anatomy, err = ReadAgentAnatomyConfig(record.Workdir, profile)
		source = "workdir"
	default:
		anatomy = DefaultAgentAnatomyConfig(profile)
	}
	if err != nil {
		return ManagedAgentAnatomyMaterialization{}, err
	}
	expectedDigest := firstNonEmpty(record.AnatomyDigest, defaults.AnatomyDigest)
	digest := AgentAnatomyDigest(anatomy)
	if expectedDigest != "" && expectedDigest != digest {
		return ManagedAgentAnatomyMaterialization{}, fmt.Errorf("agent anatomy digest mismatch: expected %s got %s", expectedDigest, digest)
	}
	if err := SaveAgentAnatomyConfig(record.Workdir, anatomy, profile); err != nil {
		return ManagedAgentAnatomyMaterialization{}, err
	}
	toolBundles, err := MaterializeManagedAgentToolBundles(record, anatomy)
	if err != nil {
		return ManagedAgentAnatomyMaterialization{}, err
	}
	return ManagedAgentAnatomyMaterialization{
		Path:                 agentAnatomyPath(record.Workdir),
		Source:               source,
		Preset:               anatomy.Preset,
		ProfileID:            anatomy.ProfileID,
		Digest:               digest,
		InstalledToolBundles: append([]string(nil), toolBundles.Installed...),
		SkippedToolBundles:   append([]string(nil), toolBundles.Skipped...),
	}, nil
}

func appendManagedAgentRuntimeConfigArgs(args []string, cfg RuntimeConfig) []string {
	cfg.ApplyDefaults()
	args = appendStringFlag(args, "--provider-id", cfg.ProviderID)
	args = appendStringFlag(args, "--model-override", cfg.ModelOverride)
	args = appendStringFlag(args, "--group-id", cfg.GroupID)
	args = appendStringFlag(args, "--llm-backend", cfg.LLMBackend)
	args = appendStringFlag(args, "--model", cfg.Model)
	args = appendStringFlag(args, "--coordination-mode", cfg.CoordinationMode)
	args = appendStringFlag(args, "--workspace-id", cfg.WorkspaceID)
	args = appendStringFlag(args, "--agent-id", cfg.AgentID)
	args = appendStringFlag(args, "--display-name", cfg.DisplayName)
	args = appendStringFlag(args, "--owner-user-id", cfg.OwnerUserID)
	args = appendStringFlag(args, "--role", cfg.Role)
	if len(cfg.Capabilities) > 0 {
		args = appendStringFlag(args, "--capabilities", strings.Join(cfg.Capabilities, ","))
	}
	if cfg.InboxDrainAdvisory {
		args = appendStringFlag(args, "--inbox-drain-advisory", "true")
	}
	if cfg.ToolLoopCompaction {
		args = appendStringFlag(args, "--tool-loop-compaction", "true")
	}
	return args
}

func appendStringFlag(args []string, name, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return args
	}
	return append(args, name, value)
}

func managedAgentStartRuntimeConfig(record ManagedAgentRecord) RuntimeConfig {
	record = normalizeManagedAgentRecord(record)
	registryDefaults := LoadBotRegistry().Defaults
	local := LoadLocalRuntimeProfile(record.Workdir)
	cfg := runtimeConfigFromLocalRuntimeProfile(local)
	cfg.Workdir = record.Workdir
	cfg.ProviderID = firstNonEmpty(record.ProviderID, cfg.ProviderID)
	cfg.ModelOverride = firstNonEmpty(record.ModelOverride, cfg.ModelOverride)
	cfg.GroupID = firstNonEmpty(record.GroupID, cfg.GroupID)
	cfg.LLMBackend = firstNonEmpty(record.LLMBackend, cfg.LLMBackend)
	cfg.Model = firstNonEmpty(record.Model, cfg.Model)
	cfg.CoordinationMode = firstNonEmpty(record.CoordinationMode, registryDefaults.CoordinationMode, cfg.CoordinationMode)
	cfg.WorkspaceID = firstNonEmpty(record.WorkspaceID, cfg.WorkspaceID)
	cfg.AgentID = firstNonEmpty(record.AgentID, cfg.AgentID)
	cfg.DisplayName = firstNonEmpty(record.DisplayName, cfg.DisplayName)
	cfg.OwnerUserID = firstNonEmpty(record.OwnerUserID, cfg.OwnerUserID)
	cfg.Role = firstNonEmpty(record.Role, cfg.Role)
	cfg.Capabilities = firstCapabilities(registryDefaults.Capabilities, cfg.Capabilities)
	cfg.InboxDrainAdvisory = true
	cfg.ApplyDefaults()
	cfg = normalizeManagedCodexStartRuntimeConfig(cfg)
	return cfg
}

func managedAgentEffectiveRuntimeConfig(record ManagedAgentRecord, cfg RuntimeConfig) RuntimeConfig {
	record = normalizeManagedAgentRecord(record)
	cfg.Workdir = firstNonEmpty(cfg.Workdir, record.Workdir)
	cfg.AgentID = firstNonEmpty(cfg.AgentID, record.AgentID)
	cfg.WorkspaceID = firstNonEmpty(cfg.WorkspaceID, record.WorkspaceID)
	cfg.ApplyDefaults()
	if !managedAgentStartNeedsRealPilot(cfg) {
		return cfg
	}
	cfg.RealLLMPilot = true
	cfg.DaemonWorkspaceTools = true
	providerCallTimeout := cfg.ProviderCallTimeout
	if providerCallTimeout <= 0 || providerCallTimeout < managedRealPilotDefaultProviderCallTimeout || providerCallTimeout > realLLMPilotMaxProviderCallTimeout {
		providerCallTimeout = managedRealPilotDefaultProviderCallTimeout
	}
	cfg.ProviderCallTimeout = providerCallTimeout
	cfg.PlannerCycleTimeout = providerCallTimeout
	if strings.TrimSpace(cfg.BudgetAccountID) == "" {
		cfg.BudgetAccountID = managedAgentDefaultBudgetAccountID(ManagedAgentRecord{AgentID: cfg.AgentID})
	}
	if cfg.BudgetHardLimitMicros <= 0 || cfg.BudgetHardLimitMicros < managedRealPilotDefaultBudgetHardLimitMicros {
		cfg.BudgetHardLimitMicros = managedRealPilotDefaultBudgetHardLimitMicros
	}
	if cfg.BudgetReserveMicros <= 0 || cfg.BudgetReserveMicros < managedRealPilotDefaultBudgetReserveMicros {
		cfg.BudgetReserveMicros = managedRealPilotDefaultBudgetReserveMicros
	}
	if cfg.BudgetMicrosPerToken <= 0 {
		cfg.BudgetMicrosPerToken = managedRealPilotDefaultBudgetMicrosPerToken
	}
	if cfg.MaxToolLoopIterations <= 0 || cfg.MaxToolLoopIterations > realLLMPilotMaxToolLoopIterations {
		cfg.MaxToolLoopIterations = realLLMPilotMaxToolLoopIterations
	}
	if cfg.MaxProviderRetryAttempts <= 0 || cfg.MaxProviderRetryAttempts > realLLMPilotMaxProviderRetryAttempts {
		cfg.MaxProviderRetryAttempts = realLLMPilotMaxProviderRetryAttempts
	}
	cfg.ApplyDefaults()
	return cfg
}

func managedAgentStartLocalRuntimeProfile(record ManagedAgentRecord, cfg RuntimeConfig) LocalRuntimeProfile {
	existing := LoadLocalRuntimeProfile(record.Workdir)
	profile := localRuntimeProfileFromConfig(cfg)
	profile.RegisteredExecutor = existing.RegisteredExecutor
	if strings.TrimSpace(profile.WorkspaceName) == "" {
		profile.WorkspaceName = existing.WorkspaceName
	}
	return profile
}

func normalizeManagedCodexStartRuntimeConfig(cfg RuntimeConfig) RuntimeConfig {
	if normalizeLLMBackend(cfg.LLMBackend) != llmBackendCodex && !strings.EqualFold(strings.TrimSpace(cfg.ProviderID), llmBackendCodex) {
		return cfg
	}
	cfg.Model = managedCodexCompatibleModel(cfg.Model)
	cfg.ModelOverride = managedCodexCompatibleModel(cfg.ModelOverride)
	if strings.TrimSpace(cfg.ModelOverride) != "" {
		cfg.Model = cfg.ModelOverride
	}
	return cfg
}

func managedCodexCompatibleModel(model string) string {
	model = strings.TrimSpace(model)
	switch strings.ToLower(model) {
	case "gpt-5.4":
		return defaultModel
	default:
		return model
	}
}

func managedAgentStartNeedsRealPilot(cfg RuntimeConfig) bool {
	cfg.ApplyDefaults()
	if cfg.RealLLMPilot {
		return true
	}
	providerID := strings.TrimSpace(cfg.ProviderID)
	if providerID == "" || strings.EqualFold(providerID, "fake") {
		return false
	}
	switch normalizeLLMBackend(cfg.LLMBackend) {
	case llmBackendOpenAI, llmBackendCodex, llmBackendQwen:
		return true
	default:
		return false
	}
}

func managedAgentDefaultBudgetAccountID(record ManagedAgentRecord) string {
	agentID := strings.ToLower(strings.TrimSpace(record.AgentID))
	if agentID == "" {
		return ""
	}
	var builder strings.Builder
	for _, r := range agentID {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-' || r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	return "pilot-agent-" + strings.Trim(builder.String(), "-_")
}

func managedAgentProcessMatchesRecord(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) {
	if pid <= 0 {
		return false, nil
	}
	commandLine, err := processCommandLine(pid)
	if err != nil || strings.TrimSpace(commandLine) == "" {
		if err != nil {
			return false, err
		}
		return false, errManagedAgentProcessIdentityUnavailable
	}
	return managedAgentCommandLineMatchesRecord(commandLine, state, record), nil
}

func managedAgentCommandLineMatchesRecord(commandLine string, state AgentProcessState, record ManagedAgentRecord) bool {
	if !managedAgentDaemonCommandLineLooksKillable(commandLine) {
		return false
	}
	record = normalizeManagedAgentRecord(record)
	cfg := managedAgentStartRuntimeConfig(record)
	info := runtimeIdentityLeaseInfo{
		PID:         state.PID,
		Mode:        string(RuntimeModeDaemon),
		Workdir:     firstNonEmpty(state.Workdir, record.Workdir),
		WorkspaceID: cfg.WorkspaceID,
		AgentID:     cfg.AgentID,
	}
	if !runtimeIdentityLeaseCommandLineMatches(info, commandLine) {
		return false
	}
	return managedAgentCommandLineContainsLaunchArgs(commandLine, state.Args)
}

func managedAgentCommandLineContainsLaunchArgs(commandLine string, args []string) bool {
	if len(args) == 0 {
		return true
	}
	normalizedCommand := runtimeIdentityNormalizeCommandLine(commandLine)
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue
		}
		if !strings.Contains(normalizedCommand, runtimeIdentityNormalizeCommandLine(arg)) {
			return false
		}
	}
	return true
}

func managedAgentProcessProvenanceUnknownReasons(state AgentProcessState) []string {
	reasons := []string{}
	if strings.TrimSpace(state.Executable) == "" || strings.TrimSpace(state.ExecutableSHA256) == "" {
		reasons = append(reasons, "missing_executable_identity")
	}
	if strings.TrimSpace(state.ArgsDigest) == "" || len(state.Args) == 0 {
		reasons = append(reasons, "missing_launch_args_identity")
	}
	if strings.TrimSpace(state.RuntimeConfigDigest) == "" {
		reasons = append(reasons, "missing_runtime_config_identity")
	}
	sort.Strings(reasons)
	return reasons
}

func managedAgentProcessProvenanceDriftReasons(state AgentProcessState, record ManagedAgentRecord) []string {
	reasons := []string{}
	if expected := strings.TrimSpace(state.ExecutableSHA256); expected != "" {
		current, err := managerFileSHA256(state.Executable)
		if err != nil {
			reasons = append(reasons, "executable_hash_unreadable")
		} else if current != expected {
			reasons = append(reasons, "executable_hash_drift")
		}
	}
	if expected := strings.TrimSpace(state.ArgsDigest); expected != "" {
		if current := managedAgentArgsDigest(managedAgentDaemonArgs(record)); current != expected {
			reasons = append(reasons, "launch_args_drift")
		}
	}
	if expected := strings.TrimSpace(state.RuntimeConfigDigest); expected != "" {
		if current := managedAgentRuntimeConfigDigest(managedAgentStartRuntimeConfig(record)); current != expected {
			reasons = append(reasons, "runtime_config_drift")
		}
	}
	if expected := strings.TrimSpace(state.BuildRevision); expected != "" {
		if current := strings.TrimSpace(managerCurrentRuntimeBuildIdentity().VCSRevision); current != "" && current != expected {
			reasons = append(reasons, "build_revision_drift")
		}
	}
	sort.Strings(reasons)
	return reasons
}

func managedAgentDaemonCommandLineLooksKillable(commandLine string) bool {
	normalized := " " + runtimeIdentityNormalizeCommandLine(commandLine) + " "
	return strings.Contains(normalized, "rhizome-bot") &&
		strings.Contains(normalized, " daemon ") &&
		strings.Contains(normalized, "--agent-id") &&
		strings.Contains(normalized, "--workdir")
}

func StopManagedAgent(record ManagedAgentRecord) error {
	record = normalizeManagedAgentRecord(record)
	state := LoadAgentProcessState(record.Workdir)
	if state.PID <= 0 {
		return finishStoppedManagedAgent(record, 0)
	}
	if managedAgentProcessTreeKnown(state.PID) {
		if err := requestManagedAgentGracefulStop(record.Workdir); err != nil {
			return fmt.Errorf("stop agent %s: request graceful stop: %w", record.AgentID, err)
		}
		if !managedAgentForceStopAllowed() {
			if err := waitForProcessExit(state.PID, managedAgentStopExitTimeout); err != nil {
				return fmt.Errorf("stop agent %s: graceful stop requested but pid %d did not exit before timeout; force stop disabled by RHIZOME_MANAGED_AGENT_ALLOW_FORCE_STOP=0", record.AgentID, state.PID)
			}
		} else if killErr := managedAgentKillProcessFunc(state.PID); killErr != nil {
			return fmt.Errorf("stop agent %s: force stop known managed process tree failed: %w", record.AgentID, killErr)
		}
		if err := waitForProcessExit(state.PID, managedAgentStopExitTimeout); err != nil {
			return fmt.Errorf("stop agent %s: %w", record.AgentID, err)
		}
		return finishStoppedManagedAgent(record, state.PID)
	}
	ok, err := managedAgentProcessExistsFunc(state.PID)
	if err != nil {
		return fmt.Errorf("inspect agent %s: %w", record.AgentID, err)
	}
	if !ok {
		if managedAgentProcessTreeKnown(state.PID) {
			if killErr := managedAgentKillProcessFunc(state.PID); killErr != nil {
				return fmt.Errorf("stop agent %s: cleanup known managed process tree after missing root failed: %w", record.AgentID, killErr)
			}
		} else {
			releaseManagedAgentProcessResources(state.PID)
		}
		return finishStoppedManagedAgent(record, state.PID)
	}
	if matches, err := managedAgentProcessMatchesFunc(state.PID, state, record); err != nil {
		return fmt.Errorf("inspect agent %s pid %d identity: %w", record.AgentID, state.PID, err)
	} else if !matches {
		if managedAgentProcessTreeKnown(state.PID) {
			if killErr := managedAgentKillProcessFunc(state.PID); killErr != nil {
				return fmt.Errorf("stop agent %s: mismatched known managed process tree force stop failed: %w", record.AgentID, killErr)
			}
		} else {
			releaseManagedAgentProcessResources(state.PID)
		}
		return RemoveAgentProcessState(record.Workdir)
	}
	if err := requestManagedAgentGracefulStop(record.Workdir); err != nil {
		return fmt.Errorf("stop agent %s: request graceful stop: %w", record.AgentID, err)
	}
	exitTimeout := managedAgentStopExitTimeout
	if managedAgentForceStopAllowed() && exitTimeout > managedAgentKnownProcessTreeGraceTimeout {
		exitTimeout = managedAgentKnownProcessTreeGraceTimeout
	}
	if err := waitForProcessExit(state.PID, exitTimeout); err != nil {
		if !managedAgentForceStopAllowed() {
			return fmt.Errorf("stop agent %s: graceful stop requested but pid %d did not exit before timeout; force stop disabled by RHIZOME_MANAGED_AGENT_ALLOW_FORCE_STOP=0", record.AgentID, state.PID)
		}
		if killErr := managedAgentKillProcessFunc(state.PID); killErr != nil {
			return fmt.Errorf("stop agent %s: graceful stop timed out and force stop failed: %w", record.AgentID, killErr)
		}
		if waitErr := waitForProcessExit(state.PID, managedAgentStopExitTimeout); waitErr != nil {
			return fmt.Errorf("stop agent %s: %w", record.AgentID, waitErr)
		}
	}
	return finishStoppedManagedAgent(record, state.PID)
}

func RequestManagedAgentGracefulStop(record ManagedAgentRecord) (bool, error) {
	record = normalizeManagedAgentRecord(record)
	state := LoadAgentProcessState(record.Workdir)
	if state.PID <= 0 {
		if err := cleanupManagedAgentWorkdirProcessesAfterStop(record); err != nil {
			return true, err
		}
		return true, RemoveAgentProcessState(record.Workdir)
	}
	ok, err := managedAgentProcessExistsFunc(state.PID)
	if err != nil {
		return false, fmt.Errorf("inspect agent %s: %w", record.AgentID, err)
	}
	if !ok {
		releaseManagedAgentProcessResources(state.PID)
		return true, finishStoppedManagedAgent(record, state.PID)
	}
	if matches, err := managedAgentProcessMatchesFunc(state.PID, state, record); err != nil {
		return false, fmt.Errorf("inspect agent %s pid %d identity: %w", record.AgentID, state.PID, err)
	} else if !matches {
		releaseManagedAgentProcessResources(state.PID)
		return true, RemoveAgentProcessState(record.Workdir)
	}
	if err := requestManagedAgentGracefulStop(record.Workdir); err != nil {
		return false, fmt.Errorf("stop agent %s: request graceful stop: %w", record.AgentID, err)
	}
	if err := waitForProcessExit(state.PID, managedAgentStopExitTimeout); err == nil {
		return true, finishStoppedManagedAgent(record, state.PID)
	}
	return false, nil
}

func finishStoppedManagedAgent(record ManagedAgentRecord, pid int) error {
	var errs []error
	if pid > 0 {
		releaseManagedAgentProcessResources(pid)
	}
	clearManagedAgentGracefulStop(record.Workdir)
	if err := RemoveAgentProcessState(record.Workdir); err != nil {
		errs = append(errs, err)
	}
	if err := cleanupManagedAgentWorkdirProcessesAfterStop(record); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func managedAgentForceStopAllowed() bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_MANAGED_AGENT_ALLOW_FORCE_STOP")))
	switch raw {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func cleanupManagedAgentWorkdirProcessesAfterStop(record ManagedAgentRecord) error {
	if managedAgentCleanupWorkdirFunc == nil {
		if err := cleanupManagedAgentBrowserSessionState(record); err != nil {
			return err
		}
		return nil
	}
	if strings.TrimSpace(record.Workdir) != "" {
		note, err := managedAgentCleanupWorkdirFunc(record.Workdir)
		if err != nil {
			return fmt.Errorf("agent %s: cleanup workdir processes: %w", strings.TrimSpace(record.AgentID), err)
		}
		if strings.TrimSpace(note) != "" {
			fmt.Fprintf(os.Stderr, "[managed-agent cleanup] %s: %s\n", strings.TrimSpace(record.AgentID), strings.TrimSpace(note))
		}
	}
	if err := cleanupManagedAgentBrowserSessionState(record); err != nil {
		return err
	}
	return nil
}

func managedAgentWorkdirProcessCleanupRoots(workdir string) []string {
	cleaned := strings.TrimSpace(workdir)
	if cleaned == "" {
		return nil
	}
	cleaned = filepath.Clean(cleaned)
	if abs, err := filepath.Abs(cleaned); err == nil {
		cleaned = filepath.Clean(abs)
	}
	var roots []string
	for _, name := range []string{
		filepath.Join(".runtime-config", "browser-sessions"),
		"project-checkouts",
		"project-remotes",
		"project-integration",
	} {
		path := filepath.Join(cleaned, name)
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			roots = append(roots, path)
		}
	}
	if len(roots) > 0 {
		roots = append([]string{cleaned}, roots...)
	}
	return dedupeShellCleanupRoots(roots)
}

func cleanupManagedAgentBrowserSessionState(record ManagedAgentRecord) error {
	workdir := strings.TrimSpace(record.Workdir)
	if workdir == "" {
		return nil
	}
	root, ok, err := managedAgentBrowserSessionRoot(workdir)
	if err != nil {
		return fmt.Errorf("agent %s: resolve browser session cleanup root: %w", strings.TrimSpace(record.AgentID), err)
	}
	if !ok {
		return nil
	}
	if info, statErr := os.Stat(root); statErr != nil {
		if os.IsNotExist(statErr) {
			return nil
		}
		return fmt.Errorf("agent %s: inspect browser session cleanup root: %w", strings.TrimSpace(record.AgentID), statErr)
	} else if !info.IsDir() {
		return fmt.Errorf("agent %s: browser session cleanup root is not a directory: %s", strings.TrimSpace(record.AgentID), root)
	}
	if err := os.RemoveAll(root); err != nil {
		return fmt.Errorf("agent %s: remove browser session state: %w", strings.TrimSpace(record.AgentID), err)
	}
	fmt.Fprintf(os.Stderr, "[managed-agent cleanup] %s: removed browser_session state root %s\n", strings.TrimSpace(record.AgentID), root)
	return nil
}

func managedAgentBrowserSessionRoot(workdir string) (string, bool, error) {
	cleaned := strings.TrimSpace(workdir)
	if cleaned == "" {
		return "", false, nil
	}
	absWorkdir, err := filepath.Abs(filepath.Clean(cleaned))
	if err != nil {
		return "", false, err
	}
	root := filepath.Join(absWorkdir, ".runtime-config", "browser-sessions")
	rel, err := filepath.Rel(absWorkdir, root)
	if err != nil {
		return "", false, err
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", false, fmt.Errorf("browser session root escapes workdir: %s", root)
	}
	return root, true, nil
}

func TailManagedAgentLogs(record ManagedAgentRecord, lines int) (ManagedAgentLogTail, error) {
	record = normalizeManagedAgentRecord(record)
	if lines <= 0 {
		lines = 40
	}
	state := LoadAgentProcessState(record.Workdir)
	outPath := state.LogOutPath
	errPath := state.LogErrPath
	if outPath == "" {
		outPath, _ = resolvedManagedAgentLogPaths(record.Workdir)
	}
	if errPath == "" {
		_, errPath = resolvedManagedAgentLogPaths(record.Workdir)
	}
	stdoutTail, err := tailLogFile(outPath, lines)
	if err != nil {
		return ManagedAgentLogTail{}, err
	}
	stderrTail, err := tailLogFile(errPath, lines)
	if err != nil {
		return ManagedAgentLogTail{}, err
	}
	return ManagedAgentLogTail{
		Stdout:     stdoutTail,
		Stderr:     stderrTail,
		LogOutPath: outPath,
		LogErrPath: errPath,
	}, nil
}

func openProcessLog(path string) (*os.File, error) {
	if err := fssecure.EnsurePrivateDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	file, err := fssecure.OpenPrivateFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY)
	if err != nil {
		return nil, fmt.Errorf("open log %s: %w", path, err)
	}
	return file, nil
}

func privateManagedAgentLogPaths(workdir string) (string, string) {
	dir := filepath.Join(workdir, agentProcessLogDirname)
	return filepath.Join(dir, "agent.out.log"), filepath.Join(dir, "agent.err.log")
}

func resolvedManagedAgentLogPaths(workdir string) (string, string) {
	legacyOut := filepath.Join(workdir, "agent.out.log")
	legacyErr := filepath.Join(workdir, "agent.err.log")
	if pathExists(legacyOut) || pathExists(legacyErr) {
		return legacyOut, legacyErr
	}
	return privateManagedAgentLogPaths(workdir)
}

func closeIfPossible(closer io.Closer) {
	if closer != nil {
		_ = closer.Close()
	}
}

func tailLogFile(path string, lines int) ([]string, error) {
	if lines <= 0 {
		lines = 40
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var all []string
	for scanner.Scan() {
		all = append(all, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(all) <= lines {
		return all, nil
	}
	return append([]string(nil), all[len(all)-lines:]...), nil
}

func waitForProcessExit(pid int, timeout time.Duration) error {
	if pid <= 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		ok, err := managedAgentProcessExistsFunc(pid)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("process %d did not exit after %s", pid, timeout)
		}
		time.Sleep(managedAgentProcessExitPollGap)
	}
}

func rollbackStartedManagedAgentProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	var errs []error
	if err := managedAgentKillProcessFunc(pid); err != nil {
		errs = append(errs, fmt.Errorf("terminate pid %d: %w", pid, err))
	}
	if err := waitForProcessExit(pid, managedAgentStopExitTimeout); err != nil {
		errs = append(errs, fmt.Errorf("wait for pid %d exit: %w", pid, err))
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
