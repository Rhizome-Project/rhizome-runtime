package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

func testManagerHome(t *testing.T) string {
	t.Helper()

	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	return home
}

func TestPrintManagerUsageIncludesCoreCommands(t *testing.T) {
	var buf bytes.Buffer

	printManagerUsage(&buf)

	got := buf.String()
	for _, want := range []string{
		"usage: " + appCommandName + " [manager]",
		appCommandName + " onboard [--workdir PATH]",
		"list | show <agent> | start <agent> | stop <agent> | restart <agent> | status <agent> | logs <agent>",
		appCommandName + " chat <agent> | attach <agent> | install [--dir PATH] [--force]",
		"defaults | set-default <field> <value> | clear-default <field>",
		"global agent manager TUI",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected usage output to contain %q, got:\n%s", want, got)
		}
	}
}

func TestSplitManagerCommandHonorsQuotedArguments(t *testing.T) {
	fields, err := splitManagerCommand(`onboard "C:\fixtures\agents\lyrica two"`)
	if err != nil {
		t.Fatalf("splitManagerCommand() error: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %#v", fields)
	}
	if fields[1] != `C:\fixtures\agents\lyrica two` {
		t.Fatalf("unexpected quoted field: %#v", fields)
	}
}

func TestSplitManagerCommandHonorsSingleQuotedArguments(t *testing.T) {
	fields, err := splitManagerCommand(`attach 'lyrica folder'`)
	if err != nil {
		t.Fatalf("splitManagerCommand() error: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %#v", fields)
	}
	if fields[1] != "lyrica folder" {
		t.Fatalf("unexpected single-quoted field: %#v", fields)
	}
}

func TestManagerTUIManagedRunPreflightOptionsParsesResumeWaivers(t *testing.T) {
	options := managerTUIManagedRunPreflightOptions([]string{"resume"})
	waiver := options.resumeContinuationWaiver()
	if !waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
		t.Fatalf("resume shorthand should enable continuation waivers, got %+v", options)
	}

	options = managerTUIManagedRunPreflightOptions([]string{"--resume-dirty-project-checkout"})
	waiver = options.resumeContinuationWaiver()
	if !waiver.AllowDirtyProjectCheckout || waiver.AllowLivePatchQueue || waiver.AllowAgentRequests || waiver.AllowLiveProjectBranches || waiver.AllowPendingResumeTriggers {
		t.Fatalf("dirty checkout waiver should not imply patch queue waiver, got %+v", options)
	}

	options = managerTUIManagedRunPreflightOptions([]string{"--allow-live-patch-queue"})
	waiver = options.resumeContinuationWaiver()
	if waiver.AllowDirtyProjectCheckout || !waiver.AllowLivePatchQueue || waiver.AllowAgentRequests || waiver.AllowLiveProjectBranches || waiver.AllowPendingResumeTriggers {
		t.Fatalf("live patch queue waiver should not imply dirty checkout waiver, got %+v", options)
	}

	options = managerTUIManagedRunPreflightOptions([]string{"--resume-agent-requests"})
	waiver = options.resumeContinuationWaiver()
	if waiver.AllowDirtyProjectCheckout || waiver.AllowLivePatchQueue || !waiver.AllowAgentRequests || waiver.AllowLiveProjectBranches || waiver.AllowPendingResumeTriggers {
		t.Fatalf("agent request waiver should not imply other continuation waivers, got %+v", options)
	}

	options = managerTUIManagedRunPreflightOptions([]string{"--resume-live-project-branches"})
	waiver = options.resumeContinuationWaiver()
	if waiver.AllowDirtyProjectCheckout || waiver.AllowLivePatchQueue || waiver.AllowAgentRequests || !waiver.AllowLiveProjectBranches || waiver.AllowPendingResumeTriggers {
		t.Fatalf("live project branch waiver should not imply other continuation waivers, got %+v", options)
	}

	options = managerTUIManagedRunPreflightOptions([]string{"--resume-pending-triggers"})
	waiver = options.resumeContinuationWaiver()
	if waiver.AllowDirtyProjectCheckout || waiver.AllowLivePatchQueue || waiver.AllowAgentRequests || waiver.AllowLiveProjectBranches || !waiver.AllowPendingResumeTriggers {
		t.Fatalf("pending trigger waiver should not imply other continuation waivers, got %+v", options)
	}
}

func TestManagerUIResolveAgentRefMatchesIndexIDDisplayNameAndFolderBase(t *testing.T) {
	testManagerHome(t)

	root := t.TempDir()
	alphaDir := filepath.Join(root, "alpha-folder")
	lyricaDir := filepath.Join(root, "lyrica-folder")
	if err := os.MkdirAll(alphaDir, 0o755); err != nil {
		t.Fatalf("mkdir alpha dir: %v", err)
	}
	if err := os.MkdirAll(lyricaDir, 0o755); err != nil {
		t.Fatalf("mkdir lyrica dir: %v", err)
	}

	for _, record := range []ManagedAgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Workdir: alphaDir, HostURL: "https://rhizome.test", WorkspaceID: "rhizome-main"},
		{AgentID: "lyrica", DisplayName: "Lyrica", Workdir: lyricaDir, HostURL: "https://rhizome.test", WorkspaceID: "rhizome-main"},
	} {
		if err := UpsertManagedAgent(record); err != nil {
			t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
		}
	}

	ui := &ManagerUI{}

	record, err := ui.resolveAgentRef([]string{"show", "1"})
	if err != nil {
		t.Fatalf("resolveAgentRef(index) error: %v", err)
	}
	if record.AgentID != "alpha" {
		t.Fatalf("expected index lookup to return alpha, got %+v", record)
	}

	record, err = ui.resolveAgentRef([]string{"show", "lyrica"})
	if err != nil {
		t.Fatalf("resolveAgentRef(agent id) error: %v", err)
	}
	if record.AgentID != "lyrica" {
		t.Fatalf("expected agent id lookup to return lyrica, got %+v", record)
	}

	record, err = ui.resolveAgentRef([]string{"show", "Lyrica"})
	if err != nil {
		t.Fatalf("resolveAgentRef(display name) error: %v", err)
	}
	if record.AgentID != "lyrica" {
		t.Fatalf("expected display-name lookup to return lyrica, got %+v", record)
	}

	record, err = ui.resolveAgentRef([]string{"show", filepath.Base(lyricaDir)})
	if err != nil {
		t.Fatalf("resolveAgentRef(folder base) error: %v", err)
	}
	if record.AgentID != "lyrica" {
		t.Fatalf("expected folder-base lookup to return lyrica, got %+v", record)
	}
}

func TestManagerUIHandleCommandUpdatesDefaults(t *testing.T) {
	testManagerHome(t)

	var out bytes.Buffer
	ui := &ManagerUI{out: &out}

	if exit, err := ui.handleCommand(context.Background(), "set-default host https://rhizome.example"); err != nil {
		t.Fatalf("set-default command error: %v", err)
	} else if exit {
		t.Fatal("set-default should not exit")
	}

	registry := LoadBotRegistry()
	if registry.Defaults.HostURL != "https://rhizome.example" {
		t.Fatalf("expected host default to update, got %q", registry.Defaults.HostURL)
	}
	if got := out.String(); !strings.Contains(got, "host_url: https://rhizome.example") {
		t.Fatalf("expected defaults output after set-default, got:\n%s", got)
	}

	out.Reset()
	if exit, err := ui.handleCommand(context.Background(), "clear-default host"); err != nil {
		t.Fatalf("clear-default command error: %v", err)
	} else if exit {
		t.Fatal("clear-default should not exit")
	}

	registry = LoadBotRegistry()
	if registry.Defaults.HostURL != defaultRhizomeHostURL {
		t.Fatalf("expected host default to fall back to built-in default, got %q", registry.Defaults.HostURL)
	}
	if got := out.String(); !strings.Contains(got, "host_url: "+defaultRhizomeHostURL) {
		t.Fatalf("expected defaults output after clear-default, got:\n%s", got)
	}
}

func TestManagerUIPrintAgentTableReflectsRuntimeStatuses(t *testing.T) {
	testManagerHome(t)
	origMatches := managedAgentProcessMatchesFunc
	defer func() { managedAgentProcessMatchesFunc = origMatches }()
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) {
		return record.AgentID == "beta", nil
	}

	root := t.TempDir()
	runningDir := filepath.Join(root, "running-agent")
	staleDir := filepath.Join(root, "stale-agent")
	stoppedDir := filepath.Join(root, "stopped-agent")
	for _, dir := range []string{runningDir, staleDir, stoppedDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	currentPID := os.Getpid()
	records := []ManagedAgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Workdir: stoppedDir, HostURL: "https://rhizome.test", WorkspaceID: "rhizome-main"},
		{AgentID: "beta", DisplayName: "Beta", Workdir: runningDir, HostURL: "https://rhizome.test", WorkspaceID: "rhizome-main"},
		{AgentID: "gamma", DisplayName: "Gamma", Workdir: staleDir, HostURL: "https://rhizome.test", WorkspaceID: "rhizome-main"},
	}
	saveTrustedAgentProcessStateForTest(t, records[1], currentPID)
	saveTrustedAgentProcessStateForTest(t, records[2], currentPID+100000)

	for _, record := range records {
		if err := UpsertManagedAgent(record); err != nil {
			t.Fatalf("UpsertManagedAgent(%s) error: %v", record.AgentID, err)
		}
	}

	var out bytes.Buffer
	ui := &ManagerUI{out: &out}
	ui.printAgentTable()

	got := out.String()
	for _, want := range []string{
		"system> agents:",
		"alpha [stopped]",
		"beta [running]",
		"gamma [stale]",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected agent table to contain %q, got:\n%s", want, got)
		}
	}
}

func TestManagerUIPrintAgentDetailsPrefersRegisteredExecutorIdentity(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "agent-stale",
		DisplayName: "Registry Name",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-registry",
		Role:        "registry-role",
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-stale",
		DisplayName: "Display Stale",
		Role:        "generalist",
		RegisteredExecutor: RegisteredExecutorIdentity{
			AgentID:     "agent-registered",
			WorkspaceID: "ws-registered",
			DisplayName: "Display Registered",
			Role:        "reviewer",
		},
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	var out bytes.Buffer
	ui := &ManagerUI{out: &out}
	ui.printAgentDetails(record)

	got := out.String()
	for _, want := range []string{
		"system> agent agent-registered",
		"- display_name: Display Registered",
		"- workspace_id: ws-registered",
		"- role: reviewer",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected agent details to contain %q, got:\n%s", want, got)
		}
	}
}

func TestManagerVisorRunRestartSelectedFailureIncludesCurrentProcessState(t *testing.T) {
	testManagerHome(t)
	bin := writeExecutableForSubstrateTest(t, t.TempDir(), "rhizome-bot", "same binary")
	installCleanManagerSubstrateStubsForTest(t, bin, bin)
	t.Setenv("RHIZOME_MANAGED_RPC_CONTRACT_SMOKE_LOCAL", "1")

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "rpc.methods.list":
			methods := []map[string]string{}
			for _, method := range managerRPCContractRequiredMethods() {
				methods = append(methods, map[string]string{"method": method, "description": "available"})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"count": len(methods), "methods": methods},
			})
		case "rpc.describe":
			params, _ := req["params"].(map[string]any)
			described, _ := params["method"].(string)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"method": described, "description": "schema", "params": managerRPCContractTestParamsSchema(described)},
			})
		case "runtime.build.info":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"vcs_revision": "abc123", "vcs_modified": false, "go_version": "go1.26.1", "binary_sha256": "deadbeef"},
			})
		case "agent.request.open.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"requests": []map[string]any{}},
			})
		case "agent.request":
			params, _ := req["params"].(map[string]any)
			requestID := "req-control"
			if params["method"] == "runtime.status" {
				requestID = "req-status"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   requestID,
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			params, _ := req["params"].(map[string]any)
			requestID, _ := params["request_id"].(string)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   requestID,
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "COMPLETED",
					"response": mustJSON(t, map[string]any{
						"summary":       "steady",
						"paused":        false,
						"attachable":    true,
						"process_state": "running",
					}),
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "project.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"projects": []map[string]any{}},
			})
		case "workspace.sessions.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"sessions": []map[string]any{}},
			})
		case "agent.state.get":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"value": "{}"},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origStart := managedAgentStartProcessFunc
	origSave := managedAgentSaveStateFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentStartProcessFunc = origStart
		managedAgentSaveStateFunc = origSave
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	callCount := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		callCount++
		switch callCount {
		case 1, 2:
			return true, nil
		default:
			return false, nil
		}
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStartProcessFunc = func(executablePath string, args []string, workdir string, env []string, stdout, stderr io.Writer) (int, error) {
		return 0, errors.New("spawn failed")
	}
	managedAgentSaveStateFunc = SaveAgentProcessState
	managedAgentStopExitTimeout = 5 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	model := managerVisorModel{
		ctx:      context.Background(),
		registry: BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected: 0,
	}

	cmd := model.runRestartSelected()
	if cmd == nil {
		t.Fatal("expected restart action to produce a tea.Cmd")
	}
	msgAny := cmd()
	msg, ok := msgAny.(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from restart action, got %T", msgAny)
	}
	if !msg.refresh {
		t.Fatalf("expected restart failure to request refresh, got %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" {
		t.Fatalf("expected restart failure to include refreshed live snapshot, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected restart failure to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected restart failure to include refreshed tension catalog, got %+v", msg.catalog)
	}
	if !strings.Contains(msg.text, "restart agent lyrica stopped current process but failed to start replacement") || !strings.Contains(msg.text, "spawn failed") {
		t.Fatalf("expected explicit restart failure in message, got %+v", msg)
	}
	if !strings.Contains(msg.text, "process: stopped") {
		t.Fatalf("expected restart failure message to include current process state, got %+v", msg)
	}
	if killedPID != 0 {
		t.Fatalf("expected restart to stop existing pid gracefully without force kill, got %d", killedPID)
	}
	if _, err := os.Stat(agentProcessStatePath(workdir)); !os.IsNotExist(err) {
		t.Fatalf("expected failed replacement restart to leave no process state file, stat err=%v", err)
	}
}

func TestManagerVisorRunStopSelectedSuccessIncludesRefreshedStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	saveTrustedAgentProcessStateForTest(t, record, 1234)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	origKill := managedAgentKillProcessFunc
	origTimeout := managedAgentStopExitTimeout
	origPollGap := managedAgentProcessExitPollGap
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
		managedAgentKillProcessFunc = origKill
		managedAgentStopExitTimeout = origTimeout
		managedAgentProcessExitPollGap = origPollGap
	}()

	callCount := 0
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		callCount++
		if callCount == 1 {
			return true, nil
		}
		return false, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}
	killedPID := 0
	managedAgentKillProcessFunc = func(pid int) error {
		killedPID = pid
		return nil
	}
	managedAgentStopExitTimeout = 10 * time.Millisecond
	managedAgentProcessExitPollGap = 1 * time.Millisecond

	model := managerVisorModel{
		ctx:      context.Background(),
		registry: BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected: 0,
	}

	cmd := model.runStopSelected()
	if cmd == nil {
		t.Fatal("expected stop action to produce a tea.Cmd")
	}
	msgAny := cmd()
	msg, ok := msgAny.(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from stop action, got %T", msgAny)
	}
	if !msg.refresh || !strings.Contains(msg.text, "stopped lyrica") {
		t.Fatalf("expected successful stop action message, got %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" {
		t.Fatalf("expected stop success to include refreshed live snapshot, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected stop success to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected stop success to include refreshed tension catalog, got %+v", msg.catalog)
	}
	if killedPID != 0 {
		t.Fatalf("expected stop to use graceful request without force kill, got %d", killedPID)
	}
}

func TestManagerVisorRunControlSelectedFailureIncludesRequestContext(t *testing.T) {
	testManagerHome(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		params, _ := req["params"].(map[string]any)
		switch method {
		case "agent.request":
			requestID := "req-failed"
			if params["method"] == "runtime.status" {
				requestID = "req-status"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   requestID,
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			requestID, _ := params["request_id"].(string)
			result := map[string]any{
				"request_id":   requestID,
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-1",
			}
			switch requestID {
			case "req-failed":
				result["status"] = "FAILED"
				result["response"] = `{"error":"runtime paused and cannot switch task"}`
			case "req-status":
				result["status"] = "COMPLETED"
				result["response"] = mustJSON(t, map[string]any{
					"summary":    "steady",
					"paused":     false,
					"attachable": true,
					"control":    map[string]any{"mode": "live", "last_action": "status"},
				})
			default:
				t.Fatalf("unexpected request_id %q", requestID)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  result,
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	model := managerVisorModel{
		ctx:      context.Background(),
		registry: BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected: 0,
	}

	cmd := model.runControlSelected("runtime.switch_task", map[string]any{"task_id": "task-2"})
	if cmd == nil {
		t.Fatal("expected control action to produce a tea.Cmd")
	}
	msgAny := cmd()
	msg, ok := msgAny.(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from control action, got %T", msgAny)
	}
	if !msg.refresh {
		t.Fatalf("expected control failure to request refresh, got %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" {
		t.Fatalf("expected control failure to preserve refreshed stopped process snapshot, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected control failure to preserve refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected control failure to preserve refreshed tension catalog, got %+v", msg.catalog)
	}
	for _, want := range []string{
		"request req-failed finished with status FAILED",
		"request_id=req-failed",
		"status=FAILED",
		"runtime paused and cannot switch task",
	} {
		if !strings.Contains(msg.text, want) {
			t.Fatalf("expected control failure message to include %q, got %+v", want, msg)
		}
	}
}

func TestManagerVisorRunControlSelectedSuccessIncludesRefreshedCatalog(t *testing.T) {
	testManagerHome(t)
	origMatches := managedAgentProcessMatchesFunc
	defer func() { managedAgentProcessMatchesFunc = origMatches }()
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, record ManagedAgentRecord) (bool, error) {
		return true, nil
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		params, _ := req["params"].(map[string]any)
		switch method {
		case "agent.request":
			requestID := "req-switch"
			if params["method"] == "runtime.status" {
				requestID = "req-status"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   requestID,
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			requestID, _ := params["request_id"].(string)
			result := map[string]any{
				"request_id":   requestID,
				"workspace_id": "ws-1",
				"to_agent_id":  "agent-1",
			}
			switch requestID {
			case "req-switch":
				result["status"] = "COMPLETED"
				result["response"] = mustJSON(t, map[string]any{
					"summary": "task switch requested",
					"task_id": "task-2",
				})
			case "req-status":
				result["status"] = "COMPLETED"
				result["response"] = mustJSON(t, map[string]any{
					"summary":    "steady",
					"paused":     false,
					"attachable": true,
					"task_id":    "task-2",
					"session_id": "session-9",
					"control":    map[string]any{"mode": "live", "last_action": "status"},
				})
			default:
				t.Fatalf("unexpected request_id %q", requestID)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  result,
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "RUNNING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-2", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	record := ManagedAgentRecord{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	saveTrustedAgentProcessStateForTest(t, record, os.Getpid())

	model := managerVisorModel{
		ctx:      context.Background(),
		registry: BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected: 0,
	}

	cmd := model.runControlSelected("runtime.switch_task", map[string]any{"task_id": "task-2"})
	if cmd == nil {
		t.Fatal("expected control action to produce a tea.Cmd")
	}
	msgAny := cmd()
	msg, ok := msgAny.(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from control action, got %T", msgAny)
	}
	if !msg.refresh {
		t.Fatalf("expected control success to request refresh, got %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "running" || msg.live.Summary != "steady" {
		t.Fatalf("expected control success to include refreshed live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected control success to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-2" {
		t.Fatalf("expected control success to include refreshed tension catalog, got %+v", msg.catalog)
	}
	if !strings.Contains(msg.text, "runtime.switch_task") || !strings.Contains(msg.text, "task switch requested") {
		t.Fatalf("expected control success message to preserve switch summary, got %+v", msg)
	}
}

func TestManagerVisorViewPrefersRegisteredExecutorIdentityInSelectedPanel(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "agent-stale",
		DisplayName: "Registry Name",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-registry",
		Role:        "registry-role",
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-stale",
		DisplayName: "Display Stale",
		Role:        "generalist",
		RegisteredExecutor: RegisteredExecutorIdentity{
			AgentID:     "agent-registered",
			WorkspaceID: "ws-registered",
			DisplayName: "Display Registered",
			Role:        "reviewer",
		},
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx:      context.Background(),
		registry: BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected: 0,
		width:    120,
		height:   40,
	}

	got := model.View()
	for _, want := range []string{
		"selected:",
		"  agent: agent-registered",
		"  display: Display Registered",
		"  workspace: ws-registered",
		"  role: reviewer",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected manager visor view to contain %q, got:\n%s", want, got)
		}
	}
}

func TestDecodeManagerLiveRuntimeStatus(t *testing.T) {
	raw := `{
	  "status":"ok",
	  "summary":"steady",
	  "paused":true,
	  "attachable":true,
	  "task_id":"task-1",
	  "session_id":"session-1",
	  "control":{"mode":"paused","last_action":"pause","last_action_reason":"operator pause"},
	  "work_trigger":{"trigger":"runtime_resume"},
	  "work_packet":{
	    "work_type":"profile_gate_closed",
	    "why_now":"default_work_mode_observer",
	    "gate":{
	      "gate_state":"closed",
	      "gate_type":"profile_autonomous_execution",
	      "summary":"Agent profile default_work_mode is observer."
	    }
	  },
	  "focus":{"task_id":"task-1","focus_tension_id":"tension-1","proto_cluster_id":"cluster-1"}
	}`

	got := decodeManagerLiveRuntimeStatus(raw, "running")

	if got.ProcessState != "running" || got.Status != "ok" || !got.Paused || !got.Attachable {
		t.Fatalf("unexpected live runtime status: %+v", got)
	}
	if got.ActiveTaskID != "task-1" || got.ActiveSessionID != "session-1" {
		t.Fatalf("unexpected active task/session: %+v", got)
	}
	if got.ControlMode != "paused" || got.LastAction != "pause" || got.LastReason != "operator pause" {
		t.Fatalf("unexpected control block: %+v", got)
	}
	if got.Trigger != "runtime_resume" {
		t.Fatalf("unexpected trigger: %+v", got)
	}
	if got.WorkType != "profile_gate_closed" || got.WorkGateState != "closed" || got.WorkGateType != "profile_autonomous_execution" {
		t.Fatalf("unexpected work gate block: %+v", got)
	}
	if got.WorkGateReason != "default_work_mode_observer" || got.WorkGateSummary != "Agent profile default_work_mode is observer." {
		t.Fatalf("unexpected work gate diagnostic: %+v", got)
	}
	if got.FocusTaskID != "task-1" || got.FocusTensionID != "tension-1" || got.FocusClusterID != "cluster-1" {
		t.Fatalf("unexpected focus block: %+v", got)
	}

	rendered := renderLiveRuntimeStatus(&got)
	for _, want := range []string{
		"work: profile_gate_closed",
		"work gate: closed / profile_autonomous_execution",
		"work gate reason: default_work_mode_observer",
		"work gate summary: Agent profile default_work_mode is observer.",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected rendered live status to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestLoadManagerLiveRuntimeStatusSurfacesFailedRequestContext(t *testing.T) {
	testManagerHome(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		params, _ := req["params"].(map[string]any)
		switch method {
		case "agent.request":
			if params["method"] != "runtime.status" {
				t.Fatalf("unexpected requested control method %q", params["method"])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-status",
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-status",
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "FAILED",
					"response":     `{"error":"runtime status unavailable"}`,
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		if pid != 4321 {
			t.Fatalf("unexpected pid %d", pid)
		}
		return true, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}

	record := ManagedAgentRecord{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	saveTrustedAgentProcessStateForTest(t, record, 4321)

	live := loadManagerLiveRuntimeStatus(context.Background(), record)
	if live.ProcessState != "running" {
		t.Fatalf("expected running process state, got %+v", live)
	}
	if !strings.Contains(live.Error, "request req-status finished with status FAILED") {
		t.Fatalf("expected failed request context in live error, got %+v", live)
	}
	if !strings.Contains(live.Error, "runtime status unavailable") {
		t.Fatalf("expected failed response detail in live error, got %+v", live)
	}
}

func TestManagerUIExecuteCommandRoutesInlineTaskAndTensionSwitch(t *testing.T) {
	testManagerHome(t)

	type observedRequest struct {
		method string
		params map[string]any
	}
	var observed []observedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		params, _ := req["params"].(map[string]any)
		switch method {
		case "agent.request":
			observed = append(observed, observedRequest{method: method, params: params})
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-" + params["method"].(string),
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			last := observed[len(observed)-1]
			resp := map[string]any{"status": "ok", "control": map[string]any{"paused": false}}
			switch last.params["method"] {
			case "runtime.switch_task":
				resp["control"] = map[string]any{"paused": false, "target_task_id": last.params["payload_json"].(string)}
				resp["summary"] = "task switch requested"
				resp["task_id"] = "task-2"
				resp["session_id"] = "session-9"
			case "runtime.switch_tension":
				resp["control"] = map[string]any{"paused": false, "target_tension_id": "tension-9"}
				resp["summary"] = "tension switch requested"
			case "runtime.status":
				resp["summary"] = "steady"
				resp["paused"] = false
				resp["attachable"] = true
				resp["task_id"] = "task-1"
				resp["session_id"] = "session-1"
				resp["control"] = map[string]any{"mode": "live", "last_action": "status"}
				resp["work_trigger"] = map[string]any{"trigger": "runtime_switch_task"}
				resp["focus"] = map[string]any{"task_id": "task-1", "focus_tension_id": "tension-9", "proto_cluster_id": "cluster-1"}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   observed[len(observed)-1].params["method"].(string),
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "COMPLETED",
					"response":     mustJSON(t, resp),
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"tasks": []any{}},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"items": []any{}},
			})
		default:
			t.Fatalf("unexpected method: %s", method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	model := managerVisorModel{
		ctx:      context.Background(),
		registry: BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected: 0,
	}

	cmd := model.executeCommand("switch-task task-2 session-9 reassign now")
	if cmd == nil {
		t.Fatal("expected switch-task command to produce a tea.Cmd")
	}
	msgAny := cmd()
	msg, ok := msgAny.(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from switch-task, got %T", msgAny)
	}
	if !strings.Contains(msg.text, "runtime.switch_task") {
		t.Fatalf("unexpected switch-task message: %+v", msg)
	}

	cmd = model.executeCommand("switch-tension tension-9 attach reviewer active focus now")
	if cmd == nil {
		t.Fatal("expected switch-tension command to produce a tea.Cmd")
	}
	msgAny = cmd()
	msg, ok = msgAny.(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from switch-tension, got %T", msgAny)
	}
	if !strings.Contains(msg.text, "runtime.switch_tension") {
		t.Fatalf("unexpected switch-tension message: %+v", msg)
	}

	if len(observed) != 2 {
		t.Fatalf("expected control-plane requests to be captured, got %#v", observed)
	}
	if observed[0].params["method"] != "runtime.switch_task" {
		t.Fatalf("expected first request to be runtime.switch_task, got %#v", observed[0].params)
	}
	var switchTaskPayload map[string]any
	if err := json.Unmarshal([]byte(observed[0].params["payload_json"].(string)), &switchTaskPayload); err != nil {
		t.Fatalf("decode switch-task payload: %v", err)
	}
	if switchTaskPayload["task_id"] != "task-2" || switchTaskPayload["session_id"] != "session-9" {
		t.Fatalf("unexpected switch-task payload: %#v", switchTaskPayload)
	}
	if observed[1].params["method"] != "runtime.switch_tension" {
		t.Fatalf("expected second request to be runtime.switch_tension, got %#v", observed[1].params)
	}
	var switchTensionPayload map[string]any
	if err := json.Unmarshal([]byte(observed[1].params["payload_json"].(string)), &switchTensionPayload); err != nil {
		t.Fatalf("decode switch-tension payload: %v", err)
	}
	if switchTensionPayload["tension_id"] != "tension-9" {
		t.Fatalf("unexpected switch-tension payload: %#v", switchTensionPayload)
	}
}

func TestSelectManagerTaskChoicesSkipsClosedTasks(t *testing.T) {
	tasks := []WorkspaceTaskRecord{
		{TaskID: "done-1", Status: "DONE", Title: "Done"},
		{TaskID: "resolved-1", Status: "RESOLVED", Title: "Resolved"},
		{TaskID: "task-1", Status: "RUNNING", Title: "Running"},
		{TaskID: "task-2", Status: "PENDING", Title: "Pending"},
	}

	got := selectManagerTaskChoices(tasks)

	if len(got) != 2 {
		t.Fatalf("expected 2 actionable tasks, got %+v", got)
	}
	if got[0].TaskID != "task-1" || got[1].TaskID != "task-2" {
		t.Fatalf("unexpected task choice ordering: %+v", got)
	}
}

func TestManagerVisorUpdateOpensTaskPickerAndNavigates(t *testing.T) {
	model := managerVisorModel{
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{{AgentID: "lyrica", DisplayName: "Lyrica", Workdir: t.TempDir()}},
		},
		catalog: &managerWorkspaceCatalog{
			Tasks: []WorkspaceTaskRecord{
				{TaskID: "task-1", Status: "RUNNING", Title: "Task One"},
				{TaskID: "task-2", Status: "PENDING", Title: "Task Two"},
			},
		},
	}

	nextModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'w'}})
	updated := nextModel.(managerVisorModel)
	if updated.pickerMode != managerPickerTask || updated.pickerIndex != 0 {
		t.Fatalf("expected task picker to open at index 0, got mode=%q index=%d", updated.pickerMode, updated.pickerIndex)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyDown})
	updated = nextModel.(managerVisorModel)
	if updated.pickerIndex != 1 {
		t.Fatalf("expected picker index to move down, got %d", updated.pickerIndex)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyEsc})
	updated = nextModel.(managerVisorModel)
	if updated.pickerMode != managerPickerNone {
		t.Fatalf("expected picker to close on esc, got %q", updated.pickerMode)
	}
}

func TestManagerVisorUpdateCyclesFiltersAndTensionAction(t *testing.T) {
	model := managerVisorModel{
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{{AgentID: "lyrica", DisplayName: "Lyrica", Workdir: t.TempDir()}},
		},
		catalog: &managerWorkspaceCatalog{
			Tasks: []WorkspaceTaskRecord{
				{TaskID: "task-1", Status: "RUNNING", Title: "Task One"},
				{TaskID: "task-2", Status: "PENDING", Title: "Task Two"},
			},
			Tensions: []TensionFrontierItem{
				{TensionID: "tension-1", TensionType: "BLOCKER", ReviewStatus: "active", Title: "Blocker"},
				{TensionID: "tension-2", TensionType: "QUESTION", ReviewStatus: "active", Title: "Question"},
			},
		},
	}

	nextModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	updated := nextModel.(managerVisorModel)
	if updated.taskStatusFilter != "RUNNING" {
		t.Fatalf("expected task filter to advance to RUNNING, got %q", updated.taskStatusFilter)
	}
	if len(updated.visibleTaskChoices()) != 1 || updated.visibleTaskChoices()[0].TaskID != "task-1" {
		t.Fatalf("unexpected visible task choices after filter: %+v", updated.visibleTaskChoices())
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'y'}})
	updated = nextModel.(managerVisorModel)
	if updated.tensionTypeFilter != "BLOCKER" {
		t.Fatalf("expected tension type filter to advance to BLOCKER, got %q", updated.tensionTypeFilter)
	}
	if len(updated.visibleTensionChoices()) != 1 || updated.visibleTensionChoices()[0].TensionID != "tension-1" {
		t.Fatalf("unexpected visible tension choices after filter: %+v", updated.visibleTensionChoices())
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	updated = nextModel.(managerVisorModel)
	if updated.tensionAction != "detach" {
		t.Fatalf("expected tension action to advance to detach, got %q", updated.tensionAction)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'u'}})
	updated = nextModel.(managerVisorModel)
	if updated.tensionLifecycle != "IN_REVIEW" {
		t.Fatalf("expected tension lifecycle to advance to IN_REVIEW, got %q", updated.tensionLifecycle)
	}
}

func TestResolveManagerCreateWorkdirBuildsPreview(t *testing.T) {
	testManagerHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir, err := resolveManagerCreateWorkdir(root, "lyrica")
	if err != nil {
		t.Fatalf("resolveManagerCreateWorkdir() error: %v", err)
	}
	want := filepath.Join(root, "lyrica")
	if workdir != want {
		t.Fatalf("expected workdir %q, got %q", want, workdir)
	}
}

func TestResolveManagerCreateWorkdirRejectsParentOutsideManagedRoot(t *testing.T) {
	testManagerHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}

	outside := t.TempDir()
	if _, err := resolveManagerCreateWorkdir(outside, "lyrica"); err == nil || !strings.Contains(err.Error(), "managed root") {
		t.Fatalf("expected outside parent rejection, got %v", err)
	}
}

func TestResolveManagerCreateWorkdirRejectsFolderTraversal(t *testing.T) {
	testManagerHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}

	if _, err := resolveManagerCreateWorkdir(root, "..\\escape"); err == nil || !strings.Contains(err.Error(), "single path component") {
		t.Fatalf("expected folder traversal rejection, got %v", err)
	}
}

func TestSuggestNewAgentFolderNameSkipsExistingBases(t *testing.T) {
	registry := BotRegistry{
		Agents: []ManagedAgentRecord{
			{AgentID: "a1", Workdir: filepath.Join("C:\\agents", "agent-01")},
			{AgentID: "a2", Workdir: filepath.Join("C:\\agents", "agent-02")},
		},
	}

	got := suggestNewAgentFolderName(registry)
	if got != "agent-03" {
		t.Fatalf("expected next folder suggestion agent-03, got %q", got)
	}
}

func TestManagerVisorNewAgentPanelOpensAndEditsFolderName(t *testing.T) {
	model := newManagerVisorModel(context.Background())

	nextModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	updated := nextModel.(managerVisorModel)
	if !updated.createMode || updated.createEditing {
		t.Fatalf("expected new agent panel to open in navigation mode, got %+v", updated)
	}
	if updated.createParentDir == "" || updated.createFolderName == "" {
		t.Fatalf("expected new agent panel defaults to be prefilled, got parent=%q folder=%q", updated.createParentDir, updated.createFolderName)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyDown})
	updated = nextModel.(managerVisorModel)
	if spec, _, _ := updated.selectedCreateField(); spec.Key != "folder_name" {
		t.Fatalf("expected folder_name field to be selected, got %q", spec.Key)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
	updated = nextModel.(managerVisorModel)
	if !updated.createEditing {
		t.Fatal("expected new agent panel to enter editing mode")
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyCtrlU})
	updated = nextModel.(managerVisorModel)
	for _, r := range "lyrica" {
		nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		updated = nextModel.(managerVisorModel)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
	updated = nextModel.(managerVisorModel)
	if updated.createEditing {
		t.Fatal("expected editing mode to close after applying new agent field")
	}
	if updated.createFolderName != "lyrica" {
		t.Fatalf("expected folder_name to update, got %q", updated.createFolderName)
	}

	spec, preview, ok := updated.selectedCreateField()
	if !ok || spec.Key != "folder_name" {
		t.Fatalf("unexpected selected create field after edit: %q", spec.Key)
	}
	if preview != "lyrica" {
		t.Fatalf("expected folder_name preview to reflect edit, got %q", preview)
	}
	if workdir := managerCreateFieldValue("workdir_preview", updated.createParentDir, updated.createFolderName); !strings.HasSuffix(workdir, string(filepath.Separator)+"lyrica") {
		t.Fatalf("expected workdir preview to end with folder name, got %q", workdir)
	}
}

func TestManagerVisorRefreshSelectsOnboardedAgentByWorkdir(t *testing.T) {
	testManagerHome(t)

	oldWorkdir := filepath.Join(t.TempDir(), "existing-agent")
	newWorkdir := filepath.Join(t.TempDir(), "new-agent")
	oldRecord := ManagedAgentRecord{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		Workdir:     oldWorkdir,
	}
	if err := UpsertManagedAgent(oldRecord); err != nil {
		t.Fatalf("UpsertManagedAgent(oldRecord) error: %v", err)
	}

	model := managerVisorModel{
		ctx:      context.Background(),
		registry: LoadBotRegistry(),
		selected: 0,
	}

	newRecord := ManagedAgentRecord{
		AgentID:     "agent-2",
		DisplayName: "Agent Two",
		Workdir:     newWorkdir,
	}
	if err := UpsertManagedAgent(newRecord); err != nil {
		t.Fatalf("UpsertManagedAgent(newRecord) error: %v", err)
	}

	nextModel, cmd := model.Update(managerActionMsg{
		text:          fmt.Sprintf("onboarded %s", newWorkdir),
		refresh:       true,
		selectWorkdir: newWorkdir,
	})
	updated := nextModel.(managerVisorModel)
	if cmd == nil {
		t.Fatal("expected onboard refresh to schedule selected refresh command")
	}
	if updated.selected != 1 {
		t.Fatalf("expected new onboarded agent to be selected, got index %d with registry %+v", updated.selected, updated.registry.Agents)
	}
	record, ok := updated.selectedRecord()
	if !ok {
		t.Fatal("expected selected record after onboard refresh")
	}
	if record.AgentID != "agent-2" || !strings.EqualFold(filepath.Clean(record.Workdir), filepath.Clean(newWorkdir)) {
		t.Fatalf("expected selected record to match onboarded agent, got %+v", record)
	}
}

func TestBuildManagerOnboardActionMsgIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "agent-2",
		DisplayName: "Agent Two",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-2",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	msg := buildManagerOnboardActionMsg(context.Background(), workdir)
	if !msg.refresh || !strings.EqualFold(filepath.Clean(msg.selectWorkdir), filepath.Clean(workdir)) {
		t.Fatalf("expected onboard action message to request refresh for selected workdir, got %+v", msg)
	}
	if !strings.Contains(msg.text, "onboarded "+workdir) {
		t.Fatalf("expected onboard action message text, got %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected onboard action message to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected onboard action message to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected onboard action message to include refreshed tension catalog, got %+v", msg.catalog)
	}
}

func TestManagerVisorLogsPaneTogglesAndSchedulesRefresh(t *testing.T) {
	workdir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workdir, "agent.out.log"), []byte("line one\nline two\n"), 0o600); err != nil {
		t.Fatalf("write stdout log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, "agent.err.log"), []byte("err one\n"), 0o600); err != nil {
		t.Fatalf("write stderr log: %v", err)
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{
				{AgentID: "lyrica", DisplayName: "Lyrica", Workdir: workdir},
			},
		},
	}

	nextModel, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	updated := nextModel.(managerVisorModel)
	if !updated.showLogs {
		t.Fatal("expected logs pane to open on l")
	}
	if cmd == nil {
		t.Fatal("expected opening logs pane to schedule load/refresh commands")
	}

	nextModel, tickCmd := updated.Update(managerLogTickMsg{})
	updated = nextModel.(managerVisorModel)
	if !updated.showLogs {
		t.Fatal("expected logs pane to stay open on tick")
	}
	if tickCmd == nil {
		t.Fatal("expected log tick to schedule another refresh")
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	updated = nextModel.(managerVisorModel)
	if updated.showLogs {
		t.Fatal("expected logs pane to close on second l")
	}
}

func TestManagerVisorRefreshHotkeyIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{record},
		},
	}

	nextModel, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	updated := nextModel.(managerVisorModel)
	if updated.selected != 0 {
		t.Fatalf("expected refresh hotkey to keep selected index, got %d", updated.selected)
	}
	if cmd == nil {
		t.Fatal("expected refresh hotkey to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from refresh hotkey, got %T", cmd())
	}
	if !msg.refresh || msg.text != "refreshed registry" {
		t.Fatalf("unexpected refresh hotkey message: %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected refresh hotkey to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refresh hotkey to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refresh hotkey to include refreshed tension catalog, got %+v", msg.catalog)
	}
}

func TestManagerVisorQueueChatAndAttachIncludeCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	assertCurrentSnapshot := func(t *testing.T, msg managerActionMsg) {
		t.Helper()
		if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
			t.Fatalf("expected current stopped live status, got %+v", msg.live)
		}
		if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
			t.Fatalf("expected refreshed task catalog, got %+v", msg.catalog)
		}
		if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
			t.Fatalf("expected refreshed tension catalog, got %+v", msg.catalog)
		}
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{record},
		},
	}

	chatCmd := model.queueChatSelected()
	if chatCmd == nil {
		t.Fatal("expected queueChatSelected to produce a tea.Cmd")
	}
	chatMsg, ok := chatCmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from queueChatSelected, got %T", chatCmd())
	}
	if chatMsg.chatAgent != "lyrica" || !strings.Contains(chatMsg.text, "opening chat for lyrica") {
		t.Fatalf("unexpected chat message: %+v", chatMsg)
	}
	assertCurrentSnapshot(t, chatMsg)

	attachCmd := model.queueAttachSelected()
	if attachCmd == nil {
		t.Fatal("expected queueAttachSelected to produce a tea.Cmd")
	}
	attachMsg, ok := attachCmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from queueAttachSelected, got %T", attachCmd())
	}
	if attachMsg.attach != "lyrica" || !strings.Contains(attachMsg.text, "opening live attach for lyrica") {
		t.Fatalf("unexpected attach message: %+v", attachMsg)
	}
	assertCurrentSnapshot(t, attachMsg)
}

func TestManagerVisorExecuteCommandAttachIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{record},
		},
	}

	cmd := model.executeCommand("attach lyrica")
	if cmd == nil {
		t.Fatal("expected attach command to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from attach command, got %T", cmd())
	}
	if msg.attach != "lyrica" || !strings.Contains(msg.text, "opening live attach for lyrica") {
		t.Fatalf("unexpected attach command message: %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected attach command to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected attach command to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected attach command to include refreshed tension catalog, got %+v", msg.catalog)
	}
}

func TestManagerVisorLoadLogsFailureIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	if err := os.Mkdir(filepath.Join(workdir, "agent.out.log"), 0o755); err != nil {
		t.Fatalf("mkdir fake stdout log dir: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{record},
		},
	}
	cmd := model.loadLogsForSelected(20)
	if cmd == nil {
		t.Fatal("expected loadLogsForSelected to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from loadLogsForSelected, got %T", cmd())
	}
	if msg.refresh {
		t.Fatalf("expected log load failure to avoid full refresh, got %+v", msg)
	}
	if !strings.Contains(msg.text, "load logs for lyrica") || !strings.Contains(msg.text, "agent.out.log") {
		t.Fatalf("expected log load failure text to surface action and failing path, got %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected log load failure to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected log load failure to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected log load failure to include refreshed tension catalog, got %+v", msg.catalog)
	}
}

func TestManagerVisorLoadLogsSuccessIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workdir, "agent.out.log"), []byte("line one\nline two\n"), 0o600); err != nil {
		t.Fatalf("write stdout log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, "agent.err.log"), []byte("err one\n"), 0o600); err != nil {
		t.Fatalf("write stderr log: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{record},
		},
	}
	cmd := model.loadLogsForSelected(20)
	if cmd == nil {
		t.Fatal("expected loadLogsForSelected to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from loadLogsForSelected, got %T", cmd())
	}
	if msg.refresh {
		t.Fatalf("expected log load success to avoid full refresh, got %+v", msg)
	}
	if !msg.loadLogs {
		t.Fatalf("expected log load success to open logs pane, got %+v", msg)
	}
	if !strings.Contains(msg.text, "loaded logs for lyrica") {
		t.Fatalf("expected log load success text, got %+v", msg)
	}
	if len(msg.logTail) == 0 || msg.logTail[0] != "line one" {
		t.Fatalf("expected merged log tail on success, got %+v", msg.logTail)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected log load success to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected log load success to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected log load success to include refreshed tension catalog, got %+v", msg.catalog)
	}
}

func TestManagerVisorRunStatusSelectedIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{record},
		},
	}
	cmd := model.runStatusSelected()
	if cmd == nil {
		t.Fatal("expected runStatusSelected to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from runStatusSelected, got %T", cmd())
	}
	if !msg.refresh || !strings.Contains(msg.text, "lyrica process: stopped") {
		t.Fatalf("unexpected status message: %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected status action to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected status action to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected status action to include refreshed tension catalog, got %+v", msg.catalog)
	}
}

func TestManagerVisorDefaultsPanelSavesUpdatedField(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Defaults: LoadBotRegistry().Defaults,
			Agents:   []ManagedAgentRecord{record},
		},
	}

	nextModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	updated := nextModel.(managerVisorModel)
	if !updated.defaultsMode || updated.defaultsEditing {
		t.Fatalf("expected defaults panel to open in navigation mode, got %+v", updated)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
	updated = nextModel.(managerVisorModel)
	if !updated.defaultsEditing {
		t.Fatal("expected defaults panel to enter editing mode")
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyCtrlU})
	updated = nextModel.(managerVisorModel)
	for _, r := range "https://rhizome.example" {
		nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		updated = nextModel.(managerVisorModel)
	}

	nextModel, cmd := updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
	updated = nextModel.(managerVisorModel)
	if updated.defaultsEditing {
		t.Fatal("expected editing mode to close after save")
	}
	if cmd == nil {
		t.Fatal("expected saving defaults field to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg after saving defaults field, got %T", cmd())
	}
	if !msg.refresh || !strings.Contains(msg.text, "updated default host") {
		t.Fatalf("unexpected save message: %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected defaults save to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected defaults save to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected defaults save to include refreshed tension catalog, got %+v", msg.catalog)
	}

	nextModel, refreshCmd := updated.Update(msg)
	updated = nextModel.(managerVisorModel)
	if refreshCmd == nil {
		t.Fatal("expected refresh command after saving defaults field")
	}
	registry := LoadBotRegistry()
	if registry.Defaults.HostURL != "https://rhizome.example" {
		t.Fatalf("expected host default to persist, got %q", registry.Defaults.HostURL)
	}
	if !updated.defaultsMode {
		t.Fatal("expected defaults panel to remain open after save")
	}
}

func TestManagerVisorExecuteCommandDefaultsIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	assertCurrentSnapshot := func(t *testing.T, msg managerActionMsg) {
		t.Helper()
		if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
			t.Fatalf("expected current stopped live status, got %+v", msg.live)
		}
		if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
			t.Fatalf("expected refreshed task catalog, got %+v", msg.catalog)
		}
		if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
			t.Fatalf("expected refreshed tension catalog, got %+v", msg.catalog)
		}
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Defaults: LoadBotRegistry().Defaults,
			Agents:   []ManagedAgentRecord{record},
		},
	}

	cmd := model.executeCommand("defaults")
	if cmd == nil {
		t.Fatal("expected defaults command to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from defaults command, got %T", cmd())
	}
	if !msg.refresh || !strings.Contains(msg.text, "defaults: workspace=") {
		t.Fatalf("unexpected defaults message: %+v", msg)
	}
	assertCurrentSnapshot(t, msg)

	cmd = model.executeCommand("set-default host https://rhizome.example")
	if cmd == nil {
		t.Fatal("expected set-default command to produce a tea.Cmd")
	}
	msg, ok = cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from set-default command, got %T", cmd())
	}
	if !msg.refresh || msg.text != "updated default" {
		t.Fatalf("unexpected set-default message: %+v", msg)
	}
	assertCurrentSnapshot(t, msg)

	registry := LoadBotRegistry()
	if registry.Defaults.HostURL != "https://rhizome.example" {
		t.Fatalf("expected set-default to persist host, got %q", registry.Defaults.HostURL)
	}

	cmd = model.executeCommand("clear-default host")
	if cmd == nil {
		t.Fatal("expected clear-default command to produce a tea.Cmd")
	}
	msg, ok = cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from clear-default command, got %T", cmd())
	}
	if !msg.refresh || msg.text != "cleared default" {
		t.Fatalf("unexpected clear-default message: %+v", msg)
	}
	assertCurrentSnapshot(t, msg)

	registry = LoadBotRegistry()
	if registry.Defaults.HostURL != defaultRhizomeHostURL {
		t.Fatalf("expected clear-default to restore built-in host default, got %q", registry.Defaults.HostURL)
	}
}

func TestManagerVisorExecuteCommandRefreshIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	assertCurrentSnapshot := func(t *testing.T, msg managerActionMsg) {
		t.Helper()
		if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
			t.Fatalf("expected current stopped live status, got %+v", msg.live)
		}
		if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
			t.Fatalf("expected refreshed task catalog, got %+v", msg.catalog)
		}
		if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
			t.Fatalf("expected refreshed tension catalog, got %+v", msg.catalog)
		}
	}

	model := managerVisorModel{
		ctx: context.Background(),
		registry: BotRegistry{
			Agents: []ManagedAgentRecord{record},
		},
	}

	for _, command := range []string{"refresh", "list", "reload"} {
		cmd := model.executeCommand(command)
		if cmd == nil {
			t.Fatalf("expected %q command to produce a tea.Cmd", command)
		}
		msg, ok := cmd().(managerActionMsg)
		if !ok {
			t.Fatalf("expected managerActionMsg from %q command, got %T", command, cmd())
		}
		if !msg.refresh || msg.text != "refreshed registry" {
			t.Fatalf("unexpected %q message: %+v", command, msg)
		}
		assertCurrentSnapshot(t, msg)
	}
}

func TestRenderManagerDefaultsMasksSecretFields(t *testing.T) {
	rendered := renderManagerDefaults(BotManagerDefaults{
		WorkspaceID:       "rhizome-main",
		WorkspacePassword: "test-workspace-password",
	}, false, false, 0, "", 80)

	if strings.Contains(rendered, "test-workspace-password") {
		t.Fatalf("expected secret defaults to be masked, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "workspace_password: ****") {
		t.Fatalf("expected masked secret placeholder, got:\n%s", rendered)
	}
}

func TestManagerVisorAgentPanelSavesDisplayName(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.example",
		WorkspaceID: "rhizome-main",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "rhizome-main",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := newManagerVisorModel(context.Background())
	model.registry = LoadBotRegistry()

	nextModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	updated := nextModel.(managerVisorModel)
	if !updated.agentMode || updated.agentEditing {
		t.Fatalf("expected agent panel to open in navigation mode, got %+v", updated)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyDown})
	updated = nextModel.(managerVisorModel)
	if spec, _, _ := updated.selectedAgentField(); spec.Key != "display_name" {
		t.Fatalf("expected display_name field to be selected, got %q", spec.Key)
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
	updated = nextModel.(managerVisorModel)
	if !updated.agentEditing {
		t.Fatal("expected agent panel to enter editing mode")
	}

	nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyCtrlU})
	updated = nextModel.(managerVisorModel)
	for _, r := range "Lyrica Prime" {
		nextModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		updated = nextModel.(managerVisorModel)
	}

	nextModel, cmd := updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
	updated = nextModel.(managerVisorModel)
	if updated.agentEditing {
		t.Fatal("expected agent editing mode to close after save")
	}
	if cmd == nil {
		t.Fatal("expected saving agent field to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg after saving agent field, got %T", cmd())
	}
	if !msg.refresh || !strings.Contains(msg.text, "updated display_name") {
		t.Fatalf("unexpected save message: %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected display-name save to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected display-name save to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected display-name save to include refreshed tension catalog, got %+v", msg.catalog)
	}

	nextModel, refreshCmd := updated.Update(msg)
	updated = nextModel.(managerVisorModel)
	if refreshCmd == nil {
		t.Fatal("expected refresh command after agent update")
	}
	registry := LoadBotRegistry()
	if len(registry.Agents) != 1 || registry.Agents[0].DisplayName != "Lyrica Prime" {
		t.Fatalf("expected updated display name in registry, got %+v", registry.Agents)
	}
	if !updated.agentMode {
		t.Fatal("expected agent panel to remain open after save")
	}
}

func TestManagerVisorAgentPanelReadOnlyFieldIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.example",
		WorkspaceID: "rhizome-main",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "rhizome-main",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := newManagerVisorModel(context.Background())
	model.registry = LoadBotRegistry()

	nextModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	updated := nextModel.(managerVisorModel)
	if !updated.agentMode || updated.agentEditing {
		t.Fatalf("expected agent panel to open in navigation mode, got %+v", updated)
	}
	if spec, _, _ := updated.selectedAgentField(); spec.Key != "agent_id" {
		t.Fatalf("expected agent_id field to be selected initially, got %q", spec.Key)
	}

	nextModel, cmd := updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
	updated = nextModel.(managerVisorModel)
	if updated.agentEditing {
		t.Fatal("expected read-only field to stay out of editing mode")
	}
	if cmd == nil {
		t.Fatal("expected read-only agent field to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg after read-only field enter, got %T", cmd())
	}
	if msg.text != "agent_id is read-only" {
		t.Fatalf("unexpected read-only field message: %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected read-only field message to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected read-only field message to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected read-only field message to include refreshed tension catalog, got %+v", msg.catalog)
	}
}

func TestManagerVisorAgentPanelRehomesWorkdir(t *testing.T) {
	testManagerHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	oldWorkdir := filepath.Join(root, "old-home")
	newWorkdir := filepath.Join(root, "new-home")
	if err := os.MkdirAll(oldWorkdir, 0o755); err != nil {
		t.Fatalf("mkdir old workdir: %v", err)
	}
	sentinelPath := filepath.Join(oldWorkdir, "agent.runtime.json")
	if err := os.WriteFile(sentinelPath, []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     oldWorkdir,
		HostURL:     "https://rhizome.example",
		WorkspaceID: "rhizome-main",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(oldWorkdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "rhizome-main",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx:             context.Background(),
		registry:        LoadBotRegistry(),
		agentMode:       true,
		agentFieldIndex: 2,
	}
	cmd := model.saveSelectedAgentField(managerAgentFieldSpecs[2], newWorkdir)
	if cmd == nil {
		t.Fatal("expected workdir save to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from workdir save, got %T", cmd())
	}
	if !msg.refresh || !strings.Contains(msg.text, "updated workdir") {
		t.Fatalf("unexpected workdir save message: %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected workdir save to include refreshed stopped live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected workdir save to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected workdir save to include refreshed tension catalog, got %+v", msg.catalog)
	}

	registry := LoadBotRegistry()
	if len(registry.Agents) != 1 || registry.Agents[0].Workdir != newWorkdir {
		t.Fatalf("expected registry to point to new workdir, got %+v", registry.Agents)
	}
	if _, err := os.Stat(filepath.Join(newWorkdir, "agent.runtime.json")); err != nil {
		t.Fatalf("expected sentinel file in moved workdir: %v", err)
	}
	if _, err := os.Stat(oldWorkdir); !os.IsNotExist(err) {
		t.Fatalf("expected old workdir to be moved away, stat err=%v", err)
	}
}

func TestMoveManagedAgentWorkdirRejectsTargetOutsideManagedRoot(t *testing.T) {
	testManagerHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	oldWorkdir := filepath.Join(root, "old-home")
	if err := os.MkdirAll(oldWorkdir, 0o755); err != nil {
		t.Fatalf("mkdir old workdir: %v", err)
	}

	outside := filepath.Join(t.TempDir(), "escape")
	if _, _, err := moveManagedAgentWorkdir(oldWorkdir, outside); err == nil || !strings.Contains(err.Error(), "managed root") {
		t.Fatalf("expected outside target rejection, got %v", err)
	}
}

func TestManagerVisorAgentPanelWorkdirFailureIncludesCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	root := t.TempDir()
	if err := SetManagerDefault("default_parent_dir", root); err != nil {
		t.Fatalf("SetManagerDefault(default_parent_dir) error: %v", err)
	}
	workdir := filepath.Join(root, "agent-home")
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		t.Fatalf("mkdir workdir: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "lyrica",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx:             context.Background(),
		registry:        LoadBotRegistry(),
		agentMode:       true,
		agentFieldIndex: 2,
	}
	outside := filepath.Join(t.TempDir(), "escape")
	cmd := model.saveSelectedAgentField(managerAgentFieldSpecs[2], outside)
	if cmd == nil {
		t.Fatal("expected workdir save failure to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from workdir failure, got %T", cmd())
	}
	if !msg.refresh || !strings.Contains(msg.text, "managed root") || !strings.Contains(msg.text, "process: stopped") {
		t.Fatalf("unexpected workdir failure message: %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected stopped live status on workdir failure, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected refreshed task catalog on workdir failure, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected refreshed tension catalog on workdir failure, got %+v", msg.catalog.Tensions)
	}
}

func TestManagerVisorAgentPanelRemovesStoppedAgent(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.example",
		WorkspaceID: "rhizome-main",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	model := managerVisorModel{
		ctx:             context.Background(),
		registry:        LoadBotRegistry(),
		agentMode:       true,
		agentFieldIndex: len(managerAgentFieldSpecs) - 1,
	}
	cmd := model.saveSelectedAgentField(managerAgentFieldSpecs[len(managerAgentFieldSpecs)-1], "")
	if cmd == nil {
		t.Fatal("expected remove action to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from remove action, got %T", cmd())
	}
	if !msg.refresh || !strings.Contains(msg.text, "removed lyrica from registry") {
		t.Fatalf("unexpected remove message: %+v", msg)
	}
	if msg.live == nil || *msg.live != (managerLiveRuntimeStatus{}) {
		t.Fatalf("expected remove of last agent to clear live snapshot, got %+v", msg.live)
	}
	if msg.catalog == nil || msg.catalog.Error != "" || len(msg.catalog.Tasks) != 0 || len(msg.catalog.Tensions) != 0 {
		t.Fatalf("expected remove of last agent to clear workspace catalog snapshot, got %+v", msg.catalog)
	}

	registry := LoadBotRegistry()
	if len(registry.Agents) != 0 {
		t.Fatalf("expected registry to be empty after remove, got %+v", registry.Agents)
	}
}

func TestManagerVisorAgentPanelRemoveSelectsReplacementStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	removedWorkdir := t.TempDir()
	replacementWorkdir := t.TempDir()
	removed := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     removedWorkdir,
	}
	replacement := ManagedAgentRecord{
		AgentID:     "orion",
		DisplayName: "Orion",
		Workdir:     replacementWorkdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(removed); err != nil {
		t.Fatalf("UpsertManagedAgent(removed) error: %v", err)
	}
	if err := UpsertManagedAgent(replacement); err != nil {
		t.Fatalf("UpsertManagedAgent(replacement) error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "PENDING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected rpc method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(replacementWorkdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "orion",
		AgentToken:  "local-token",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	model := managerVisorModel{
		ctx:             context.Background(),
		registry:        LoadBotRegistry(),
		selected:        0,
		agentMode:       true,
		agentFieldIndex: len(managerAgentFieldSpecs) - 1,
	}
	cmd := model.saveSelectedAgentField(managerAgentFieldSpecs[len(managerAgentFieldSpecs)-1], "")
	if cmd == nil {
		t.Fatal("expected remove action to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from remove action, got %T", cmd())
	}
	if !msg.refresh || !strings.Contains(msg.text, "removed lyrica from registry") {
		t.Fatalf("unexpected remove message: %+v", msg)
	}
	if !strings.EqualFold(filepath.Clean(msg.selectWorkdir), filepath.Clean(replacementWorkdir)) {
		t.Fatalf("expected remove message to select replacement workdir, got %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "stopped" || msg.live.Error != "" {
		t.Fatalf("expected remove message to include replacement live snapshot, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected remove message to include replacement task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected remove message to include replacement tension catalog, got %+v", msg.catalog)
	}

	nextModel, refreshCmd := model.Update(msg)
	updated := nextModel.(managerVisorModel)
	if refreshCmd == nil {
		t.Fatal("expected remove update to schedule follow-up refresh")
	}
	if updated.selected != 0 {
		t.Fatalf("expected replacement agent to stay selected at index 0, got %d", updated.selected)
	}
	selected, ok := updated.selectedRecord()
	if !ok || selected.AgentID != "orion" {
		t.Fatalf("expected replacement agent to become selected, got %+v", selected)
	}
	if updated.liveStatus == nil || updated.liveStatus.ProcessState != "stopped" || updated.liveStatus.Error != "" {
		t.Fatalf("expected updated model to carry replacement live snapshot immediately, got %+v", updated.liveStatus)
	}
	if updated.catalog == nil || len(updated.catalog.Tasks) != 1 || updated.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected updated model to carry replacement task catalog immediately, got %+v", updated.catalog)
	}
	if len(updated.catalog.Tensions) != 1 || updated.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected updated model to carry replacement tension catalog immediately, got %+v", updated.catalog)
	}
}

func TestManagerVisorAgentPanelRejectsRunningRemoveWithCurrentStatusAndCatalog(t *testing.T) {
	testManagerHome(t)

	workdir := t.TempDir()
	record := ManagedAgentRecord{
		AgentID:     "lyrica",
		DisplayName: "Lyrica",
		Workdir:     workdir,
		HostURL:     "https://rhizome.test",
		WorkspaceID: "ws-1",
	}
	if err := UpsertManagedAgent(record); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "agent.request":
			params, _ := req["params"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-" + fmt.Sprint(params["method"]),
					"workspace_id": "ws-1",
					"to_agent_id":  "lyrica",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-runtime.status",
					"workspace_id": "ws-1",
					"to_agent_id":  "lyrica",
					"status":       "COMPLETED",
					"response": mustJSON(t, map[string]any{
						"status":     "ok",
						"summary":    "steady",
						"paused":     false,
						"attachable": true,
						"task_id":    "task-2",
						"session_id": "session-2",
					}),
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tasks": []map[string]any{
						{"task_id": "task-2", "status": "RUNNING", "title": "Task Two"},
					},
				},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"items": []map[string]any{
						{"tension_id": "tension-1", "title": "Review blocker", "review_status": "active"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", method)
		}
	}))
	defer server.Close()

	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}
	saveTrustedAgentProcessStateForTest(t, record, 4321)

	origExists := managedAgentProcessExistsFunc
	origMatches := managedAgentProcessMatchesFunc
	defer func() {
		managedAgentProcessExistsFunc = origExists
		managedAgentProcessMatchesFunc = origMatches
	}()
	managedAgentProcessExistsFunc = func(pid int) (bool, error) {
		return pid == 4321, nil
	}
	managedAgentProcessMatchesFunc = func(pid int, state AgentProcessState, gotRecord ManagedAgentRecord) (bool, error) {
		return true, nil
	}

	model := managerVisorModel{
		ctx:             context.Background(),
		registry:        LoadBotRegistry(),
		agentMode:       true,
		agentFieldIndex: len(managerAgentFieldSpecs) - 1,
	}
	cmd := model.saveSelectedAgentField(managerAgentFieldSpecs[len(managerAgentFieldSpecs)-1], "")
	if cmd == nil {
		t.Fatal("expected remove action to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from blocked remove action, got %T", cmd())
	}
	if !msg.refresh {
		t.Fatalf("expected blocked remove to request refresh, got %+v", msg)
	}
	if msg.live == nil || msg.live.ProcessState != "running" || msg.live.ActiveTaskID != "task-2" {
		t.Fatalf("expected blocked remove to include refreshed live status, got %+v", msg.live)
	}
	if msg.catalog == nil || len(msg.catalog.Tasks) != 1 || msg.catalog.Tasks[0].TaskID != "task-2" {
		t.Fatalf("expected blocked remove to include refreshed task catalog, got %+v", msg.catalog)
	}
	if len(msg.catalog.Tensions) != 1 || msg.catalog.Tensions[0].TensionID != "tension-1" {
		t.Fatalf("expected blocked remove to include refreshed tension catalog, got %+v", msg.catalog)
	}
	if !strings.Contains(msg.text, "stop lyrica before removing it") || !strings.Contains(msg.text, "process: running pid=4321") {
		t.Fatalf("expected blocked remove message to include guard plus current process, got %+v", msg)
	}

	registry := LoadBotRegistry()
	if len(registry.Agents) != 1 || registry.Agents[0].AgentID != "lyrica" {
		t.Fatalf("expected blocked remove to preserve registry entry, got %+v", registry.Agents)
	}
}

func TestManagerVisorConfirmPickerSelectionDispatchesLiveControl(t *testing.T) {
	testManagerHome(t)

	type observedRequest struct {
		method string
		params map[string]any
	}
	var observed []observedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		params, _ := req["params"].(map[string]any)
		switch method {
		case "agent.request":
			observed = append(observed, observedRequest{method: method, params: params})
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-" + params["method"].(string),
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			last := observed[len(observed)-1]
			resp := map[string]any{
				"status":     "ok",
				"summary":    "steady",
				"paused":     false,
				"attachable": true,
				"task_id":    "task-1",
				"session_id": "session-1",
				"control":    map[string]any{"mode": "live", "last_action": last.params["method"]},
			}
			switch last.params["method"] {
			case "runtime.switch_task":
				resp["summary"] = "task switch requested"
				resp["task_id"] = "task-2"
			case "runtime.switch_tension":
				resp["summary"] = "tension switch requested"
				resp["focus"] = map[string]any{"focus_tension_id": "tension-2"}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-" + last.params["method"].(string),
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "COMPLETED",
					"response":     mustJSON(t, resp),
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"tasks": []any{}},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"items": []any{}},
			})
		default:
			t.Fatalf("unexpected method: %s", method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}

	taskModel := managerVisorModel{
		ctx:      context.Background(),
		registry: BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected: 0,
		catalog: &managerWorkspaceCatalog{
			Tasks: []WorkspaceTaskRecord{
				{TaskID: "task-2", Status: "RUNNING", Title: "Task Two"},
			},
		},
		pickerMode:  managerPickerTask,
		pickerIndex: 0,
	}
	taskCmd := taskModel.confirmPickerSelection()
	if taskCmd == nil {
		t.Fatal("expected task picker confirmation to produce a tea.Cmd")
	}
	taskMsg, ok := taskCmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from task picker, got %T", taskCmd())
	}
	if !strings.Contains(taskMsg.text, "runtime.switch_task") {
		t.Fatalf("unexpected task picker message: %+v", taskMsg)
	}

	tensionModel := managerVisorModel{
		ctx:      context.Background(),
		registry: BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected: 0,
		catalog: &managerWorkspaceCatalog{
			Tensions: []TensionFrontierItem{
				{TensionID: "tension-2", Title: "Review blocker", ReviewStatus: "active"},
			},
		},
		pickerMode:  managerPickerTension,
		pickerIndex: 0,
	}
	tensionCmd := tensionModel.confirmPickerSelection()
	if tensionCmd == nil {
		t.Fatal("expected tension picker confirmation to produce a tea.Cmd")
	}
	tensionMsg, ok := tensionCmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from tension picker, got %T", tensionCmd())
	}
	if !strings.Contains(tensionMsg.text, "runtime.switch_tension") {
		t.Fatalf("unexpected tension picker message: %+v", tensionMsg)
	}

	var sawTaskSwitch bool
	var sawTensionSwitch bool
	for _, req := range observed {
		switch req.params["method"] {
		case "runtime.switch_task":
			sawTaskSwitch = true
			var payload map[string]any
			if err := json.Unmarshal([]byte(req.params["payload_json"].(string)), &payload); err != nil {
				t.Fatalf("decode task picker payload: %v", err)
			}
			if payload["task_id"] != "task-2" || payload["reason"] != "visor task picker" {
				t.Fatalf("unexpected task picker payload: %#v", payload)
			}
		case "runtime.switch_tension":
			sawTensionSwitch = true
			var payload map[string]any
			if err := json.Unmarshal([]byte(req.params["payload_json"].(string)), &payload); err != nil {
				t.Fatalf("decode tension picker payload: %v", err)
			}
			if payload["tension_id"] != "tension-2" || payload["action"] != "focus" || payload["reason"] != "visor tension picker" {
				t.Fatalf("unexpected tension picker payload: %#v", payload)
			}
		}
	}
	if !sawTaskSwitch || !sawTensionSwitch {
		t.Fatalf("expected both picker control requests, got %#v", observed)
	}
}

func TestManagerVisorConfirmPickerSelectionSupportsTensionLifecycleAction(t *testing.T) {
	testManagerHome(t)

	type observedRequest struct {
		method string
		params map[string]any
	}
	var observed []observedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		method, _ := req["method"].(string)
		params, _ := req["params"].(map[string]any)
		switch method {
		case "agent.request":
			observed = append(observed, observedRequest{method: method, params: params})
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-" + params["method"].(string),
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "PENDING",
				},
			})
		case "agent.request.result":
			last := observed[len(observed)-1]
			resp := map[string]any{
				"status":     "ok",
				"summary":    "steady",
				"paused":     false,
				"attachable": true,
				"task_id":    "task-1",
				"session_id": "session-1",
				"control":    map[string]any{"mode": "live", "last_action": last.params["method"]},
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"request_id":   "req-" + last.params["method"].(string),
					"workspace_id": "ws-1",
					"to_agent_id":  "agent-1",
					"status":       "COMPLETED",
					"response":     mustJSON(t, resp),
				},
			})
		case "workspace.tasks.list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"tasks": []any{}},
			})
		case "workspace.tension.frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"items": []any{}},
			})
		default:
			t.Fatalf("unexpected method: %s", method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveLocalRuntimeProfile(workdir, LocalRuntimeProfile{
		HostURL:     server.URL,
		RPCEndpoint: server.URL,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		AgentToken:  "token-1",
	}); err != nil {
		t.Fatalf("SaveLocalRuntimeProfile() error: %v", err)
	}

	record := ManagedAgentRecord{
		AgentID:     "agent-1",
		DisplayName: "Agent One",
		Workdir:     workdir,
		HostURL:     server.URL,
		WorkspaceID: "ws-1",
	}

	model := managerVisorModel{
		ctx:              context.Background(),
		registry:         BotRegistry{Agents: []ManagedAgentRecord{record}},
		selected:         0,
		tensionAction:    "lifecycle",
		tensionLifecycle: "RESOLVED",
		catalog: &managerWorkspaceCatalog{
			Tensions: []TensionFrontierItem{
				{TensionID: "tension-2", TensionType: "BLOCKER", ReviewStatus: "active", Title: "Review blocker"},
			},
		},
		pickerMode:  managerPickerTension,
		pickerIndex: 0,
	}

	cmd := model.confirmPickerSelection()
	if cmd == nil {
		t.Fatal("expected lifecycle tension picker confirmation to produce a tea.Cmd")
	}
	msg, ok := cmd().(managerActionMsg)
	if !ok {
		t.Fatalf("expected managerActionMsg from lifecycle tension picker, got %T", cmd())
	}
	if !strings.Contains(msg.text, "runtime.switch_tension") {
		t.Fatalf("unexpected lifecycle tension picker message: %+v", msg)
	}

	var sawLifecycle bool
	for _, req := range observed {
		if req.params["method"] != "runtime.switch_tension" {
			continue
		}
		sawLifecycle = true
		var payload map[string]any
		if err := json.Unmarshal([]byte(req.params["payload_json"].(string)), &payload); err != nil {
			t.Fatalf("decode lifecycle tension picker payload: %v", err)
		}
		if payload["action"] != "lifecycle" || payload["lifecycle_state"] != "RESOLVED" {
			t.Fatalf("unexpected lifecycle tension picker payload: %#v", payload)
		}
	}
	if !sawLifecycle {
		t.Fatalf("expected lifecycle tension control request, got %#v", observed)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(raw)
}
