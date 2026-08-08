package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/server"
)

func TestDashboardSmoke_WorkspaceGraph(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	bodyStr := string(body)

	// Check if the dashboard HTML incorporates the renamed Workspace Graph
	if !strings.Contains(bodyStr, "Workspace Graph") {
		t.Errorf("Dashboard doesn't contain 'Workspace Graph' text")
	}

	// Check for script onerror handler
	if !strings.Contains(bodyStr, "script.onerror") {
		t.Errorf("Dashboard is missing script.onerror handler for library loading")
	}

	// Check that TASK_FOCUS is now a live mode rather than a hidden placeholder.
	if !strings.Contains(bodyStr, `value="TASK_FOCUS">Task Focus`) {
		t.Errorf("Dashboard did not expose TASK_FOCUS mode")
	}

	if !strings.Contains(bodyStr, `value="CONTROL">Control View`) {
		t.Errorf("Dashboard did not expose CONTROL mode")
	}

	if !strings.Contains(bodyStr, `value="MEMORY_OVERLAY">Memory Overlay`) {
		t.Errorf("Dashboard did not expose MEMORY_OVERLAY mode")
	}

	if !strings.Contains(bodyStr, `value="MEMORY_ATLAS">Memory Atlas`) {
		t.Errorf("Dashboard did not expose MEMORY_ATLAS mode")
	}

	if !strings.Contains(bodyStr, `id="graph-control-focus-select"`) {
		t.Errorf("Dashboard did not expose control focus picker")
	}

	for _, needle := range []string{
		"Advisory Controls",
		"Candidate Controls",
		"Effective Control State",
		"workspace fallback",
		"candidate_only",
		"they do not self-apply policy",
		"inspectability only",
		"not a second arbiter or control authority",
		"Registration: ",
		"Presence: ",
		"Protocol: ",
		"Capabilities: ",
		"Registered only",
		`id="delete-confirm" onclick="if(event.target===this)cancelDelete()"`,
		`id="resolve-overlay" onclick="if(event.target===this)cancelResolve()"`,
		`function dashboardCloseTopOverlay()`,
		`.resolve-box .btn-row button`,
		"Repo Mutation Gate",
		"repo_mutation_activation",
		"Repo Mutation Activation",
		"Repo Mutation Actuator Dry Run",
		"repo_mutation_actuator_dry_run",
		"Patch Queue Durability",
		"project_patch_queue_durability",
		"Project Patch Queue Durability",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Errorf("Dashboard does not contain %q", needle)
		}
	}
}

func TestDashboardRuntimeHealthRendersRepoMutationActuatorDryRunPayload(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skipf("node executable not available: %v", err)
	}
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)
	for _, id := range []string{"runtime-health-badge", "runtime-health-summary"} {
		if !strings.Contains(bodyStr, `id="`+id+`"`) {
			t.Fatalf("dashboard HTML is missing runtime health anchor id=%q", id)
		}
	}
	payload := map[string]any{
		"status": "ok",
		"ts":     "2026-04-26T00:00:00Z",
		"runtime": map[string]any{
			"repo_root":     `C:\fixtures\Rhizome`,
			"binary_path":   `C:\fixtures\Rhizome\rhizome.exe`,
			"vcs_revision":  "0123456789abcdef",
			"vcs_modified":  false,
			"vcs_branch":    "main",
			"build_profile": "test",
		},
		"checkout": map[string]any{
			"branch": "main",
			"head":   "0123456789abcdef",
			"dirty":  false,
		},
		"metrics": map[string]any{
			"status": "ok",
			"health": map[string]any{
				"verdict": "ok",
				"reasons": []string{},
			},
		},
		"semantics": map[string]any{
			"readiness": map[string]any{"state": "ok"},
			"degraded":  map[string]any{"state": "ok"},
		},
		"extended_readiness": map[string]any{},
		"repo_mutation_activation": map[string]any{
			"schema":           "repo_mutation_activation_gates.v1",
			"status":           "blocked",
			"mutation_allowed": false,
			"blocking_reasons": []string{
				"materialization_preflight_verified: patch materialization is required",
				"live_mutation_actuator_enabled: live mutation actuator is not enabled",
			},
		},
		"repo_mutation_actuator_dry_run": map[string]any{
			"schema":                      "repo_mutation_actuator_dry_run.v1",
			"status":                      "blocked",
			"activation_digest":           "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"activation_status":           "blocked",
			"activation_mutation_allowed": false,
			"verifier_ready":              true,
			"actuator_enabled":            false,
			"would_mutate":                false,
			"mutation_executed":           false,
			"blocking_reasons": []string{
				"materialization_preflight_verified: patch materialization is required",
				"live_mutation_actuator_enabled: live mutation actuator is not enabled",
			},
		},
		"project_patch_queue_durability": map[string]any{
			"contract":           "project_patch_queue_durability_proof.v2",
			"state":              "ok",
			"durable":            true,
			"live_item_count":    1,
			"claimed_item_count": 1,
		},
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal dashboard runtime health payload: %v", err)
	}
	functionNames := []string{
		"authorityReferenceMs",
		"timeAgo",
		"shortHealthRevision",
		"runtimeHealthTone",
		"runtimeHealthReviewerScarcityBreakdown",
		"renderRuntimeHealth",
		"showRuntimeHealthDetail",
	}
	var js strings.Builder
	js.WriteString("function runtimeHealthTestElement(id) { return { id, style: {}, textContent: '', innerHTML: '' }; }\n")
	js.WriteString("const elements = new Map([['runtime-health-badge', runtimeHealthTestElement('runtime-health-badge')], ['runtime-health-summary', runtimeHealthTestElement('runtime-health-summary')]]);\n")
	js.WriteString("global.document = { getElementById(id) { return elements.get(id) || null; } };\n")
	js.WriteString("let modal = null;\n")
	js.WriteString("function openModal(title, body) { modal = { title, body }; }\n")
	js.WriteString("function toast(message) { throw new Error('unexpected toast: ' + message); }\n")
	js.WriteString("function esc(s) { return String(s || '').replace(/[&<>\\\"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','\\\"':'&quot;',\"'\":'&#39;'}[c])); }\n")
	js.WriteString("let runtimeHealthCache = ")
	js.Write(payloadJSON)
	js.WriteString(";\n")
	for _, name := range functionNames {
		js.WriteString(extractDashboardJSFunction(t, bodyStr, name))
		js.WriteString("\n")
	}
	js.WriteString(`
renderRuntimeHealth();
const summary = elements.get("runtime-health-summary").innerHTML;
if (!summary.includes("Repo Mutation Actuator Dry Run: blocked / no mutation / no planned mutation")) {
  throw new Error("runtime health summary did not render dry-run actuator diagnostic: " + summary);
}
if (!summary.includes("materialization_preflight_verified")) {
  throw new Error("runtime health summary did not render materialization blocker: " + summary);
}
showRuntimeHealthDetail();
if (!modal || modal.title !== "Runtime Health") {
  throw new Error("runtime health detail modal was not opened");
}
if (!modal.body.includes("Repo Mutation Actuator Dry Run") || !modal.body.includes("materialization_preflight_verified") || !modal.body.includes("mutation_executed")) {
  throw new Error("runtime health detail modal did not render dry-run payload: " + modal.body);
}
`)
	scriptPath := filepath.Join(t.TempDir(), "dashboard-runtime-health-smoke.js")
	if err := os.WriteFile(scriptPath, []byte(js.String()), 0o644); err != nil {
		t.Fatalf("write dashboard runtime health smoke script: %v", err)
	}
	cmd := exec.Command("node", scriptPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("dashboard runtime health JS smoke failed: %v\n%s", err, strings.TrimSpace(string(output)))
	}
}

func TestDashboardWorkspaceOpsFormsPreferCurrentRevisionTokens(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"current_revision: existing && Number(existing.revision || 0) > 0 ? Number(existing.revision) : undefined",
		"current_revision: Number(item.revision || 0) > 0 ? Number(item.revision) : undefined",
		"const item = operatorQueueCache.find(x => x.queue_id === queueId);",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard queue mutation path is missing %q", needle)
		}
	}
}

func TestDashboardKeepsTensionDependencyEventsLive(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"normalized === 'tension.dependency.added'",
		"normalized === 'tension.dependency.removed'",
		"normalized === 'tension.resolved'",
		"normalized === 'tension.dormant'",
		"normalized === 'tension.active'",
		"normalized === 'tension.emergent'",
		"normalized === 'tension.condensed'",
		"'tension.archived','tension.resolved','tension.dormant','tension.active','tension.recovered'",
		"'tension.dependency.added', 'tension.dependency.removed'",
		"'tension.resolved', 'tension.dormant', 'tension.active', 'tension.recovered'",
		"'tension.emergent', 'tension.condensed'",
		"normalized === 'tension.agent.attached'",
		"normalized === 'tension.agent.detached'",
		"'tension.agent.attached','tension.agent.detached'",
		"'tension.agent.attached', 'tension.agent.detached'",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard tension dependency live wiring is missing %q", needle)
		}
	}
}

func TestDashboardTensionActionsUseRunnableLifecycleMethods(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"resolve: 'workspace.tension.resolve'",
		"dormant: 'workspace.tension.dormant'",
		"lifecycle === 'RESOLVED' || lifecycle === 'DISCARDED'",
		"lifecycle === 'ACTIVE' || lifecycle === 'DORMANT'",
		"actOnTension('resolve'",
		"actOnTension('dormant'",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard tension lifecycle action wiring is missing %q", needle)
		}
	}

	for _, needle := range []string{
		"workspace.tension.recover",
		"actOnTension('recover'",
		"workspace.tension.supersede",
		"workspace.tension.dispute",
	} {
		if strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard tension lifecycle action wiring exposes non-runnable method %q", needle)
		}
	}
}

func TestDashboardProjectCrudUsesWorkspaceAndProfileActor(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"const actorID = currentProfileId();",
		"created_by:actorID",
		"rpc('project.get', {workspace_id: WS_ID, project_id: projectId})",
		"await rpc('project.update', {\n      workspace_id: WS_ID,",
		"actor_id: actorID",
		"rpc('project.delete', {workspace_id: WS_ID, project_id: projectId, actor_id: actorID})",
		"'project.created','project.updated','project.deleted'",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard project CRUD wiring is missing %q", needle)
		}
	}

	for _, forbidden := range []string{
		"created_by:currentProfileId() || 'dashboard'",
		"created_by:'dashboard'",
		"rpc('project.delete', {workspace_id: WS_ID, project_id: projectId})",
	} {
		if strings.Contains(bodyStr, forbidden) {
			t.Fatalf("dashboard project CRUD wiring still contains stale fallback %q", forbidden)
		}
	}
}

func extractDashboardJSFunction(t *testing.T, body, name string) string {
	t.Helper()
	marker := "function " + name + "("
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatalf("dashboard JS function %s not found", name)
	}
	open := strings.Index(body[start:], "{")
	if open < 0 {
		t.Fatalf("dashboard JS function %s opening brace not found", name)
	}
	open += start
	depth := 0
	var quote byte
	escaped := false
	lineComment := false
	blockComment := false
	for i := open; i < len(body); i++ {
		ch := body[i]
		next := byte(0)
		if i+1 < len(body) {
			next = body[i+1]
		}
		if lineComment {
			if ch == '\n' || ch == '\r' {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if ch == '*' && next == '/' {
				blockComment = false
				i++
			}
			continue
		}
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '/' && next == '/' {
			lineComment = true
			i++
			continue
		}
		if ch == '/' && next == '*' {
			blockComment = true
			i++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return body[start : i+1]
			}
		}
	}
	t.Fatalf("dashboard JS function %s closing brace not found", name)
	return ""
}

func TestDashboardProjectDetailSurfacesCoordinationAndPatchQueue(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"function renderProjectCoordinationPanel(projectId, coordinationResult)",
		"rpc('project.coordination.get', {workspace_id: WS_ID, project_id: projectId})",
		"function renderProjectPatchQueueItems(projectId, items)",
		"function enableProjectPatchQueueOperator(projectId, queueId, itemId, claimToken)",
		"rpc('operator.patch_queue.enable'",
		"function projectPatchQueueFollowupTasks(projectId, tasks)",
		"function mergeProjectCoordinationTasks(tasks)",
		"function projectCoordinationNumber(value, fallback)",
		"function projectCoordinationJSArg(value)",
		"projectCoordinationNumber(coordination.open_task_count, projectTasks.length)",
		"coordination.patch_queue_items",
		"coordination.tasks",
		"renderProjectPatchQueueItems(projectId, patchItems)",
		"Patch Queue Follow-up Tasks",
		"'project.patch_queue.changed'",
		"'project.patch_queue.accepted'",
		"'project.branch.changed'",
		"'project.repository.changed'",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard project coordination visibility is missing %q", needle)
		}
	}
	if strings.Contains(bodyStr, "rpc('workspace.tasks.list', {workspace_id: WS_ID}).catch(e => ({tasks: _cachedTasks") {
		t.Fatal("project detail should use project.coordination.get tasks instead of a separate workspace.tasks.list fallback")
	}
}

func TestDashboardLimitGroupMutationsUseCurrentProfileActor(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"const actorID = currentProfileId();",
		"actor_id: actorID",
		"rpc('limits.group.delete', {workspace_id: WS_ID, group_id: groupId, actor_id: actorID})",
		"'limits.group.created','limits.group.updated','limits.group.deleted'",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard limit group wiring is missing %q", needle)
		}
	}

}

func TestDashboardAgentDeleteUsesCurrentProfileActor(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"const actorID = currentProfileId();",
		"Select a profile before deleting agents",
		"await rpc('agent.delete', {workspace_id: WS_ID, agent_id: agentId, actor: actorID})",
		"'agent.update','agent.deleted','workspace.change'",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard agent delete wiring is missing %q", needle)
		}
	}

	if strings.Contains(bodyStr, "agent_id: agentId, actor: currentProfileId() || 'dashboard'") {
		t.Fatal("dashboard agent.delete still falls back to dashboard actor")
	}
}

func TestDashboardVaultAndNewsMutationsUseCurrentProfileActor(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"function currentProfileType()",
		"await rpc('vault.create', { workspace_id:WS_ID, title, description:desc, fields_json:JSON.stringify(fields), created_by:actorID });",
		"await rpc('vault.update', {\n      workspace_id: WS_ID,",
		"actor: actorID",
		"await rpc('vault.delete', { workspace_id: WS_ID, entry_id: _editingVaultId, actor: actorID });",
		"await rpc('news.publish', {\n      workspace_id: WS_ID, title: title, content: content,\n      author_id: author, author_type: currentProfileType()",
		"await rpc('news.delete', {workspace_id: WS_ID, news_id: newsId, actor_id: actorID});",
		"'vault.entry.created','vault.entry.updated','vault.entry.deleted','vault.entry.read','vault.entries.listed','news.published','news.deleted'",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard vault/news mutation wiring is missing %q", needle)
		}
	}

	for _, needle := range []string{
		"created_by:'developer'",
		"actor: 'developer'",
		"author_type: 'human'",
		"rpc('news.delete', {news_id: newsId})",
	} {
		if strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard vault/news mutation wiring still contains stale fallback %q", needle)
		}
	}
}

func TestDashboardLogsLoadOnDemandAndRenderVisibleErrors(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"if (name === 'logs') resetRpcLogs();",
		"const msg = esc(e && e.message ? e.message : 'Failed to load log entries');",
		"el.innerHTML = '<div class=\"empty\">'+msg+'</div>';",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard logs on-demand wiring is missing %q", needle)
		}
	}

	if strings.Contains(bodyStr, "loadRpcLogs(false), loadProjects()") {
		t.Fatal("dashboard global refresh still fans rpc logs through the shared refresh storm")
	}
}

func TestDashboardRefreshUsesScheduledRetryInsteadOfRecursiveLoop(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"let _refreshRetryTimer = null;",
		"function scheduleRefreshRetry() {",
		"if (shouldRetry) scheduleRefreshRetry();",
		"_refreshRetryTimer = setTimeout(function() {",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard refresh retry wiring is missing %q", needle)
		}
	}
}

func TestDashboardLogsAppendFailuresRenderVisibleErrors(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		"Failed to load more log entries:",
		"_rpcLogsHasMore = false;",
		"const msg = esc(e && e.message ? e.message : 'Failed to load log entries');",
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard logs append failure wiring is missing %q", needle)
		}
	}
}

func TestDashboardDoesNotShipBootstrapCredentialsOrRemoteFonts(t *testing.T) {
	rec := httptest.NewRecorder()
	server.ServeDashboard()(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	body := rec.Body.String()
	for _, forbidden := range []string{
		"14" + "88",
		"Default workspace password",
		"fonts.googleapis.com",
		"fonts.gstatic.com",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("dashboard contains forbidden bootstrap or remote dependency marker %q", forbidden)
		}
	}
}

func TestDashboardSetsBrowserSecurityHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	server.ServeDashboard()(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))

	for name, want := range map[string]string{
		"Cache-Control":           "no-store",
		"Content-Security-Policy": "default-src 'none'",
		"Permissions-Policy":      "camera=()",
		"Referrer-Policy":         "no-referrer",
		"X-Content-Type-Options":  "nosniff",
		"X-Frame-Options":         "DENY",
	} {
		if got := rec.Header().Get(name); !strings.Contains(got, want) {
			t.Errorf("%s = %q, want it to contain %q", name, got, want)
		}
	}
}

func TestDashboardHasNoDynamicInlineEventHandlers(t *testing.T) {
	rec := httptest.NewRecorder()
	server.ServeDashboard()(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	page := rec.Body.String()
	inlineEvent := regexp.MustCompile(`(?i)\bon[a-z]+\s*=\s*"([^"]*)"`)
	for _, match := range inlineEvent.FindAllStringSubmatch(page, -1) {
		body := match[1]
		if strings.Contains(body, "'+") || strings.Contains(body, "+'") || strings.Contains(body, "${") {
			t.Fatalf("dynamic data is still concatenated into an inline event handler: %s", match[0])
		}
	}
	if !strings.Contains(page, "data-dashboard-action") || !strings.Contains(page, "dashboardActions") {
		t.Fatal("dashboard is missing delegated dynamic-action binding")
	}
}

func TestDashboardDynamicActionsKeepHostileValuesOutOfExecutableSource(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skipf("node executable not available: %v", err)
	}
	rec := httptest.NewRecorder()
	server.ServeDashboard()(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	body := rec.Body.String()

	var js strings.Builder
	js.WriteString(`
let dashboardActionSequence = 0;
const dashboardActions = new Map();
let dashboardActionPruneQueued = false;
const document = { querySelectorAll() { return []; } };
`)
	js.WriteString(extractDashboardJSFunction(t, body, "dashboardAction"))
	js.WriteString(`
const hostile = "'); globalThis.executed = true; //";
const values = {
  news_title: hostile,
  mcp_display_name: hostile,
  mcp_environment_value: hostile,
  project_title: hostile,
  limit_group_title: hostile,
  task_title: hostile,
  vault_key: hostile,
  vault_value: hostile,
  attacker_controlled_id: hostile
};
globalThis.executed = false;
const observed = {};
for (const [surface, value] of Object.entries(values)) {
  const attribute = dashboardAction(function() { observed[surface] = value; });
  if (attribute.includes(value) || attribute.includes('onclick') || attribute.includes('onerror')) {
    throw new Error(surface + ' leaked into executable attribute source: ' + attribute);
  }
  const match = attribute.match(/^data-dashboard-action="([a-z0-9-]+)"$/);
  if (!match) throw new Error(surface + ' emitted an invalid delegated action attribute: ' + attribute);
  const callback = dashboardActions.get(match[1]);
  if (typeof callback !== 'function') throw new Error(surface + ' callback is missing');
  callback({}, {});
  if (observed[surface] !== value) throw new Error(surface + ' closure did not preserve the original value');
}
if (globalThis.executed) throw new Error('hostile value executed');
`)
	scriptPath := filepath.Join(t.TempDir(), "dashboard-delegated-actions.js")
	if err := os.WriteFile(scriptPath, []byte(js.String()), 0o600); err != nil {
		t.Fatalf("write dashboard delegated-action script: %v", err)
	}
	if output, err := exec.Command("node", scriptPath).CombinedOutput(); err != nil {
		t.Fatalf("dashboard delegated-action smoke failed: %v\n%s", err, strings.TrimSpace(string(output)))
	}
}

func TestDashboardInlineScriptParses(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skipf("node executable not available: %v", err)
	}
	rec := httptest.NewRecorder()
	server.ServeDashboard()(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	page := rec.Body.String()
	start := strings.Index(page, "<script>")
	if start < 0 {
		t.Fatal("dashboard inline script opening tag not found")
	}
	start += len("<script>")
	end := strings.Index(page[start:], "</script>")
	if end < 0 {
		t.Fatal("dashboard inline script closing tag not found")
	}
	scriptPath := filepath.Join(t.TempDir(), "dashboard-inline.js")
	if err := os.WriteFile(scriptPath, []byte(page[start:start+end]), 0o600); err != nil {
		t.Fatalf("write dashboard inline script: %v", err)
	}
	if output, err := exec.Command("node", "--check", scriptPath).CombinedOutput(); err != nil {
		t.Fatalf("dashboard inline script has invalid syntax: %v\n%s", err, strings.TrimSpace(string(output)))
	}
}

func TestDashboardTaskTitleUsesEventBindingInsteadOfInlineJavaScript(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skipf("node executable not available: %v", err)
	}
	rec := httptest.NewRecorder()
	server.ServeDashboard()(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	body := rec.Body.String()

	for _, forbidden := range []string{
		`onclick="showTaskDetail(\''+esc(t.task_id)+'\',\''+esc(t.title||t.task_id)+'\')"`,
		`onclick="showTaskDetail(\''+esc(t.task_id)+'\',\''+esc(t.title||t.task_id)+'\')">`,
		`onclick="showTaskDetail(\'' + esc(item.task_id) + '\',\'' + esc(item.title || item.task_id) + '\')"`,
		`onclick="event.preventDefault();closeModal();switchTab(\'tasks\');showTaskDetail(\''+esc(a.task_id)+'\',\''+esc(a.task_title||a.task_id)+'\')"`,
		`onclick="showTaskDetail(\''+projectCoordinationJSArg(task.task_id)+'\',\''+projectCoordinationJSArg(task.title||task.task_id)+'\')"`,
		`onclick="showTaskDetail(\'' + esc(refID) + '\',\'' + esc(n.label || refID) + '\')"`,
		`onclick="showTaskDetail(\'' + esc(focusID) + '\',\'' + esc(graphFocusLabel(focusID)) + '\')"`,
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("dashboard task rendering still embeds task data in inline JavaScript: %s", forbidden)
		}
	}

	var js strings.Builder
	js.WriteString("let captured = null;\n")
	js.WriteString("let executed = false;\n")
	js.WriteString("function showTaskDetail(taskId, title) { captured = { taskId, title }; }\n")
	js.WriteString(extractDashboardJSFunction(t, body, "bindTaskDetailElements"))
	js.WriteString(`
const listeners = [];
const element = { addEventListener(type, handler) {
  if (type !== 'click') throw new Error('unexpected event type: ' + type);
  listeners.push(handler);
} };
const root = { querySelectorAll(selector) {
  if (selector !== '.task-card') throw new Error('unexpected selector: ' + selector);
  return [element];
} };
const hostileTitle = "'); executed = true; //";
bindTaskDetailElements(root, [{ task_id: 'task-safe', title: hostileTitle }], '.task-card');
if (listeners.length !== 1) throw new Error('expected one bound click listener');
let prevented = false;
listeners[0]({ preventDefault() { prevented = true; } });
if (!prevented) throw new Error('bound task click did not suppress link navigation');
if (executed) throw new Error('hostile task title executed as JavaScript');
if (!captured || captured.taskId !== 'task-safe' || captured.title !== hostileTitle) {
  throw new Error('task detail binding did not preserve task data');
}

`)
	scriptPath := filepath.Join(t.TempDir(), "dashboard-task-title-binding.js")
	if err := os.WriteFile(scriptPath, []byte(js.String()), 0o644); err != nil {
		t.Fatalf("write dashboard task-title binding script: %v", err)
	}
	cmd := exec.Command("node", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dashboard task-title binding smoke failed: %v\n%s", err, strings.TrimSpace(string(output)))
	}
}

func TestDashboardIncludesConfigurableLegalSourceNotice(t *testing.T) {
	t.Setenv("RHIZOME_SOURCE_URL", "https://example.invalid/source/revision-abc?one=1&two=2")
	rec := httptest.NewRecorder()
	server.ServeDashboard()(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	page := rec.Body.String()
	for _, want := range []string{
		"Legal &amp; source",
		"Rhizome Project contributors",
		"No warranty",
		"AGPL-3.0-only",
		"https://example.invalid/source/revision-abc?one=1&amp;two=2",
		"Corresponding source for this build",
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("dashboard legal notice missing %q", want)
		}
	}
}

func TestDashboardRejectsUnsafeLegalSourceURL(t *testing.T) {
	t.Setenv("RHIZOME_SOURCE_URL", "javascript:alert(1)")
	rec := httptest.NewRecorder()
	server.ServeDashboard()(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	if !strings.Contains(rec.Body.String(), `href="https://github.com/Rhizome-Project/rhizome-runtime"`) {
		t.Fatal("dashboard did not fall back to the canonical source URL")
	}
}
