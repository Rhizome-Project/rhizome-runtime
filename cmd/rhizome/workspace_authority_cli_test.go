package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceAuthorityEnsureLocalCLIClaimsMissingAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-sh1a-cli-authority-ensure-local"
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "SH1A CLI Authority Ensure Local",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	out, err := captureStdout(t, func() error {
		return runWorkspaceAuthorityEnsureLocal([]string{
			"--workspace-id", workspaceID,
			"--actor-type", "operator",
			"--actor-id", "tests",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceAuthorityEnsureLocal failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	status, err := store.GetLocalWorkspaceAuthorityStatus(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get local workspace authority status: %v", err)
	}
	if status.Authority == nil || !status.LocalHolder || !status.LeaseLive {
		t.Fatalf("expected live local authority after CLI ensure-local, got %+v", status)
	}
	assertWorkspaceAuthorityCLIOutputSafe(t, out, status.Authority.LeaseToken)
}

func TestWorkspaceAuthorityStatusCLIOutputRedactsLeaseToken(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-cli-authority-status-redaction"
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "CLI Authority Status Redaction",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	authority := claimCLITestWorkspaceAuthority(t, workspaceID)

	out, err := captureStdout(t, func() error {
		return runWorkspaceAuthorityStatus([]string{"--workspace-id", workspaceID})
	})
	if err != nil {
		t.Fatalf("runWorkspaceAuthorityStatus failed: %v", err)
	}
	assertWorkspaceAuthorityCLIOutputSafe(t, out, authority.LeaseToken)
}

func TestWorkspaceAuthorityForceBreakCLIExpiresCurrentHolder(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-sh1a-cli-authority-force-break"
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "SH1A CLI Authority Force Break",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	current := claimCLITestWorkspaceAuthority(t, workspaceID)
	const peerNodeID = "authnode-999-7201"
	transferCLITestWorkspaceAuthorityToPeer(t, workspaceID, current, peerNodeID)

	out, err := captureStdout(t, func() error {
		return runWorkspaceAuthorityForceBreak([]string{
			"--workspace-id", workspaceID,
			"--actor-type", "operator",
			"--actor-id", "tests",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceAuthorityForceBreak failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	status, err := store.GetLocalWorkspaceAuthorityStatus(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("get local workspace authority status: %v", err)
	}
	if status.Authority == nil || status.Authority.Status != sqlite.WorkspaceAuthorityStatusExpired {
		t.Fatalf("expected expired authority after CLI force-break, got %+v", status)
	}
	assertWorkspaceAuthorityCLIOutputSafe(t, out, "lease-"+peerNodeID)
}

func TestWorkspaceAuthorityMaintainOnceCLIReclaimsStaleSessionOwnership(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-sh4c-cli-maintain-once-reclaim"
		agentID     = "agent-sh4c-cli-maintain-once-reclaim"
		sessionID   = "sess-sh4c-cli-maintain-once-reclaim"
		taskID      = "task-sh4c-cli-maintain-once-reclaim"
	)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "SH4C CLI Maintain Once Reclaim",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	authority := claimCLITestWorkspaceAuthority(t, workspaceID)
	if err := runAgentRegister([]string{
		"--workspace-id", workspaceID,
		"--agent-id", agentID,
		"--owner-user-id", "developer",
		"--display-name", "SH4C Maintain Once Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "SH4C Maintain Once Task",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "Claim before reclaim drill",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	staleAt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Started before reclaim drill",
		UpdatedAt:   staleAt,
	}); err != nil {
		t.Fatalf("record session start: %v", err)
	}
	blockedState, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventBlocked,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "Blocked before reclaim drill",
		BlockedOn:   []model.AgentUpdateBlockedRef{{Kind: "bridge", Detail: "wake timeout"}},
		UpdatedAt:   staleAt,
	})
	if err != nil {
		t.Fatalf("record blocked session: %v", err)
	}
	if _, err := store.SyncOperatorQueueFromSessionState(ctx, blockedState); err != nil {
		t.Fatalf("sync operator queue: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE agents SET last_seen_at = ?, updated_at = ?, status = 'active' WHERE workspace_id = ? AND agent_id = ?`,
		staleAt,
		staleAt,
		workspaceID,
		agentID,
	); err != nil {
		t.Fatalf("age agent last_seen_at: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runWorkspaceAuthorityMaintainOnce(nil)
	})
	if err != nil {
		t.Fatalf("runWorkspaceAuthorityMaintainOnce failed: %v", err)
	}
	assertWorkspaceAuthorityCLIOutputSafe(t, out, authority.LeaseToken)

	var payload struct {
		Result struct {
			LeaseMaintenance struct {
				ReferenceAt string `json:"reference_at"`
			} `json:"lease_maintenance"`
			SessionReclaim struct {
				SessionsEnded         int `json:"sessions_ended"`
				SessionQueuesResolved int `json:"session_queues_resolved"`
				TaskClaimsReleased    int `json:"task_claims_released"`
			} `json:"session_reclaim"`
			OrphanClaim struct {
				TaskClaimsReleased int `json:"task_claims_released"`
			} `json:"orphan_claim_reclaim"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode maintain-once output: %v; output=%q", err, out)
	}
	if strings.TrimSpace(payload.Result.LeaseMaintenance.ReferenceAt) == "" {
		t.Fatalf("expected lease maintenance reference_at in output, got %+v", payload.Result.LeaseMaintenance)
	}
	if payload.Result.SessionReclaim.SessionsEnded != 1 || payload.Result.SessionReclaim.SessionQueuesResolved != 1 || payload.Result.SessionReclaim.TaskClaimsReleased != 1 {
		t.Fatalf("expected session reclaim counters in output, got %+v", payload.Result.SessionReclaim)
	}
	if payload.Result.OrphanClaim.TaskClaimsReleased != 0 {
		t.Fatalf("expected no orphan-claim release in output, got %+v", payload.Result.OrphanClaim)
	}

	state, err := store.GetAgentSessionState(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get session state after maintain-once: %v", err)
	}
	if state.Status != model.SessionStatusEnded {
		t.Fatalf("expected maintain-once reclaim to end stale session, got %+v", state)
	}

	var claimStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT claim_status FROM task_claims WHERE workspace_id = ? AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&claimStatus); err != nil {
		t.Fatalf("query task claim after maintain-once: %v", err)
	}
	if claimStatus != model.TaskClaimStatusReleased {
		t.Fatalf("expected maintain-once reclaim to release task claim, got %q", claimStatus)
	}

	var queueStatus, resolvedBy string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, COALESCE(resolved_by,'') FROM operator_queue_items WHERE workspace_id = ? AND queue_key = ?`,
		workspaceID,
		"session:"+sessionID+":blocker",
	).Scan(&queueStatus, &resolvedBy); err != nil {
		t.Fatalf("query operator queue after maintain-once: %v", err)
	}
	if queueStatus != "RESOLVED" || resolvedBy != "local_lease_manager" {
		t.Fatalf("expected maintain-once reclaim to resolve blocked operator queue, got status=%q resolved_by=%q", queueStatus, resolvedBy)
	}
}

func assertWorkspaceAuthorityCLIOutputSafe(t *testing.T, output, leaseToken string) {
	t.Helper()
	if strings.TrimSpace(leaseToken) == "" {
		t.Fatal("test requires a non-empty lease token sentinel")
	}
	if strings.Contains(output, leaseToken) {
		t.Fatalf("workspace authority CLI output leaked raw lease token %q: %s", leaseToken, output)
	}
	if strings.Contains(output, `"lease_token":`) {
		t.Fatalf("workspace authority CLI output retained a raw lease_token field: %s", output)
	}
	fingerprint := workspaceAuthorityCLILeaseTokenFingerprint(leaseToken)
	if !strings.Contains(output, fingerprint) {
		t.Fatalf("workspace authority CLI output omitted useful token fingerprint %q: %s", fingerprint, output)
	}
	if !strings.Contains(output, `"status"`) {
		t.Fatalf("workspace authority CLI output omitted authority status: %s", output)
	}
}

func TestSafeWorkspaceAuthorityCLIEventRejectsPoisonedFingerprint(t *testing.T) {
	const sentinel = "raw-lease-token-in-fingerprint-field"
	event := &sqlite.RuntimeEventRecord{
		EventID:                        "event-poisoned-fingerprint",
		AuthorityLeaseTokenFingerprint: sentinel,
		PayloadJSON:                    `{"lease_token":"` + sentinel + `"}`,
	}

	encoded, err := json.Marshal(safeWorkspaceAuthorityCLIEvent(event, ""))
	if err != nil {
		t.Fatalf("marshal safe event: %v", err)
	}
	if strings.Contains(string(encoded), sentinel) || strings.Contains(string(encoded), `"payload_json"`) {
		t.Fatalf("safe event retained poisoned authority data: %s", encoded)
	}
	if strings.Contains(string(encoded), `"authority_lease_token_fingerprint"`) {
		t.Fatalf("safe event retained a non-canonical fingerprint: %s", encoded)
	}

	const fallbackToken = "lease-token-fallback-sentinel"
	encoded, err = json.Marshal(safeWorkspaceAuthorityCLIEvent(event, fallbackToken))
	if err != nil {
		t.Fatalf("marshal safe event with fallback: %v", err)
	}
	if strings.Contains(string(encoded), sentinel) || strings.Contains(string(encoded), fallbackToken) {
		t.Fatalf("safe event exposed raw authority data with fallback: %s", encoded)
	}
	if !strings.Contains(string(encoded), workspaceAuthorityCLILeaseTokenFingerprint(fallbackToken)) {
		t.Fatalf("safe event omitted recomputed fallback fingerprint: %s", encoded)
	}
}

func transferCLITestWorkspaceAuthorityToPeer(t *testing.T, workspaceID string, current sqlite.WorkspaceAuthorityRecord, peerNodeID string) {
	t.Helper()

	dbPath := strings.TrimSpace(os.Getenv("RHIZOME_DB"))
	if dbPath == "" {
		t.Fatal("RHIZOME_DB is not set")
	}
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open cli test store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	referenceAt := time.Now().UTC().Round(0)
	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head before transfer: %v", err)
	}
	commitWatermark := current.CommitWatermark + 1
	if journalHead > commitWatermark {
		commitWatermark = journalHead
	}
	appliedWatermark := current.AppliedWatermark + 1
	if appliedWatermark > commitWatermark {
		appliedWatermark = commitWatermark
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-"+peerNodeID,
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        "workspace",
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-" + peerNodeID,
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    "system",
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority to peer: %v", err)
	}
}
