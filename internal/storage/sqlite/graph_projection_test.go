package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestGetGraphSnapshot_NonEmpty(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-test"

	err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Test Workspace",
		CreatedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "testuser",
		DisplayName:       "Test User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	err = store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: wsID,
		AgentID:     "agent-test-1",
		OwnerUserID: human.UserID,
		DisplayName: "Graph Test Agent",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	// Insert task via SQL
	_, err = store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-1', 'Test Graph Task', ?, 'OPEN', 'high', 'standard', '2026-04-05T00:00:00Z', '2026-04-05T00:00:00Z')`, human.UserID)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}
	_, err = store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-1', ?, '2026-04-05T00:00:00Z')`, wsID, human.UserID)
	if err != nil {
		t.Fatalf("insert workspace_task: %v", err)
	}

	// Fetch snapshot and verify there is data
	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "SYSTEM",
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	if snap == nil {
		t.Fatalf("Expected non-nil GraphSnapshot")
	}

	if len(snap.Nodes) == 0 {
		t.Errorf("Expected non-empty GraphNodes list, maybe projection SQL queries are wrong?")
	}

	// Verify we got our agent
	agentFound := false
	taskFound := false
	for _, n := range snap.Nodes {
		if n.Type == "agent" && n.ID == "agent-test-1" {
			agentFound = true
		}
		if n.Type == "task" && n.Label == "Test Graph Task" {
			taskFound = true
		}
	}

	if !agentFound {
		t.Errorf("Agent not found in graph projection")
	}
	if !taskFound {
		t.Errorf("Task not found in graph projection")
	}

	if snap.TimeAuthority == nil {
		t.Errorf("TimeAuthority was nil, expected populated timestamp or struct")
	}
}

func TestGetGraphSnapshot_NormalizesModeAndKeepsClaimEdges(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-mode"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Mode Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "modeuser",
		DisplayName:       "Mode User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: wsID,
		AgentID:     "agent-mode-1",
		OwnerUserID: human.UserID,
		DisplayName: "Mode Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-mode-1', 'Mode Task', ?, 'RUNNING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-mode-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace_task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO agent_sessions (session_id, agent_id, workspace_id, task_id, status, started_at) VALUES ('s1', 'agent-mode-1', ?, 'task-mode-1', 'RUNNING', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO task_claims (task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at) VALUES ('task-mode-1', ?, 'agent-mode-1', 'CLAIMED', 'claim', '2026-04-06T00:00:00Z', NULL, '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert claim: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "system",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	if snap.Mode != "SYSTEM" {
		t.Fatalf("expected normalized mode SYSTEM, got %q", snap.Mode)
	}

	sessionFound := false
	claimFound := false
	for _, node := range snap.Nodes {
		if node.ID == "s1" && node.Label == "Session s1" {
			sessionFound = true
		}
	}
	for _, edge := range snap.Edges {
		if edge.Source == "agent-mode-1" && edge.Target == "task-mode-1" && edge.Label == "claims_task" {
			claimFound = true
		}
	}

	if !sessionFound {
		t.Fatalf("expected short session id label to be preserved")
	}
	if !claimFound {
		t.Fatalf("expected CLAIMED task to appear as graph edge")
	}
}

func TestGetGraphSnapshot_SystemIncludesPendingBlockingHumanActions(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-system-blockers"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph System Blockers Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "blockeruser",
		DisplayName:       "Blocker User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: wsID,
		AgentID:     "agent-blocker-1",
		OwnerUserID: human.UserID,
		DisplayName: "Blocker Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-blocker-1', 'Blocked Task', ?, 'BLOCKED', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-blocker-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO agent_sessions (session_id, agent_id, workspace_id, task_id, status, started_at) VALUES ('session-blocker-1', 'agent-blocker-1', ?, 'task-blocker-1', 'BLOCKED', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert session: %v", err)
	}

	actionID := "action-blocker-1"
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO human_actions(action_id, workspace_id, task_id, agent_id, assigned_to, title, description, blocking, status, created_at) VALUES (?, ?, 'task-blocker-1', 'agent-blocker-1', ?, 'Need human approval', 'Waiting on operator decision', 1, 'PENDING', '2026-04-06T00:00:00Z')`, actionID, wsID, human.UserID); err != nil {
		t.Fatalf("insert human action: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "SYSTEM",
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	actionNodeID := graphNodeIDByTypeRef(snap.Nodes, "action", actionID)
	humanNodeID := graphNodeIDByTypeRef(snap.Nodes, "human", human.UserID)
	if actionNodeID == "" {
		t.Fatalf("expected blocking action node in system graph")
	}
	if humanNodeID == "" {
		t.Fatalf("expected assigned human node in system graph")
	}
	if !graphHasEdge(snap.Edges, "session-blocker-1", "task-blocker-1", "works_on_task") {
		t.Fatalf("expected session -> task edge in system graph")
	}

	blockedEdgeFound := false
	awaitingHumanEdgeFound := false
	for _, edge := range snap.Edges {
		if edge.Source == "task-blocker-1" && edge.Target == actionNodeID && edge.Label == "blocked_by_action" {
			blockedEdgeFound = true
			if edge.Semantics != "warning" {
				t.Fatalf("expected blocked_by_action warning semantics, got %+v", edge)
			}
		}
		if edge.Source == humanNodeID && edge.Target == actionNodeID && edge.Label == "awaiting_human" {
			awaitingHumanEdgeFound = true
			if edge.Semantics != "muted" {
				t.Fatalf("expected awaiting_human muted semantics, got %+v", edge)
			}
		}
	}

	if !blockedEdgeFound {
		t.Fatalf("expected task -> blocking action edge in system graph")
	}
	if !awaitingHumanEdgeFound {
		t.Fatalf("expected human -> blocker action edge in system graph")
	}
}

func TestGetGraphSnapshot_SystemAllowsNullSessionTaskID(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-null-session-task"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Null Session Task Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "nulltaskuser",
		DisplayName:       "Null Task User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: wsID,
		AgentID:     "agent-null-task-1",
		OwnerUserID: human.UserID,
		DisplayName: "Null Task Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO agent_sessions (session_id, agent_id, workspace_id, task_id, status, started_at) VALUES ('session-null-task-1', 'agent-null-task-1', ?, NULL, 'RUNNING', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert null-task session: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "SYSTEM",
		Limit:       100,
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot should tolerate NULL session task_id: %v", err)
	}

	if graphNodeIDByTypeRef(snap.Nodes, "session", "session-null-task-1") == "" {
		t.Fatalf("expected session node for null-task session")
	}
	if !graphHasEdge(snap.Edges, "agent-null-task-1", "session-null-task-1", "runs_session") {
		t.Fatalf("expected agent -> session edge for null-task session")
	}
}

func TestGetGraphSnapshot_IncludesAffinityEdgesForScoredTensions(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-affinity"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Affinity Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "affinityuser",
		DisplayName:       "Affinity User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: wsID,
		AgentID:     "agent-affinity-1",
		OwnerUserID: human.UserID,
		DisplayName: "Affinity Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-affinity-1', 'Affinity Task', ?, 'PENDING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-affinity-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tensions (tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary, task_ids_json, session_ids_json, agent_ids_json, created_at, updated_at) VALUES ('tension-affinity-1', ?, 'cluster-affinity-1', 'feature', 'ACTIVE', 'PENDING', 'Affinity Tension', '', '["task-affinity-1"]', '[]', '[]', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert tension: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "SYSTEM",
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	affinityFound := false
	for _, edge := range snap.Edges {
		if edge.Source == "agent-affinity-1" && edge.Target == "tension-affinity-1" && edge.Label == "candidate_for" {
			affinityFound = true
			if edge.Semantics != "affinity" || edge.Authority != "inferred" || edge.SourceModel != "attachment" {
				t.Fatalf("unexpected affinity edge metadata %+v", edge)
			}
			if edge.FitScore == nil || edge.SemanticDistance == nil {
				t.Fatalf("expected fit and semantic distance on affinity edge %+v", edge)
			}
			if edge.Strength <= 0 {
				t.Fatalf("expected positive affinity strength %+v", edge)
			}
			if !edge.HiddenByDefault {
				t.Fatalf("expected affinity edge to be hidden by default %+v", edge)
			}
		}
	}

	if !affinityFound {
		t.Fatalf("expected agent->tension affinity edge in graph snapshot")
	}
}

func TestGetGraphSnapshot_IncludesSurfaceAndPressureEdges(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-surface"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Surface Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "surfaceuser",
		DisplayName:       "Surface User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-surface-1', 'Surface Task', ?, 'PENDING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-surface-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tensions (tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary, task_ids_json, session_ids_json, agent_ids_json, surface_score, evidence_count, created_at, updated_at) VALUES ('tension-surface-1', ?, 'cluster-surface-1', 'feature', 'ACTIVE', 'PENDING', 'Surface Tension', '', '["task-surface-1"]', '[]', '[]', 72, 5, '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert tension: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_cluster_control_state(
			workspace_id, proto_cluster_id, resolution_kind, corridor_profile, epoch,
			current_mode, candidate_mode, candidate_streak, attention_band, pressure_score,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, agent_ids_json,
			summary, last_basis_at, last_tick_at, last_transition_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		wsID, "cluster-surface-1", "proto_cluster", "exploration", 1,
		"UNFREEZE", "UNFREEZE", 2, "WATCH", 81,
		`["task-surface-1"]`, `[]`, `[]`, `[]`, `[]`,
		"surface pressure fixture", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z",
	); err != nil {
		t.Fatalf("insert cluster control state: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "SYSTEM",
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	clusterFound := false
	surfaceFound := false
	pressureFound := false
	for _, node := range snap.Nodes {
		if node.ID == "cluster-surface-1" && node.Type == "proto_cluster" && node.Status == "WATCH" {
			clusterFound = true
		}
	}
	for _, edge := range snap.Edges {
		if edge.Source == "task-surface-1" && edge.Target == "tension-surface-1" && edge.Label == "surfaces" {
			surfaceFound = true
			if edge.Semantics != "affinity" || edge.Authority != "derived" || edge.SourceModel != "surface" {
				t.Fatalf("unexpected surface edge metadata %+v", edge)
			}
			if edge.Strength <= 0 || edge.EvidenceCount != 5 || !edge.HiddenByDefault {
				t.Fatalf("unexpected surface edge payload %+v", edge)
			}
		}
		if edge.Source == "cluster-surface-1" && edge.Target == "tension-surface-1" && edge.Label == "pressure_on" {
			pressureFound = true
			if edge.Semantics != "affinity" || edge.Authority != "derived" || edge.SourceModel != "control" {
				t.Fatalf("unexpected pressure edge metadata %+v", edge)
			}
			if edge.Strength <= 0 || !edge.HiddenByDefault {
				t.Fatalf("unexpected pressure edge payload %+v", edge)
			}
		}
	}

	if !clusterFound {
		t.Fatalf("expected proto-cluster node in graph snapshot")
	}
	if !surfaceFound {
		t.Fatalf("expected task->tension surface edge in graph snapshot")
	}
	if !pressureFound {
		t.Fatalf("expected cluster->tension pressure edge in graph snapshot")
	}
}

func graphNodeIDByTypeRef(nodes []sqlite.GraphNode, nodeType, refID string) string {
	for _, node := range nodes {
		candidate := node.RefID
		if candidate == "" {
			candidate = node.ID
		}
		if node.Type == nodeType && candidate == refID {
			return node.ID
		}
	}
	return ""
}

func graphHasEdge(edges []sqlite.GraphEdge, source, target, label string) bool {
	for _, edge := range edges {
		if edge.Source == source && edge.Target == target && edge.Label == label {
			return true
		}
	}
	return false
}

func TestGetGraphSnapshot_TaskFocusIncludesTaskNeighborhood(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-task-focus"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Task Focus Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, wsID)

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "focususer",
		DisplayName:       "Focus User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: wsID,
		AgentID:     "agent-focus-1",
		OwnerUserID: human.UserID,
		DisplayName: "Focus Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	taskID := "task-focus-1"
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES (?, 'Focus Task', ?, 'RUNNING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, taskID, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, ?, ?, '2026-04-06T00:00:00Z')`, wsID, taskID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, wsID)
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO agent_sessions (session_id, agent_id, workspace_id, task_id, status, started_at) VALUES ('session-focus-1', 'agent-focus-1', ?, ?, 'RUNNING', '2026-04-06T00:00:00Z')`, wsID, taskID); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO task_claims (task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at) VALUES (?, ?, 'agent-focus-1', 'CLAIMED', 'claim', '2026-04-06T00:00:00Z', NULL, '2026-04-06T00:00:00Z')`, taskID, wsID); err != nil {
		t.Fatalf("insert claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO dag_nodes(node_id, task_id, node_type, status, attempt_count, last_error, created_at, updated_at) VALUES ('plan', ?, 'PLAN', 'PENDING', 0, NULL, '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, taskID); err != nil {
		t.Fatalf("insert dag node plan: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO dag_nodes(node_id, task_id, node_type, status, attempt_count, last_error, created_at, updated_at) VALUES ('apply', ?, 'EXECUTE', 'PENDING', 0, NULL, '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, taskID); err != nil {
		t.Fatalf("insert dag node apply: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO node_dependencies(task_id, node_id, depends_on_node_id) VALUES (?, 'apply', 'plan')`, taskID); err != nil {
		t.Fatalf("insert node dependency: %v", err)
	}
	if err := store.ClaimNode(ctx, sqlite.NodeClaimInput{
		WorkspaceID: wsID,
		TaskID:      taskID,
		NodeID:      "plan",
		AgentID:     "agent-focus-1",
		Summary:     "working plan",
	}); err != nil {
		t.Fatalf("claim node: %v", err)
	}
	actionID, err := store.CreateHumanAction(ctx, sqlite.HumanActionInput{
		WorkspaceID: wsID,
		TaskID:      taskID,
		AgentID:     "agent-focus-1",
		Title:       "Need operator unblock",
		Description: "Manual approval required",
		Blocking:    true,
	})
	if err != nil {
		t.Fatalf("create human action: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tensions (tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary, task_ids_json, session_ids_json, agent_ids_json, surface_score, evidence_count, created_at, updated_at) VALUES ('tension-focus-1', ?, 'cluster-focus-1', 'feature', 'ACTIVE', 'PENDING', 'Focus Tension', '', '["task-focus-1"]', '["session-focus-1"]', '["agent-focus-1"]', 67, 3, '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert tension: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_cluster_control_state(
			workspace_id, proto_cluster_id, resolution_kind, corridor_profile, epoch,
			current_mode, candidate_mode, candidate_streak, attention_band, pressure_score,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, agent_ids_json,
			summary, last_basis_at, last_tick_at, last_transition_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		wsID, "cluster-focus-1", "proto_cluster", "exploration", 1,
		"UNFREEZE", "UNFREEZE", 2, "WATCH", 74,
		`["task-focus-1"]`, `["session-focus-1"]`, `[]`, `[]`, `["agent-focus-1"]`,
		"focus pressure fixture", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z",
	); err != nil {
		t.Fatalf("insert cluster control state: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "TASK_FOCUS",
		FocusID:     taskID,
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}
	if snap.Mode != "TASK_FOCUS" {
		t.Fatalf("expected task focus mode, got %q", snap.Mode)
	}
	if snap.Focus != taskID {
		t.Fatalf("expected focus %q, got %q", taskID, snap.Focus)
	}
	stats, ok := snap.Stats.(map[string]any)
	if !ok {
		t.Fatalf("expected stats map, got %#v", snap.Stats)
	}
	if supports, ok := stats["supports_focus"].(bool); !ok || !supports {
		t.Fatalf("expected supports_focus=true in stats, got %#v", stats["supports_focus"])
	}

	taskNodeID := graphNodeIDByTypeRef(snap.Nodes, "task", taskID)
	sessionNodeID := graphNodeIDByTypeRef(snap.Nodes, "session", "session-focus-1")
	planNodeID := graphNodeIDByTypeRef(snap.Nodes, "dag_node", "plan")
	applyNodeID := graphNodeIDByTypeRef(snap.Nodes, "dag_node", "apply")
	actionNodeID := graphNodeIDByTypeRef(snap.Nodes, "action", actionID)
	tensionNodeID := graphNodeIDByTypeRef(snap.Nodes, "tension", "tension-focus-1")
	clusterNodeID := graphNodeIDByTypeRef(snap.Nodes, "proto_cluster", "cluster-focus-1")

	for label, nodeID := range map[string]string{
		"task":          taskNodeID,
		"session":       sessionNodeID,
		"dag plan":      planNodeID,
		"dag apply":     applyNodeID,
		"action":        actionNodeID,
		"tension":       tensionNodeID,
		"proto cluster": clusterNodeID,
	} {
		if nodeID == "" {
			t.Fatalf("expected %s node in task focus graph", label)
		}
	}

	if !graphHasEdge(snap.Edges, "agent-focus-1", taskNodeID, "claims_task") {
		t.Fatalf("expected claims_task edge in task focus graph")
	}
	if !graphHasEdge(snap.Edges, sessionNodeID, taskNodeID, "works_on_task") {
		t.Fatalf("expected works_on_task edge in task focus graph")
	}
	if !graphHasEdge(snap.Edges, taskNodeID, planNodeID, "contains_node") {
		t.Fatalf("expected task -> dag_node containment edge")
	}
	if !graphHasEdge(snap.Edges, planNodeID, applyNodeID, "depends_on") {
		t.Fatalf("expected dag dependency edge")
	}
	if !graphHasEdge(snap.Edges, "agent-focus-1", planNodeID, "claims_node") {
		t.Fatalf("expected agent -> dag_node claim edge")
	}
	if !graphHasEdge(snap.Edges, taskNodeID, actionNodeID, "blocked_by_action") {
		t.Fatalf("expected blocking action edge")
	}
	if !graphHasEdge(snap.Edges, tensionNodeID, taskNodeID, "requires") {
		t.Fatalf("expected tension -> task requires edge")
	}
	if !graphHasEdge(snap.Edges, taskNodeID, tensionNodeID, "surfaces") {
		t.Fatalf("expected task -> tension surface edge")
	}
	if !graphHasEdge(snap.Edges, clusterNodeID, tensionNodeID, "pressure_on") {
		t.Fatalf("expected cluster -> tension pressure edge")
	}
}

func TestGetGraphSnapshot_ControlFocusIncludesClusterNeighborhood(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-control-focus"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Control Focus Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "controluser",
		DisplayName:       "Control User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: wsID,
		AgentID:     "agent-control-1",
		OwnerUserID: human.UserID,
		DisplayName: "Control Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-control-1', 'Control Task', ?, 'RUNNING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-control-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO agent_sessions (session_id, agent_id, workspace_id, task_id, status, started_at) VALUES ('session-control-1', 'agent-control-1', ?, 'task-control-1', 'RUNNING', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_cluster_control_state(
			workspace_id, proto_cluster_id, resolution_kind, corridor_profile, epoch,
			current_mode, candidate_mode, candidate_streak, attention_band, pressure_score,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, agent_ids_json,
			summary, last_basis_at, last_tick_at, last_transition_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		wsID, "cluster-control-1", "proto_cluster", "execution", 2,
		"FREEZE", "FREEZE", 3, "HOT", 88,
		`["task-control-1"]`, `["session-control-1"]`, `[]`, `[]`, `["agent-control-1"]`,
		"control neighborhood fixture", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z",
	); err != nil {
		t.Fatalf("insert cluster control state: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tensions (tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary, task_ids_json, session_ids_json, agent_ids_json, surface_score, evidence_count, created_at, updated_at) VALUES ('tension-control-1', ?, 'cluster-control-1', 'feature', 'ACTIVE', 'PENDING', 'Control Tension', '', '["task-control-1"]', '["session-control-1"]', '["agent-control-1"]', 63, 4, '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert tension: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "CONTROL",
		FocusID:     "cluster-control-1",
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}
	if snap.Mode != "CONTROL" {
		t.Fatalf("expected control mode, got %q", snap.Mode)
	}
	if snap.Focus != "cluster-control-1" {
		t.Fatalf("expected control focus cluster-control-1, got %q", snap.Focus)
	}

	stats, ok := snap.Stats.(map[string]any)
	if !ok {
		t.Fatalf("expected stats map, got %#v", snap.Stats)
	}
	if got := stats["focus_type"]; got != "proto_cluster" {
		t.Fatalf("expected proto_cluster focus type, got %#v", got)
	}

	clusterNodeID := graphNodeIDByTypeRef(snap.Nodes, "proto_cluster", "cluster-control-1")
	taskNodeID := graphNodeIDByTypeRef(snap.Nodes, "task", "task-control-1")
	sessionNodeID := graphNodeIDByTypeRef(snap.Nodes, "session", "session-control-1")
	tensionNodeID := graphNodeIDByTypeRef(snap.Nodes, "tension", "tension-control-1")
	agentNodeID := graphNodeIDByTypeRef(snap.Nodes, "agent", "agent-control-1")

	for label, nodeID := range map[string]string{
		"cluster": clusterNodeID,
		"task":    taskNodeID,
		"session": sessionNodeID,
		"tension": tensionNodeID,
		"agent":   agentNodeID,
	} {
		if nodeID == "" {
			t.Fatalf("expected %s node in control graph", label)
		}
	}

	if !graphHasEdge(snap.Edges, clusterNodeID, taskNodeID, "tracks_task") {
		t.Fatalf("expected cluster -> task edge in control graph")
	}
	if !graphHasEdge(snap.Edges, clusterNodeID, sessionNodeID, "observes_session") {
		t.Fatalf("expected cluster -> session edge in control graph")
	}
	if !graphHasEdge(snap.Edges, agentNodeID, clusterNodeID, "stewards_cluster") {
		t.Fatalf("expected agent -> cluster edge in control graph")
	}
	if !graphHasEdge(snap.Edges, agentNodeID, sessionNodeID, "runs_session") {
		t.Fatalf("expected agent -> session edge in control graph")
	}
	if !graphHasEdge(snap.Edges, tensionNodeID, taskNodeID, "requires") {
		t.Fatalf("expected tension -> task edge in control graph")
	}
	if !graphHasEdge(snap.Edges, taskNodeID, tensionNodeID, "surfaces") {
		t.Fatalf("expected task -> tension surface edge in control graph")
	}
	if !graphHasEdge(snap.Edges, clusterNodeID, tensionNodeID, "pressure_on") {
		t.Fatalf("expected cluster -> tension pressure edge in control graph")
	}
}

func TestGetGraphSnapshot_ControlFallsBackToAdvisoryClusters(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-control-advisory"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Control Advisory Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "controlfallback",
		DisplayName:       "Control Fallback User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-control-fallback-1', 'Control Fallback Task', ?, 'PENDING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-control-fallback-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tensions (tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary, task_ids_json, session_ids_json, agent_ids_json, surface_score, evidence_count, created_at, updated_at) VALUES ('tension-control-fallback-1', ?, 'cluster-control-fallback-1', 'feature', 'ACTIVE', 'PENDING', 'Fallback Tension', '', '["task-control-fallback-1"]', '[]', '[]', 58, 2, '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert tension: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "CONTROL",
		FocusID:     "cluster-control-fallback-1",
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	clusterNodeID := graphNodeIDByTypeRef(snap.Nodes, "proto_cluster", "cluster-control-fallback-1")
	taskNodeID := graphNodeIDByTypeRef(snap.Nodes, "task", "task-control-fallback-1")
	tensionNodeID := graphNodeIDByTypeRef(snap.Nodes, "tension", "tension-control-fallback-1")
	if clusterNodeID == "" || taskNodeID == "" || tensionNodeID == "" {
		t.Fatalf("expected advisory fallback to materialize cluster/task/tension nodes, got %+v", snap.Nodes)
	}
	if !graphHasEdge(snap.Edges, clusterNodeID, taskNodeID, "tracks_task") {
		t.Fatalf("expected advisory fallback cluster -> task edge")
	}
	if !graphHasEdge(snap.Edges, tensionNodeID, taskNodeID, "requires") {
		t.Fatalf("expected advisory fallback tension -> task edge")
	}
}

func TestGetGraphSnapshot_HumanizesProtoClusterLabels(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-control-labels"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Control Labels Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "controllabels",
		DisplayName:       "Control Labels User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	clusterID := "task:" + wsID + "/task-label-1"
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-label-1', 'Label Task', ?, 'PENDING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-label-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_cluster_control_state(
			workspace_id, proto_cluster_id, resolution_kind, corridor_profile, epoch,
			current_mode, candidate_mode, candidate_streak, attention_band, pressure_score,
			task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, agent_ids_json,
			summary, last_basis_at, last_tick_at, last_transition_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		wsID, clusterID, "task", "execution", 1,
		"FREEZE", "FREEZE", 1, "WATCH", 55,
		`["task-label-1"]`, `[]`, `[]`, `[]`, `[]`,
		"label fixture", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z", "2026-04-06T00:00:00Z",
	); err != nil {
		t.Fatalf("insert cluster control state: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "CONTROL",
		FocusID:     clusterID,
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	label := ""
	for _, node := range snap.Nodes {
		if node.Type == "proto_cluster" && node.RefID == clusterID {
			label = node.Label
			break
		}
	}
	if label == "" {
		t.Fatalf("expected proto-cluster node in control graph")
	}
	if label != "Task Cluster: task-label-1" {
		t.Fatalf("expected humanized cluster label, got %q", label)
	}
}

func TestGetGraphSnapshot_MemoryOverlayIncludesAnchoredMemoryNodes(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-memory-overlay"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Memory Overlay Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "memoryoverlay",
		DisplayName:       "Memory Overlay User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: wsID,
		AgentID:     "agent-memory-1",
		OwnerUserID: human.UserID,
		DisplayName: "Memory Overlay Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-memory-1', 'Memory Overlay Task', ?, 'RUNNING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-memory-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO agent_sessions (session_id, agent_id, workspace_id, task_id, status, started_at) VALUES ('session-memory-1', 'agent-memory-1', ?, 'task-memory-1', 'RUNNING', '2026-04-06T00:00:00Z')`, wsID); err != nil {
		t.Fatalf("insert session: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: wsID,
		MemoryType:  "update_digest",
		Title:       "Handoff for deploy",
		Body:        "Deployment handoff needs follow-up verification and issue triage.",
		Summary:     "Deploy handoff",
		AgentID:     "agent-memory-1",
		SessionID:   "session-memory-1",
		TaskID:      "task-memory-1",
		SourceKind:  "session_event",
		SourceID:    "session-memory-1",
		Importance:  0.84,
		Confidence:  0.91,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "MEMORY_OVERLAY",
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	if snap.Mode != "MEMORY_OVERLAY" {
		t.Fatalf("expected memory overlay mode, got %q", snap.Mode)
	}

	memoryRefID := "memnode:workspace_memory:" + record.MemoryID
	memoryNodeID := graphNodeIDByTypeRef(snap.Nodes, "memory_node", memoryRefID)
	taskNodeID := graphNodeIDByTypeRef(snap.Nodes, "task", "task-memory-1")
	sessionNodeID := graphNodeIDByTypeRef(snap.Nodes, "session", "session-memory-1")
	agentNodeID := graphNodeIDByTypeRef(snap.Nodes, "agent", "agent-memory-1")

	if memoryNodeID == "" {
		t.Fatalf("expected memory node for recorded workspace memory")
	}
	if taskNodeID == "" || sessionNodeID == "" || agentNodeID == "" {
		t.Fatalf("expected task/session/agent anchors in memory overlay graph")
	}
	for _, node := range snap.Nodes {
		if node.Type != "memory_node" {
			continue
		}
		if node.OriginKind != "workspace_memory" {
			t.Fatalf("expected memory overlay to suppress derived memory replicas, got origin %q for node %q", node.OriginKind, node.ID)
		}
	}
	if !graphHasEdge(snap.Edges, taskNodeID, memoryNodeID, "anchors_memory") {
		t.Fatalf("expected task -> memory anchor edge")
	}
	if !graphHasEdge(snap.Edges, sessionNodeID, memoryNodeID, "emits_memory") {
		t.Fatalf("expected session -> memory edge")
	}
	if !graphHasEdge(snap.Edges, agentNodeID, memoryNodeID, "holds_memory") {
		t.Fatalf("expected agent -> memory edge")
	}
}

func TestGetGraphSnapshot_MemoryOverlayFallsBackToRecentWorkspaceMemory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	wsID := "ws-graph-memory-overlay-fallback"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: wsID,
		Title:       "Graph Memory Overlay Fallback Workspace",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	human, err := store.RegisterHuman(ctx, sqlite.HumanRegisterInput{
		WorkspaceID:       wsID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		Username:          "memoryfallback",
		DisplayName:       "Memory Fallback User",
		Password:          "graph-password",
	})
	if err != nil {
		t.Fatalf("register human: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at) VALUES ('task-memory-fallback-1', 'Memory Fallback Task', ?, 'RUNNING', 'high', 'standard', '2026-04-06T00:00:00Z', '2026-04-06T00:00:00Z')`, human.UserID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at) VALUES (?, 'task-memory-fallback-1', ?, '2026-04-06T00:00:00Z')`, wsID, human.UserID); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: wsID,
		MemoryType:  "summary",
		Title:       "Workspace-only note",
		Body:        "This memory is intentionally unanchored to task, session, and agent so the overlay must fall back to recent workspace memory.",
		Summary:     "Unanchored workspace memory",
		SourceKind:  "workspace_note",
		SourceID:    "workspace-note-1",
		Importance:  0.62,
		Confidence:  0.78,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	snap, err := store.GetGraphSnapshot(ctx, sqlite.GraphSnapshotRequest{
		WorkspaceID: wsID,
		Mode:        "MEMORY_OVERLAY",
	})
	if err != nil {
		t.Fatalf("GetGraphSnapshot returned err: %v", err)
	}

	memoryRefID := "memnode:workspace_memory:" + record.MemoryID
	if graphNodeIDByTypeRef(snap.Nodes, "memory_node", memoryRefID) == "" {
		t.Fatalf("expected memory overlay fallback to include recent workspace memory node")
	}
}
