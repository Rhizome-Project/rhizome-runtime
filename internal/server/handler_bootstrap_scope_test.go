package server

import (
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestScopeAgentBootstrapSnapshotFiltersHistoricalForeignContext(t *testing.T) {
	t.Parallel()

	snapshot := sqlite.WorkspaceSnapshot{
		Docs: []sqlite.WorkspaceDocRecord{
			{
				DocKey:  "tooling",
				Title:   "Tooling",
				Content: "Use the current tool surface.",
			},
			{
				DocKey:  "agent.worker-neo.current_context",
				Title:   "Worker Context",
				Content: "# Agent Current Context\n- task_id: human-mcp-1776459451185\n- summary: Firecrawl\n",
			},
			{
				DocKey:  "agent.worker-neo.claimed_work",
				Title:   "Claimed Work",
				Content: "# Claimed Work Ledger\n- task_id: human-mcp-1776459451185\n",
			},
			{
				DocKey:  "agent.observer.current_context",
				Title:   "Observer Context",
				Content: "# Agent Current Context\n- task_id: task-1776448149516\n- summary: stale blocker\n",
			},
			{
				DocKey:  "agent.topology.additional_agents.v1",
				Title:   "Topology",
				Content: "stale topology blocker",
			},
			{
				DocKey:  "task.human-mcp-1776459451185",
				Title:   "Task Packet",
				Content: "Current human MCP request.",
			},
		},
		Sessions: []sqlite.AgentSessionStateRecord{
			{
				SessionID: "session-current",
				AgentID:   "worker-neo",
				TaskID:    "human-mcp-1776459451185",
				Status:    "ACTIVE",
				StartedAt: "2026-04-18T10:00:00Z",
			},
		},
		Agents: []sqlite.AgentRecord{
			{
				AgentID: "worker-neo",
				ActiveTasks: []sqlite.AgentCurrentTask{
					{TaskID: "human-mcp-1776459451185", ClaimStatus: "CLAIMED", Summary: "planner claimed task"},
				},
			},
		},
		RecentUpdates: []sqlite.AgentUpdateRecord{
			{
				UpdateID:    "update-old",
				AgentID:     "worker-neo",
				UpdateType:  "session.keepalive",
				Summary:     "stale blocked summary",
				PayloadJSON: `{"task_id":"human-mcp-1776459451185","session_id":"session-old"}`,
				CreatedAt:   "2026-04-18T09:59:00Z",
			},
			{
				UpdateID:    "update-current",
				AgentID:     "worker-neo",
				UpdateType:  "session.start",
				Summary:     "Firecrawl",
				PayloadJSON: `{"task_id":"human-mcp-1776459451185","session_id":"session-current"}`,
				CreatedAt:   "2026-04-18T10:00:02Z",
			},
			{
				UpdateID:    "update-peer",
				AgentID:     "observer",
				UpdateType:  "agent.request",
				Summary:     "peer request on current task",
				PayloadJSON: `{"task_id":"human-mcp-1776459451185"}`,
				CreatedAt:   "2026-04-18T10:00:03Z",
			},
		},
		RecentMessages: []sqlite.MessageRecord{
			{
				MessageID:    "msg-old",
				FromAgentID:  "observer",
				ToAgentID:    "worker-neo",
				MetadataJSON: `{"task_id":"human-mcp-1776459451185","session_id":"session-old"}`,
				CreatedAt:    "2026-04-18T09:59:30Z",
			},
			{
				MessageID:    "msg-current",
				FromAgentID:  "observer",
				ToAgentID:    "worker-neo",
				MetadataJSON: `{"task_id":"human-mcp-1776459451185","session_id":"session-current"}`,
				CreatedAt:    "2026-04-18T10:00:04Z",
			},
			{
				MessageID:    "msg-foreign",
				FromAgentID:  "observer",
				ToAgentID:    "synthesizer",
				MetadataJSON: `{"task_id":"human-mcp-1776459451185"}`,
				CreatedAt:    "2026-04-18T10:00:05Z",
			},
		},
		RecentMemory: []sqlite.WorkspaceMemoryRecord{
			{
				MemoryID:   "memory-old",
				AgentID:    "worker-neo",
				TaskID:     "task-1776448149516",
				SessionID:  "session-old",
				CreatedAt:  "2026-04-18T09:58:00Z",
				UpdatedAt:  "2026-04-18T09:58:00Z",
				Title:      "Old blocker",
				Summary:    "stale",
				MemoryType: "INCIDENT",
			},
			{
				MemoryID:   "memory-current",
				AgentID:    "worker-neo",
				TaskID:     "human-mcp-1776459451185",
				SessionID:  "session-current",
				CreatedAt:  "2026-04-18T10:00:06Z",
				UpdatedAt:  "2026-04-18T10:00:06Z",
				Title:      "Current task fact",
				Summary:    "firecrawl context",
				MemoryType: "NOTE",
			},
		},
	}

	scoped := scopeAgentBootstrapSnapshot("worker-neo", snapshot)

	gotDocs := map[string]bool{}
	for _, doc := range scoped.Docs {
		gotDocs[doc.DocKey] = true
	}
	if !gotDocs["tooling"] || !gotDocs["task.human-mcp-1776459451185"] || !gotDocs["agent.worker-neo.current_context"] || !gotDocs["agent.worker-neo.claimed_work"] {
		t.Fatalf("expected scoped docs to keep current task/agent context, got %+v", scoped.Docs)
	}
	if gotDocs["agent.observer.current_context"] {
		t.Fatalf("expected foreign observer context to be removed, got %+v", scoped.Docs)
	}
	if gotDocs["agent.topology.additional_agents.v1"] {
		t.Fatalf("expected stale topology doc to be removed from bootstrap docs, got %+v", scoped.Docs)
	}

	if len(scoped.RecentUpdates) != 2 {
		t.Fatalf("expected current-session updates only, got %+v", scoped.RecentUpdates)
	}
	for _, update := range scoped.RecentUpdates {
		if update.UpdateID == "update-old" {
			t.Fatalf("expected stale pre-session update to be filtered, got %+v", scoped.RecentUpdates)
		}
	}

	if len(scoped.RecentMessages) != 1 || scoped.RecentMessages[0].MessageID != "msg-current" {
		t.Fatalf("expected only current-session message to remain, got %+v", scoped.RecentMessages)
	}
	if len(scoped.RecentMemory) != 1 || scoped.RecentMemory[0].MemoryID != "memory-current" {
		t.Fatalf("expected only current-task memory to remain, got %+v", scoped.RecentMemory)
	}
}

func TestScopeAgentBootstrapSnapshotKeepsOwnIdleContext(t *testing.T) {
	t.Parallel()

	snapshot := sqlite.WorkspaceSnapshot{
		Docs: []sqlite.WorkspaceDocRecord{
			{
				DocKey:  "agent.synthesizer.current_context",
				Title:   "Synth Context",
				Content: "# Agent Current Context\n- task_id: (none)\n- outcome: idle\n",
			},
			{
				DocKey:  "agent.synthesizer.claimed_work",
				Title:   "Synth Ledger",
				Content: "# Claimed Work Ledger\n- state: idle\n",
			},
			{
				DocKey:  "agent.worker-neo.current_context",
				Title:   "Foreign Context",
				Content: "# Agent Current Context\n- task_id: human-mcp-1776459451185\n",
			},
			{
				DocKey:  "tooling",
				Title:   "Tooling",
				Content: "Use the current tool surface.",
			},
		},
		RecentUpdates: []sqlite.AgentUpdateRecord{
			{UpdateID: "own-update", AgentID: "synthesizer", Summary: "idle heartbeat"},
			{UpdateID: "foreign-update", AgentID: "worker-neo", Summary: "foreign"},
		},
	}

	scoped := scopeAgentBootstrapSnapshot("synthesizer", snapshot)

	keys := make([]string, 0, len(scoped.Docs))
	for _, doc := range scoped.Docs {
		keys = append(keys, doc.DocKey)
	}
	joined := strings.Join(keys, ",")
	if !strings.Contains(joined, "agent.synthesizer.current_context") || !strings.Contains(joined, "agent.synthesizer.claimed_work") || !strings.Contains(joined, "tooling") {
		t.Fatalf("expected own idle docs plus generic tooling, got %+v", scoped.Docs)
	}
	if strings.Contains(joined, "agent.worker-neo.current_context") {
		t.Fatalf("expected foreign idle context to be filtered, got %+v", scoped.Docs)
	}
	if len(scoped.RecentUpdates) != 1 || scoped.RecentUpdates[0].UpdateID != "own-update" {
		t.Fatalf("expected only own idle updates to remain, got %+v", scoped.RecentUpdates)
	}
}

func TestScopeAgentBootstrapSnapshotDropsOwnStaleTaskContextWhenIdle(t *testing.T) {
	t.Parallel()

	snapshot := sqlite.WorkspaceSnapshot{
		Docs: []sqlite.WorkspaceDocRecord{
			{
				DocKey:  "agent.alpha.current_context",
				Title:   "Alpha Context",
				Content: "# Agent Current Context\n- task_id: branch-choir-first-test\n- summary: stale branch blocker\n",
			},
			{
				DocKey:  "agent.alpha.claimed_work",
				Title:   "Alpha Claimed Work",
				Content: "# Claimed Work Ledger\n- task_id: branch-choir-first-test\n- state: BLOCKED\n",
			},
			{
				DocKey:  "tooling",
				Title:   "Tooling",
				Content: "Use the current tool surface.",
			},
		},
		Agents: []sqlite.AgentRecord{
			{AgentID: "alpha"},
		},
	}

	scoped := scopeAgentBootstrapSnapshot("alpha", snapshot)

	gotDocs := map[string]bool{}
	for _, doc := range scoped.Docs {
		gotDocs[doc.DocKey] = true
	}
	if gotDocs["agent.alpha.current_context"] || gotDocs["agent.alpha.claimed_work"] {
		t.Fatalf("expected idle bootstrap to drop same-agent stale task docs, got %+v", scoped.Docs)
	}
	if !gotDocs["tooling"] {
		t.Fatalf("expected generic tooling to remain, got %+v", scoped.Docs)
	}
}
