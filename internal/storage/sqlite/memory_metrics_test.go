package sqlite

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestReportMemoryMetricsPersistsDerivedRatesAndRuntimeEvent(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-metrics"
		agentID     = "agent-memory-metrics"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Metrics",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Metrics Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	result, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:             workspaceID,
		AgentID:                 agentID,
		ReportScope:             "agent",
		WindowStartedAt:         "2026-03-23T00:00:00Z",
		WindowEndedAt:           "2026-03-23T01:00:00Z",
		LookupCount:             20,
		L1HitCount:              10,
		L2HitCount:              6,
		P3HitCount:              2,
		StaleHitCount:           3,
		PromotionCount:          5,
		PromotionReuseCount:     4,
		FlushCount:              4,
		FlushPositiveCount:      3,
		LocalConsolidationCount: 4,
		PotentialSharedOpCount:  30,
		Notes: map[string]any{
			"agent_runtime": "native",
		},
	})
	if err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}
	if result.Event.EventType != "memory.metrics_reported" || result.Event.EntityType != "memory_metrics" {
		t.Fatalf("unexpected runtime event %+v", result.Event)
	}
	if result.Report.ReportID == "" || result.Report.WorkspaceID != workspaceID || result.Report.AgentID != agentID {
		t.Fatalf("unexpected report %+v", result.Report)
	}
	if result.Report.TimeAuthority.WorkspaceID != workspaceID || result.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected report time authority, got %+v", result.Report.TimeAuthority)
	}
	assertFloatNear(t, "l1_hit_rate", result.Report.L1HitRate, 0.5)
	assertFloatNear(t, "l2_hit_rate", result.Report.L2HitRate, 0.6)
	assertFloatNear(t, "p3_hit_rate", result.Report.P3HitRate, 0.5)
	assertFloatNear(t, "stale_hit_rate", result.Report.StaleHitRate, 3.0/18.0)
	assertFloatNear(t, "promotion_precision", result.Report.PromotionPrecision, 0.8)
	assertFloatNear(t, "flush_utility", result.Report.FlushUtility, 0.75)
	assertFloatNear(t, "offload_ratio", result.Report.OffloadRatio, 22.0/30.0)

	stored, err := store.GetMemoryMetricsReport(ctx, workspaceID, result.Report.ReportID)
	if err != nil {
		t.Fatalf("get memory metrics report: %v", err)
	}
	if stored.WindowStartedAt != "2026-03-23T00:00:00Z" || stored.WindowEndedAt != "2026-03-23T01:00:00Z" {
		t.Fatalf("expected normalized window timestamps, got %+v", stored)
	}
	if stored.TimeAuthority.WorkspaceID != workspaceID || stored.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected stored report time authority, got %+v", stored.TimeAuthority)
	}
	if stored.Notes["agent_runtime"] != "native" {
		t.Fatalf("expected notes to round-trip, got %+v", stored.Notes)
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.metrics_reported",
		EntityType:  "memory_metrics",
		EntityID:    result.Report.ReportID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory metrics event, got %+v", events)
	}
	payload := decodeMemoryMetricsRuntimePayload(t, events[0].PayloadJSON)
	if payload["typed_event_type"] != "MEMORY_METRICS_REPORT" {
		t.Fatalf("unexpected runtime payload %+v", payload)
	}
	assertFloatNear(t, "payload offload_ratio", payload["offload_ratio"].(float64), 22.0/30.0)
}

func TestReportMemoryMetricsUpsertPreservesOwnerAndReplacesCounts(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-metrics-upsert"
		agentID     = "agent-memory-metrics-upsert"
		reportID    = "memmet-fixed"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Metrics Upsert",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Metrics Replace Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "other-agent",
		OwnerUserID: "developer",
		DisplayName: "Other Agent",
	}); err != nil {
		t.Fatalf("register other agent: %v", err)
	}

	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		ReportID:                reportID,
		WorkspaceID:             workspaceID,
		AgentID:                 agentID,
		LookupCount:             10,
		L1HitCount:              4,
		PromotionCount:          2,
		PromotionReuseCount:     1,
		LocalConsolidationCount: 1,
		PotentialSharedOpCount:  10,
	}); err != nil {
		t.Fatalf("first report memory metrics: %v", err)
	}

	result, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		ReportID:               reportID,
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		LookupCount:            8,
		L1HitCount:             3,
		L2HitCount:             3,
		P3HitCount:             1,
		StaleHitCount:          1,
		FlushCount:             2,
		FlushPositiveCount:     2,
		PotentialSharedOpCount: 12,
	})
	if err != nil {
		t.Fatalf("second report memory metrics: %v", err)
	}
	if result.Report.LookupCount != 8 || result.Report.TotalHitCount != 7 {
		t.Fatalf("expected updated counts, got %+v", result.Report)
	}

	if _, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     "other-agent",
		LookupCount: 1,
		ReportScope: "AGENT",
	}); err == nil || !stringsContains(err.Error(), "already belongs to another memory metrics owner") {
		t.Fatalf("expected ownership rejection, got %v", err)
	}
}

func TestListMemoryMetricsReportsFiltersByAgentAndScope(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-metrics-list"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Metrics List",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-b",
		AgentID:     "agent-b",
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create agent session: %v", err)
	}
	for _, input := range []MemoryMetricsReportInput{
		{ReportID: "m1", WorkspaceID: workspaceID, AgentID: "agent-a", ReportScope: "agent", LookupCount: 5},
		{ReportID: "m2", WorkspaceID: workspaceID, AgentID: "agent-b", ReportScope: "session", SessionID: "sess-b", LookupCount: 7},
	} {
		if _, err := store.ReportMemoryMetrics(ctx, input); err != nil {
			t.Fatalf("report memory metrics %s: %v", input.ReportID, err)
		}
	}

	items, err := store.ListMemoryMetricsReports(ctx, MemoryMetricsReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     "agent-b",
		ReportScope: "session",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory metrics reports: %v", err)
	}
	if len(items) != 1 || items[0].ReportID != "m2" || items[0].SessionID != "sess-b" {
		t.Fatalf("unexpected filtered reports %+v", items)
	}
	if items[0].TimeAuthority.WorkspaceID != workspaceID || items[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected listed report time authority, got %+v", items[0].TimeAuthority)
	}
}

func TestMemoryMetricsReportsExposeWorkspaceTimeAuthority(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-metrics-time-authority"
		agentID     = "agent-memory-metrics-time-authority"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Metrics Time Authority",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Metrics Time Authority Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	result, err := store.ReportMemoryMetrics(ctx, MemoryMetricsReportInput{
		WorkspaceID:             workspaceID,
		AgentID:                 agentID,
		ReportScope:             "agent",
		LookupCount:             6,
		L1HitCount:              3,
		L2HitCount:              2,
		P3HitCount:              1,
		StaleHitCount:           1,
		PromotionCount:          2,
		PromotionReuseCount:     1,
		FlushCount:              1,
		FlushPositiveCount:      1,
		LocalConsolidationCount: 1,
		PotentialSharedOpCount:  10,
	})
	if err != nil {
		t.Fatalf("report memory metrics: %v", err)
	}
	if result.Report.TimeAuthority.WorkspaceID != workspaceID || result.Report.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected write result time authority, got %+v", result.Report.TimeAuthority)
	}

	items, err := store.ListMemoryMetricsReports(ctx, MemoryMetricsReportFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory metrics reports: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one metrics report, got %+v", items)
	}
	if items[0].TimeAuthority.WorkspaceID != workspaceID || items[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected listed metrics report time authority, got %+v", items[0].TimeAuthority)
	}

	stored, err := store.GetMemoryMetricsReport(ctx, workspaceID, result.Report.ReportID)
	if err != nil {
		t.Fatalf("get memory metrics report: %v", err)
	}
	if stored.TimeAuthority.WorkspaceID != workspaceID || stored.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected stored metrics report time authority, got %+v", stored.TimeAuthority)
	}
}

func TestReportMemoryMetricsRejectsInvalidCountsAndTimestamps(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-metrics-validation"
		agentID     = "agent-memory-metrics-validation"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Metrics Validation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Metrics Validation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "other-agent",
		OwnerUserID: "developer",
		DisplayName: "Other Metrics Agent",
	}); err != nil {
		t.Fatalf("register other agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-valid",
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-23T00:00:00Z",
	}); err != nil {
		t.Fatalf("create valid session: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   "sess-other",
		AgentID:     "other-agent",
		WorkspaceID: workspaceID,
		StartedAt:   "2026-03-23T00:05:00Z",
	}); err != nil {
		t.Fatalf("create other session: %v", err)
	}

	cases := []MemoryMetricsReportInput{
		{WorkspaceID: workspaceID, AgentID: agentID, LookupCount: 1, L1HitCount: 2},
		{WorkspaceID: workspaceID, AgentID: agentID, LookupCount: 3, L1HitCount: 2, L2HitCount: 2},
		{WorkspaceID: workspaceID, AgentID: agentID, LookupCount: 2, L1HitCount: 1, L2HitCount: 1, StaleHitCount: 3},
		{WorkspaceID: workspaceID, AgentID: agentID, PromotionCount: 1, PromotionReuseCount: 2},
		{WorkspaceID: workspaceID, AgentID: agentID, FlushCount: 1, FlushPositiveCount: 2},
		{WorkspaceID: workspaceID, AgentID: agentID, WindowStartedAt: "2026-03-23T02:00:00Z", WindowEndedAt: "2026-03-23T01:00:00Z"},
		{WorkspaceID: workspaceID, AgentID: agentID, WindowStartedAt: "nope"},
		{WorkspaceID: workspaceID, AgentID: agentID, ReportScope: "session"},
		{WorkspaceID: workspaceID, AgentID: agentID, ReportScope: "session", SessionID: "missing-session"},
		{WorkspaceID: workspaceID, AgentID: agentID, ReportScope: "session", SessionID: "sess-other"},
	}
	for _, input := range cases {
		if _, err := store.ReportMemoryMetrics(ctx, input); err == nil {
			t.Fatalf("expected validation error for %+v", input)
		}
	}
}

func decodeMemoryMetricsRuntimePayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	out := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("decode memory metrics runtime payload: %v", err)
	}
	return out
}

func assertJSONDoesNotContainField(t *testing.T, value any, field string) {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal value: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal value: %v", err)
	}
	if jsonContainsField(decoded, field) {
		t.Fatalf("unexpected %s in payload: %s", field, string(raw))
	}
}

func jsonContainsField(value any, field string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed[field]; ok {
			return true
		}
		for _, child := range typed {
			if jsonContainsField(child, field) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if jsonContainsField(child, field) {
				return true
			}
		}
	}
	return false
}

func assertFloatNear(t *testing.T, label string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s: expected %.12f, got %.12f", label, want, got)
	}
}

func stringsContains(haystack, needle string) bool {
	return haystack != "" && needle != "" && strings.Contains(haystack, needle)
}
