package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRSPAnomalyShadowEmitsRuntimeSignalWithoutStrongConsequences(t *testing.T) {
	t.Setenv("RHIZOME_RSP_LIVE_ACTUATION", "0")

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-observe-only"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Observe Only",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "tester",
		DisplayName: "agent-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	authority := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	fh := NewRSPFirehose(store)
	fh.emitGlobalAnomalyAlert(ctx, workspaceID, "agent-a", "entity-a", "MOTIF_THRASH", "should stay shadow-only")

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 || events[0].EventType != "ANOMALY_ALERT" {
		t.Fatalf("expected one shadow anomaly event, got %+v", events)
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
	if events[0].EntityType != "FACT" || events[0].EntityID != "entity-a" || events[0].ActorID != "rsp_firehose" || events[0].AgentID != "agent-a" {
		t.Fatalf("expected canonical ANOMALY_ALERT envelope, got %+v", events[0])
	}
	var payload struct {
		ActuationClass  string             `json:"actuation_class"`
		ShadowOnly      bool               `json:"shadow_only"`
		CapabilityFlags RSPCapabilityFlags `json:"capability_flags"`
	}
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode anomaly payload: %v", err)
	}
	if payload.ActuationClass != "shadow_only" || !payload.ShadowOnly {
		t.Fatalf("expected shadow-only anomaly payload, got %+v", payload)
	}
	if !payload.CapabilityFlags.AnomalyShadow || payload.CapabilityFlags.StrongConsequencesLive {
		t.Fatalf("unexpected anomaly capability flags %+v", payload.CapabilityFlags)
	}
	for _, eventType := range []string{"ephemeral.rsp.motif.thrash", "ephemeral.rsp.motif.bounce", "ephemeral.system.instruction", "ephemeral.system.meta_tension.request"} {
		ephemeral, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   eventType,
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("list %s runtime events: %v", eventType, err)
		}
		if len(ephemeral) != 0 {
			t.Fatalf("expected internal motif signal %s to stay off runtime journal, got %+v", eventType, ephemeral)
		}
	}
}

func TestRSPAnomalyAlertPublishesLiveMirrorCallback(t *testing.T) {
	t.Setenv("RHIZOME_RSP_LIVE_ACTUATION", "0")

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-live-mirror"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Live Mirror",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "tester",
		DisplayName: "agent-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	authority := claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	fh := NewRSPFirehose(store)
	liveCh := make(chan RuntimeEventRecord, 1)
	fh.SetLiveMirror(func(event RuntimeEventRecord) {
		select {
		case liveCh <- event:
		default:
		}
	})

	fh.emitGlobalAnomalyAlert(ctx, workspaceID, "agent-a", "entity-a", "MOTIF_THRASH", "should publish live mirror")

	select {
	case live := <-liveCh:
		if live.EventType != "ANOMALY_ALERT" || live.WorkspaceID != workspaceID || live.EntityID != "entity-a" || live.AgentID != "agent-a" {
			t.Fatalf("unexpected live mirrored anomaly event %+v", live)
		}
		assertRuntimeEventAuthorityMetadata(t, live, authority)
		if live.EventID == "" || live.IngestSeq == 0 {
			t.Fatalf("expected live mirrored anomaly to preserve canonical identity fields, got %+v", live)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(live.PayloadJSON), &payload); err != nil {
			t.Fatalf("decode live mirror payload: %v", err)
		}
		if payload["entity_id"] != "entity-a" || payload["alert_type"] != "MOTIF_THRASH" {
			t.Fatalf("unexpected live mirror payload %+v", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live mirrored ANOMALY_ALERT")
	}
}

func TestDetectAnomalyAlertTensionObserveOnly(t *testing.T) {
	t.Setenv("RHIZOME_RSP_LIVE_ACTUATION", "0")

	store := NewTestStore(t)
	ctx := context.Background()
	cluster := &tensionClusterContext{
		cluster: ProtoClusterReport{
			ProtoClusterID: "cluster-1",
		},
		recentEvents: []RuntimeEventRecord{{
			EventID:     "evt-1",
			WorkspaceID: "ws-rsp-observe-only",
			EventType:   "ANOMALY_ALERT",
			EntityType:  "FACT",
			EntityID:    "entity-a",
			PayloadJSON: `{"alert_type":"MOTIF_THRASH","reason":"thrash"}`,
			CreatedAt:   "2026-03-26T00:00:00Z",
		}},
	}

	if got := store.detectAnomalyAlertTension(ctx, "ws-rsp-observe-only", cluster); len(got) != 0 {
		t.Fatalf("expected shadow-only anomaly to avoid derived tensions, got %+v", got)
	}
}

func TestDetectAnomalyAlertTensionAllowsGovernedHintsByPolicy(t *testing.T) {
	t.Setenv("RHIZOME_RSP_LIVE_ACTUATION", "0")

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-governed-hints"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Governed Hints",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  rspCapabilityGovernedHintsLive,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable governed hints for test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put governed hints policy: %v", err)
	}

	cluster := &tensionClusterContext{
		cluster: ProtoClusterReport{
			ProtoClusterID: "cluster-1",
		},
		recentEvents: []RuntimeEventRecord{{
			EventID:     "evt-1",
			WorkspaceID: workspaceID,
			EventType:   "ANOMALY_ALERT",
			EntityType:  "FACT",
			EntityID:    "entity-a",
			PayloadJSON: `{"alert_type":"MOTIF_THRASH","reason":"thrash","actuation_class":"governed_hint"}`,
			CreatedAt:   "2026-03-26T00:00:00Z",
		}},
	}

	if got := store.detectAnomalyAlertTension(ctx, workspaceID, cluster); len(got) == 0 {
		t.Fatalf("expected governed-hint policy to allow anomaly-derived tensions")
	}
}

func TestRSPLocalActuatorStaysObserveOnlyWithoutCanonicalCommandPath(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-local-observe-only"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Local Observe Only",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "tester",
		DisplayName: "agent-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  rspCapabilitySafeLocalAutonomics,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable safe local autonomics for observe-only regression",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put safe local autonomics policy: %v", err)
	}

	actuator := NewRSPLocalActuator(store)
	actuator.EvaluateAndActuate(ctx, workspaceID, "agent-a", 0.95, 0.90, 3, 3)

	for _, eventType := range []string{"agent.control.flush_cache", "agent.control.refresh_kernel"} {
		events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   eventType,
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("list %s runtime events: %v", eventType, err)
		}
		if len(events) != 0 {
			t.Fatalf("expected local actuator to stay observe-only for %s, got %+v", eventType, events)
		}
	}
}

func TestRSPFirehoseStateShadowFlushDoesNotEmitLocalActuation(t *testing.T) {
	t.Setenv("RHIZOME_RSP_LIVE_ACTUATION", "0")

	store := NewTestStore(t)
	ctx := context.Background()
	workspaceID := "ws-rsp-firehose-observe-only"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP Firehose Observe Only",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		OwnerUserID: "tester",
		DisplayName: "agent-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  rspCapabilityStateShadow,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable state shadow for firehose regression",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put state shadow policy: %v", err)
	}
	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  rspCapabilitySafeLocalAutonomics,
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "enable local autonomics gate without canonical command path",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("put safe local autonomics policy: %v", err)
	}

	fh := NewRSPFirehose(store)
	for i := 0; i < 10; i++ {
		fh.processEvent(ctx, RuntimeEventRecord{
			EventID:     nextID("evt"),
			WorkspaceID: workspaceID,
			EventType:   "cache.hit",
			EntityType:  "AGENT",
			EntityID:    "agent-a",
			AgentID:     "agent-a",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		})
	}

	events, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent.control.flush_cache",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list flush_cache runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected firehose flush path to stay observe-only, got %+v", events)
	}

	dump, err := store.DumpRSPTelemetry(ctx, RSPTelemetryDumpFilter{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("dump rsp telemetry: %v", err)
	}
	if dump.Summary.StateLogCount == 0 {
		t.Fatalf("expected firehose flush path to persist state telemetry, got %+v", dump.Summary)
	}
}
