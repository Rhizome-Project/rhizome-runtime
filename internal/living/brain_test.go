package living_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/living"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// --- Mock RhizomeClient for brain tests ---

type mockRhizomeForBrain struct {
	memory               []living.WorkspaceMemoryRecord
	compactionCandidates []living.SessionCompactionCandidate
	compactionSnapshots  []sqlite.SessionCompactionSnapshotRecord
	promotions           []sqlite.MemoryPromotionRecord
	coherenceScopes      map[string]sqlite.MemoryCoherenceScopeReport
	invalidations        []sqlite.MemoryInvalidationRecord
	invalidationCursor   sqlite.MemoryInvalidationCursorRecord
	memoryGraphList      []sqlite.MemoryGraphNodeRecord
	memoryGraphDetails   map[string]sqlite.MemoryGraphNodeDetail
	memoryNodeSearch     sqlite.MemoryNodeSearchResult
	tensionList          living.WorkspaceTensionListResult
	tensionDetail        sqlite.TensionDetail
	tensionFrontier      living.WorkspaceTensionFrontierResult
	tensionAttachable    living.WorkspaceTensionAttachableResult
	rspStateReport       sqlite.RSPStateReport
	rspForecastReport    sqlite.RSPForecastReport
	rspBeliefReport      sqlite.RSPBeliefReport
	rspBeliefClaim       sqlite.RSPBeliefClaimReport
	rspTelemetryDump     sqlite.RSPTelemetryDump
	unifiedControlReport sqlite.UnifiedControlReport
	controlReport        sqlite.ControlReport
	controlClusterDetail sqlite.ControlClusterDetail
	controlStateReport   sqlite.ClusterControlStateReport
	controlStateDetail   sqlite.ClusterControlStateDetail
	rspCapabilityFlags   sqlite.RSPCapabilityFlags
	runtimeReplayReport  sqlite.RuntimeReplayReport
}

func (m *mockRhizomeForBrain) FetchTasks(_ context.Context, _ []string) ([]living.Task, error) {
	return nil, nil
}
func (m *mockRhizomeForBrain) ClaimTask(_ context.Context, _, _ string) error      { return nil }
func (m *mockRhizomeForBrain) ReleaseTask(_ context.Context, _, _, _ string) error { return nil }
func (m *mockRhizomeForBrain) CompleteTask(_ context.Context, _, _ string) error   { return nil }
func (m *mockRhizomeForBrain) FailTask(_ context.Context, _, _ string) error       { return nil }
func (m *mockRhizomeForBrain) GetTaskUpdates(_ context.Context, _ string, _ time.Time) ([]living.Update, error) {
	return nil, nil
}
func (m *mockRhizomeForBrain) SendUpdate(_ context.Context, _, _, _, _, _ string) error { return nil }
func (m *mockRhizomeForBrain) FetchMessages(_ context.Context, _ string, _ time.Time) ([]living.Message, error) {
	return nil, nil
}
func (m *mockRhizomeForBrain) SendMessage(_ context.Context, _, _, _, _ string) error { return nil }
func (m *mockRhizomeForBrain) Heartbeat(_ context.Context, _, _, _ string) error      { return nil }
func (m *mockRhizomeForBrain) EscalateTask(_ context.Context, _, _ string) error      { return nil }

func (m *mockRhizomeForBrain) RecordWorkspaceMemory(_ context.Context, input living.WorkspaceMemoryInput) (living.WorkspaceMemoryRecord, error) {
	record := living.WorkspaceMemoryRecord{
		MemoryID:    "memory-" + strings.ToLower(strings.ReplaceAll(strings.TrimSpace(input.Title), " ", "-")),
		WorkspaceID: input.WorkspaceID,
		MemoryType:  strings.ToUpper(strings.TrimSpace(input.MemoryType)),
		Title:       input.Title,
		Body:        input.Body,
		Summary:     input.Summary,
		AgentID:     input.AgentID,
		SessionID:   input.SessionID,
		TaskID:      input.TaskID,
		SourceKind:  input.SourceKind,
		SourceID:    input.SourceID,
		Tags:        input.Tags,
		Importance:  input.Importance,
		Confidence:  input.Confidence,
		CreatedAt:   "2026-03-21T00:00:00Z",
		UpdatedAt:   "2026-03-21T00:00:00Z",
	}
	if record.MemoryType == "" {
		record.MemoryType = "NOTE"
	}
	if record.MemoryID == "memory-" {
		record.MemoryID = "memory-1"
	}
	m.memory = append(m.memory, record)
	return record, nil
}

func (m *mockRhizomeForBrain) ListWorkspaceMemory(_ context.Context, filter living.WorkspaceMemorySearchFilter) ([]living.WorkspaceMemoryRecord, error) {
	var out []living.WorkspaceMemoryRecord
	for _, item := range m.memory {
		if filter.MemoryType != "" && item.MemoryType != strings.ToUpper(strings.TrimSpace(filter.MemoryType)) {
			continue
		}
		if filter.TaskID != "" && item.TaskID != filter.TaskID {
			continue
		}
		out = append(out, item)
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (m *mockRhizomeForBrain) SearchWorkspaceMemory(_ context.Context, filter living.WorkspaceMemorySearchFilter) ([]living.WorkspaceMemoryRecord, error) {
	query := strings.ToLower(strings.TrimSpace(filter.Query))
	tokens := strings.Fields(query)
	var out []living.WorkspaceMemoryRecord
	for _, item := range m.memory {
		if filter.MemoryType != "" && item.MemoryType != strings.ToUpper(strings.TrimSpace(filter.MemoryType)) {
			continue
		}
		haystack := strings.ToLower(item.Title + " " + item.Body + " " + item.Summary)
		if len(tokens) > 0 {
			matched := true
			for _, token := range tokens {
				if !strings.Contains(haystack, token) {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
		}
		out = append(out, item)
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (m *mockRhizomeForBrain) BuildMemoryShellPacket(_ context.Context, filter living.WorkspaceMemoryPacketFilter) (sqlite.MemoryShellPacket, error) {
	packet := sqlite.MemoryShellPacket{
		Meta: sqlite.MemoryPacketMeta{
			PacketKind:    "SHELL",
			SchemaVersion: "test",
			WorkspaceID:   strings.TrimSpace(filter.WorkspaceID),
			TaskID:        strings.TrimSpace(filter.TaskID),
			SessionID:     strings.TrimSpace(filter.SessionID),
			AgentID:       strings.TrimSpace(filter.AgentID),
			Scope:         "task",
		},
	}
	for _, item := range m.memory {
		if filter.TaskID != "" && item.TaskID != filter.TaskID {
			continue
		}
		packet.IdentityMemories = append(packet.IdentityMemories, sqlite.WorkspaceMemoryRecord{
			MemoryID:    item.MemoryID,
			WorkspaceID: item.WorkspaceID,
			MemoryType:  item.MemoryType,
			Title:       item.Title,
			Body:        item.Body,
			Summary:     item.Summary,
			AgentID:     item.AgentID,
			SessionID:   item.SessionID,
			TaskID:      item.TaskID,
			SourceKind:  item.SourceKind,
			SourceID:    item.SourceID,
			Tags:        item.Tags,
			Importance:  item.Importance,
			Confidence:  item.Confidence,
			CreatedAt:   item.CreatedAt,
			UpdatedAt:   item.UpdatedAt,
		})
	}
	return packet, nil
}

func (m *mockRhizomeForBrain) BuildMemoryKernelPacket(_ context.Context, filter living.WorkspaceMemoryPacketFilter) (sqlite.MemoryKernelPacket, error) {
	packet := sqlite.MemoryKernelPacket{
		Meta: sqlite.MemoryPacketMeta{
			PacketKind:    "KERNEL",
			SchemaVersion: "test",
			WorkspaceID:   strings.TrimSpace(filter.WorkspaceID),
			TaskID:        strings.TrimSpace(filter.TaskID),
			SessionID:     strings.TrimSpace(filter.SessionID),
			Scope:         "task",
		},
		BoundarySummary: &sqlite.MemoryPacketBoundarySummary{
			DecisionRecordCount: 1,
		},
		BasisSummary: &sqlite.MemoryPacketBasisSummary{
			TotalRefCount:          1,
			CoordinationBasisCount: 1,
		},
	}
	return packet, nil
}

func (m *mockRhizomeForBrain) GetMemoryPromotion(_ context.Context, filter living.WorkspaceMemoryPromotionFilter) (sqlite.MemoryPromotionRecord, error) {
	for _, item := range m.promotions {
		if strings.TrimSpace(filter.PromotionID) == item.PromotionID {
			return item, nil
		}
	}
	return sqlite.MemoryPromotionRecord{}, fmt.Errorf("memory promotion %s not found", strings.TrimSpace(filter.PromotionID))
}

func (m *mockRhizomeForBrain) ListMemoryPromotions(_ context.Context, filter living.WorkspaceMemoryPromotionFilter) ([]sqlite.MemoryPromotionRecord, error) {
	state := strings.ToUpper(strings.TrimSpace(filter.State))
	candidateKind := strings.ToUpper(strings.TrimSpace(filter.CandidateKind))
	candidateType := strings.ToUpper(strings.TrimSpace(filter.CandidateType))
	var out []sqlite.MemoryPromotionRecord
	for _, item := range m.promotions {
		if state != "" && item.State != state {
			continue
		}
		if candidateKind != "" && item.CandidateKind != candidateKind {
			continue
		}
		if candidateType != "" && item.CandidateType != candidateType {
			continue
		}
		out = append(out, item)
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (m *mockRhizomeForBrain) GetMemoryCoherenceScope(_ context.Context, filter living.WorkspaceMemoryCoherenceFilter) (sqlite.MemoryCoherenceScopeReport, error) {
	key := strings.TrimSpace(filter.AgentID) + "|" + strings.TrimSpace(filter.SessionID) + "|" + strings.ToUpper(strings.TrimSpace(filter.ReportScope))
	if scope, ok := m.coherenceScopes[key]; ok {
		return scope, nil
	}
	fallbackKey := strings.TrimSpace(filter.AgentID) + "||"
	if scope, ok := m.coherenceScopes[fallbackKey]; ok {
		return scope, nil
	}
	return sqlite.MemoryCoherenceScopeReport{}, fmt.Errorf("memory coherence scope %s not found", key)
}

func (m *mockRhizomeForBrain) ListMemoryInvalidations(_ context.Context, filter living.WorkspaceMemoryInvalidationListFilter) (living.WorkspaceMemoryInvalidationListResult, error) {
	agentID := strings.TrimSpace(filter.AgentID)
	sessionID := strings.TrimSpace(filter.SessionID)
	var items []sqlite.MemoryInvalidationRecord
	for _, item := range m.invalidations {
		if agentID != "" && item.AgentID != agentID {
			continue
		}
		if sessionID != "" && item.SessionID != sessionID {
			continue
		}
		if !filter.IncludeAcked && strings.EqualFold(item.State, "ACKED") {
			continue
		}
		if !filter.IncludeDeadLetter && strings.EqualFold(item.State, "DEAD_LETTER") {
			continue
		}
		items = append(items, item)
	}
	if filter.Limit > 0 && len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	result := living.WorkspaceMemoryInvalidationListResult{
		WorkspaceID:   "ws-1",
		AgentID:       agentID,
		TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
		Items:         items,
		Count:         len(items),
	}
	if len(items) > 0 {
		result.WorkspaceID = items[0].WorkspaceID
		result.AgentID = items[0].AgentID
		result.TimeAuthority = items[0].TimeAuthority
	}
	return result, nil
}

func (m *mockRhizomeForBrain) GetMemoryInvalidation(_ context.Context, filter living.WorkspaceMemoryInvalidationGetFilter) (sqlite.MemoryInvalidationRecord, error) {
	invalidationID := strings.TrimSpace(filter.InvalidationID)
	agentID := strings.TrimSpace(filter.AgentID)
	for _, item := range m.invalidations {
		if invalidationID != "" && item.InvalidationID != invalidationID {
			continue
		}
		if agentID != "" && item.AgentID != agentID {
			continue
		}
		return item, nil
	}
	return sqlite.MemoryInvalidationRecord{}, fmt.Errorf("memory invalidation %s not found", invalidationID)
}

func (m *mockRhizomeForBrain) GetMemoryInvalidationCursor(_ context.Context, filter living.WorkspaceMemoryInvalidationCursorFilter) (sqlite.MemoryInvalidationCursorRecord, error) {
	cursor := m.invalidationCursor
	if strings.TrimSpace(cursor.WorkspaceID) == "" {
		cursor.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(cursor.AgentID) == "" {
		cursor.AgentID = strings.TrimSpace(filter.AgentID)
	}
	if strings.TrimSpace(cursor.SessionID) == "" {
		cursor.SessionID = strings.TrimSpace(filter.SessionID)
	}
	if strings.TrimSpace(cursor.WorkspaceID) == "" || strings.TrimSpace(cursor.AgentID) == "" {
		return sqlite.MemoryInvalidationCursorRecord{}, fmt.Errorf("memory invalidation cursor not found")
	}
	return cursor, nil
}

func (m *mockRhizomeForBrain) ListMemoryGraphNodes(_ context.Context, filter living.WorkspaceMemoryGraphListFilter) (living.WorkspaceMemoryGraphListResult, error) {
	agentID := strings.TrimSpace(filter.AgentID)
	sessionID := strings.TrimSpace(filter.SessionID)
	taskID := strings.TrimSpace(filter.TaskID)
	memoryType := strings.ToUpper(strings.TrimSpace(filter.MemoryType))
	var items []sqlite.MemoryGraphNodeRecord
	for _, item := range m.memoryGraphList {
		if memoryType != "" && item.MemoryType != memoryType {
			continue
		}
		if agentID != "" && item.AgentID != agentID {
			continue
		}
		if sessionID != "" && item.SessionID != sessionID {
			continue
		}
		if taskID != "" && item.TaskID != taskID {
			continue
		}
		items = append(items, item)
	}
	if filter.Limit > 0 && len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" && len(items) > 0 {
		workspaceID = items[0].WorkspaceID
	}
	if workspaceID == "" {
		workspaceID = "ws-1"
	}
	return living.WorkspaceMemoryGraphListResult{
		WorkspaceID: workspaceID,
		TimeAuthority: sqlite.WorkspaceTimeAuthority{
			WorkspaceID:  workspaceID,
			ReferenceAt:  "2026-03-30T00:00:00Z",
			CurrentEpoch: 7,
		},
		Items: items,
		Count: len(items),
	}, nil
}

func (m *mockRhizomeForBrain) GetMemoryGraphNode(_ context.Context, filter living.WorkspaceMemoryGraphGetFilter) (sqlite.MemoryGraphNodeDetail, error) {
	memoryID := strings.TrimSpace(filter.MemoryID)
	if detail, ok := m.memoryGraphDetails[memoryID]; ok {
		return detail, nil
	}
	return sqlite.MemoryGraphNodeDetail{}, fmt.Errorf("memory graph node %s not found", memoryID)
}

func (m *mockRhizomeForBrain) SearchMemoryNodes(_ context.Context, filter living.WorkspaceMemoryNodeSearchFilter) (sqlite.MemoryNodeSearchResult, error) {
	result := m.memoryNodeSearch
	if strings.TrimSpace(result.WorkspaceID) == "" {
		result.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(result.Query) == "" {
		result.Query = strings.TrimSpace(filter.Query)
	}
	if filter.Limit > 0 && len(result.Hits) > filter.Limit {
		result.Hits = result.Hits[:filter.Limit]
		result.Count = len(result.Hits)
	}
	if strings.TrimSpace(result.GeneratedAt) == "" {
		result.GeneratedAt = "2026-03-30T00:00:00Z"
	}
	return result, nil
}

func (m *mockRhizomeForBrain) ListTensions(_ context.Context, filter living.WorkspaceTensionFilter) (living.WorkspaceTensionListResult, error) {
	result := m.tensionList
	if strings.TrimSpace(result.WorkspaceID) == "" {
		result.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(result.TimeAuthority.WorkspaceID) == "" {
		result.TimeAuthority = sqlite.WorkspaceTimeAuthority{
			WorkspaceID:  result.WorkspaceID,
			ReferenceAt:  "2026-03-30T00:00:00Z",
			CurrentEpoch: 7,
		}
	}
	if filter.Limit > 0 && len(result.Items) > filter.Limit {
		result.Items = result.Items[:filter.Limit]
	}
	result.Count = len(result.Items)
	return result, nil
}

func (m *mockRhizomeForBrain) GetTension(_ context.Context, filter living.WorkspaceTensionGetFilter) (sqlite.TensionDetail, error) {
	detail := m.tensionDetail
	if strings.TrimSpace(detail.TimeAuthority.WorkspaceID) == "" {
		detail.TimeAuthority = sqlite.WorkspaceTimeAuthority{
			WorkspaceID:  strings.TrimSpace(filter.WorkspaceID),
			ReferenceAt:  "2026-03-30T00:00:00Z",
			CurrentEpoch: 7,
		}
	}
	if strings.TrimSpace(detail.Tension.TensionID) == "" {
		detail.Tension.TensionID = strings.TrimSpace(filter.TensionID)
	}
	return detail, nil
}

func (m *mockRhizomeForBrain) ListTensionFrontier(_ context.Context, filter living.WorkspaceTensionFilter) (living.WorkspaceTensionFrontierResult, error) {
	result := m.tensionFrontier
	if strings.TrimSpace(result.WorkspaceID) == "" {
		result.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(result.TimeAuthority.WorkspaceID) == "" {
		result.TimeAuthority = sqlite.WorkspaceTimeAuthority{
			WorkspaceID:  result.WorkspaceID,
			ReferenceAt:  "2026-03-30T00:00:00Z",
			CurrentEpoch: 7,
		}
	}
	if filter.Limit > 0 && len(result.Items) > filter.Limit {
		result.Items = result.Items[:filter.Limit]
	}
	result.Count = len(result.Items)
	return result, nil
}

func (m *mockRhizomeForBrain) ListAttachableTensions(_ context.Context, filter living.WorkspaceTensionAttachableFilter) (living.WorkspaceTensionAttachableResult, error) {
	result := m.tensionAttachable
	if strings.TrimSpace(result.WorkspaceID) == "" {
		result.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(result.AgentID) == "" {
		result.AgentID = strings.TrimSpace(filter.AgentID)
	}
	result.Count = len(result.Items)
	return result, nil
}

func (m *mockRhizomeForBrain) GetRSPStateReport(_ context.Context, filter living.WorkspaceRSPStateFilter) (sqlite.RSPStateReport, error) {
	report := m.rspStateReport
	if strings.TrimSpace(report.WorkspaceID) == "" {
		report.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(report.AgentID) == "" {
		report.AgentID = strings.TrimSpace(filter.AgentID)
	}
	if strings.TrimSpace(report.TaskID) == "" {
		report.TaskID = strings.TrimSpace(filter.TaskID)
	}
	if strings.TrimSpace(report.SessionID) == "" {
		report.SessionID = strings.TrimSpace(filter.SessionID)
	}
	return report, nil
}

func (m *mockRhizomeForBrain) GetRSPForecastReport(_ context.Context, filter living.WorkspaceRSPForecastFilter) (sqlite.RSPForecastReport, error) {
	report := m.rspForecastReport
	if strings.TrimSpace(report.WorkspaceID) == "" {
		report.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(report.AgentID) == "" {
		report.AgentID = strings.TrimSpace(filter.AgentID)
	}
	if strings.TrimSpace(report.TaskID) == "" {
		report.TaskID = strings.TrimSpace(filter.TaskID)
	}
	if strings.TrimSpace(report.SessionID) == "" {
		report.SessionID = strings.TrimSpace(filter.SessionID)
	}
	return report, nil
}

func (m *mockRhizomeForBrain) GetRSPBeliefReport(_ context.Context, filter living.WorkspaceRSPBeliefFilter) (sqlite.RSPBeliefReport, error) {
	report := m.rspBeliefReport
	if strings.TrimSpace(report.WorkspaceID) == "" {
		report.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(report.AgentID) == "" {
		report.AgentID = strings.TrimSpace(filter.AgentID)
	}
	if strings.TrimSpace(report.TaskID) == "" {
		report.TaskID = strings.TrimSpace(filter.TaskID)
	}
	if strings.TrimSpace(report.SessionID) == "" {
		report.SessionID = strings.TrimSpace(filter.SessionID)
	}
	return report, nil
}

func (m *mockRhizomeForBrain) GetRSPBeliefClaim(_ context.Context, filter living.WorkspaceRSPBeliefClaimFilter) (sqlite.RSPBeliefClaimReport, error) {
	item := m.rspBeliefClaim
	if strings.TrimSpace(item.WorkspaceID) == "" {
		item.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(item.ClaimID) == "" {
		item.ClaimID = strings.TrimSpace(filter.ClaimID)
	}
	return item, nil
}

func (m *mockRhizomeForBrain) GetRSPTelemetryDump(_ context.Context, _ living.WorkspaceRSPTelemetryFilter) (sqlite.RSPTelemetryDump, error) {
	dump := m.rspTelemetryDump
	return dump, nil
}

func (m *mockRhizomeForBrain) GetUnifiedControlReport(_ context.Context, filter living.WorkspaceUnifiedControlFilter) (sqlite.UnifiedControlReport, error) {
	report := m.unifiedControlReport
	if strings.TrimSpace(report.WorkspaceID) == "" {
		report.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	return report, nil
}

func (m *mockRhizomeForBrain) GetControlReport(_ context.Context, filter living.WorkspaceControlReportFilter) (sqlite.ControlReport, error) {
	report := m.controlReport
	if strings.TrimSpace(report.WorkspaceID) == "" {
		report.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	return report, nil
}

func (m *mockRhizomeForBrain) GetControlClusterDetail(_ context.Context, filter living.WorkspaceControlClusterFilter) (sqlite.ControlClusterDetail, error) {
	detail := m.controlClusterDetail
	if strings.TrimSpace(detail.Cluster.ProtoClusterID) == "" {
		detail.Cluster.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	}
	return detail, nil
}

func (m *mockRhizomeForBrain) GetControlStateReport(_ context.Context, filter living.WorkspaceControlStateFilter) (sqlite.ClusterControlStateReport, error) {
	report := m.controlStateReport
	if strings.TrimSpace(report.WorkspaceID) == "" {
		report.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	return report, nil
}

func (m *mockRhizomeForBrain) GetControlStateClusterDetail(_ context.Context, filter living.WorkspaceControlStateClusterFilter) (sqlite.ClusterControlStateDetail, error) {
	detail := m.controlStateDetail
	if strings.TrimSpace(detail.TimeAuthority.WorkspaceID) == "" {
		detail.TimeAuthority.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(detail.Cluster.ProtoClusterID) == "" {
		detail.Cluster.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	}
	if strings.TrimSpace(detail.State.ProtoClusterID) == "" {
		detail.State.ProtoClusterID = strings.TrimSpace(filter.ProtoClusterID)
	}
	return detail, nil
}

func (m *mockRhizomeForBrain) GetRSPCapabilityFlags(_ context.Context, _ living.WorkspaceRSPCapabilityFilter) (sqlite.RSPCapabilityFlags, error) {
	return m.rspCapabilityFlags, nil
}

func (m *mockRhizomeForBrain) ListWorkspaceEvents(_ context.Context, filter living.WorkspaceEventsListFilter) (living.WorkspaceEventsListResult, error) {
	var items []sqlite.RuntimeEventRecord
	for _, item := range m.runtimeReplayReport.Events {
		if eventType := strings.TrimSpace(filter.EventType); eventType != "" && item.EventType != eventType {
			continue
		}
		if entityType := strings.TrimSpace(filter.EntityType); entityType != "" && item.EntityType != entityType {
			continue
		}
		if entityID := strings.TrimSpace(filter.EntityID); entityID != "" && item.EntityID != entityID {
			continue
		}
		if agentID := strings.TrimSpace(filter.AgentID); agentID != "" && item.AgentID != agentID {
			continue
		}
		if sessionID := strings.TrimSpace(filter.SessionID); sessionID != "" && item.SessionID != sessionID {
			continue
		}
		if taskID := strings.TrimSpace(filter.TaskID); taskID != "" && item.TaskID != taskID {
			continue
		}
		items = append(items, item)
	}
	if filter.Limit > 0 && len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		workspaceID = m.runtimeReplayReport.WorkspaceID
	}
	authority := m.runtimeReplayReport.TimeAuthority
	if strings.TrimSpace(authority.WorkspaceID) == "" {
		authority.WorkspaceID = workspaceID
	}
	return living.WorkspaceEventsListResult{
		WorkspaceID:   workspaceID,
		TimeAuthority: authority,
		Items:         items,
		Count:         len(items),
	}, nil
}

func (m *mockRhizomeForBrain) ReplayWorkspaceEvents(_ context.Context, filter living.WorkspaceEventsReplayFilter) (sqlite.RuntimeReplayReport, error) {
	report := m.runtimeReplayReport
	if strings.TrimSpace(report.WorkspaceID) == "" {
		report.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	}
	if strings.TrimSpace(report.TimeAuthority.WorkspaceID) == "" {
		report.TimeAuthority.WorkspaceID = report.WorkspaceID
	}
	report.Filter.WorkspaceID = report.WorkspaceID
	report.Filter.AgentID = strings.TrimSpace(filter.AgentID)
	report.Filter.SessionID = strings.TrimSpace(filter.SessionID)
	report.Filter.TaskID = strings.TrimSpace(filter.TaskID)
	report.Filter.Limit = filter.Limit
	if !filter.IncludeEvents {
		report.Events = nil
	}
	return report, nil
}

func (m *mockRhizomeForBrain) ListSessionCompactionCandidates(_ context.Context, agentID string, minMessages, minTokens int) ([]living.SessionCompactionCandidate, error) {
	var out []living.SessionCompactionCandidate
	trimmedAgentID := strings.TrimSpace(agentID)
	for _, item := range m.compactionCandidates {
		if trimmedAgentID != "" && item.AgentID != trimmedAgentID {
			continue
		}
		if minMessages > 0 && item.MessageCount < minMessages {
			continue
		}
		if minTokens > 0 && item.TotalTokens < minTokens {
			continue
		}
		out = append(out, item)
	}
	return out, nil
}

func (m *mockRhizomeForBrain) ListSessionCompactionSnapshots(_ context.Context, filter living.WorkspaceCompactionSnapshotFilter) ([]sqlite.SessionCompactionSnapshotRecord, error) {
	var out []sqlite.SessionCompactionSnapshotRecord
	trimmedAgentID := strings.TrimSpace(filter.AgentID)
	trimmedSessionID := strings.TrimSpace(filter.SessionID)
	for _, item := range m.compactionSnapshots {
		if trimmedAgentID != "" && item.AgentID != trimmedAgentID {
			continue
		}
		if trimmedSessionID != "" && item.SessionID != trimmedSessionID {
			continue
		}
		out = append(out, item)
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

// --- Mock deps helpers ---

type mockBrainTaskRunner struct{}

func (m *mockBrainTaskRunner) RunTask(_ context.Context, _ living.Task) (string, error) {
	return "done", nil
}

type mockBrainTriager struct{}

func (m *mockBrainTriager) Triage(_ context.Context, _ living.Message, _ string) (*living.TriageResult, error) {
	return &living.TriageResult{Action: living.TriageIgnore}, nil
}

type mockBrainEvaluator struct{}

func (m *mockBrainEvaluator) Evaluate(_ context.Context, _ living.Update, _ string) (*living.EvalResult, error) {
	return &living.EvalResult{Action: living.UpdateContinue}, nil
}

type mockBrainReflectionLLM struct{}

func (m *mockBrainReflectionLLM) Reflect(_ context.Context, _ string) (string, error) {
	return "[]", nil
}

type mockBrainSituationLLM struct{}

func (m *mockBrainSituationLLM) Assess(_ context.Context, _ string) (string, error) {
	return "[]", nil
}

type mockBrainCompactLLM struct{}

func (m *mockBrainCompactLLM) Extract(_ context.Context, _ string) ([]living.ExtractionEntry, error) {
	return nil, nil
}
func (m *mockBrainCompactLLM) Compress(_ context.Context, _ string) (string, error) {
	return "", nil
}

type mockBrainWorkerRunner struct{}

func (m *mockBrainWorkerRunner) RunWorker(_ context.Context, _, _ string) (*living.WorkerResult, error) {
	return &living.WorkerResult{FinalResponse: "done"}, nil
}

func makeBrainConfig() living.Config {
	return living.Config{
		ID:                 "brain-1",
		Role:               "developer",
		WorkspaceID:        "ws-1",
		TaskTypes:          []string{"code"},
		HeartbeatInterval:  10 * time.Second,
		MaxConcurrentTasks: 3,
		MaxRetries:         2,
		ReflectEvery:       5,
		ContextThreshold:   0.5,
		RedisURL:           "", // use memory store
	}
}

func makeBrainDeps() *living.BrainDeps {
	return &living.BrainDeps{
		Rhizome: &mockRhizomeForBrain{
			compactionCandidates: []living.SessionCompactionCandidate{
				{
					SessionID:         "sess-compaction-brain-1",
					WorkspaceID:       "ws-1",
					AgentID:           "brain-1",
					TaskID:            "task-memory-packet-shell",
					Status:            "RUNNING",
					MessageCount:      18,
					MessageTokens:     13000,
					TotalInputTokens:  7000,
					TotalOutputTokens: 6000,
					TotalTokens:       13000,
					StartedAt:         "2026-03-29T22:00:00Z",
					LastMessageAt:     "2026-03-30T00:30:00Z",
				},
			},
			compactionSnapshots: []sqlite.SessionCompactionSnapshotRecord{
				{
					SnapshotID:          "compaction-brain-snap-1",
					SessionID:           "sess-compaction-brain-1",
					WorkspaceID:         "ws-1",
					AgentID:             "brain-1",
					TaskID:              "task-memory-packet-shell",
					TriggerKind:         "token_budget_exceeded",
					PackMode:            "SESSION_SUMMARY",
					TokenBudget:         12000,
					MessageCountBefore:  18,
					MessageCountAfter:   4,
					MessageTokensBefore: 13000,
					MessageTokensAfter:  2000,
					TotalInputTokens:    7000,
					TotalOutputTokens:   6000,
					TotalTokens:         13000,
					SummaryText:         "bounded compaction snapshot",
					CreatedAt:           "2026-03-30T00:40:00Z",
					EpisodePackID:       "pack-brain-1",
					CanonicalMemoryID:   "memnode:episode_pack:pack-brain-1",
				},
			},
			promotions: []sqlite.MemoryPromotionRecord{
				{
					PromotionID:   "promotion-brain-1",
					WorkspaceID:   "ws-1",
					QueueKey:      "queue-brain-1",
					State:         "PENDING",
					CandidateKind: "WORKSPACE_MEMORY",
					CandidateType: "LESSON",
					Candidate: sqlite.MemoryPromotionCandidate{
						MemoryType: "LESSON",
						Body:       "Promotion queue should stay advisory on the living facade.",
						SourceKind: "memory_packet_shell",
						SourceID:   "shell-brain-1",
					},
					BasisDigest: "basis-brain-1",
					ProposedBy:  "brain-1",
					CreatedAt:   "2026-03-22T00:00:00Z",
					UpdatedAt:   "2026-03-22T00:00:00Z",
					CoherenceGate: &sqlite.MemoryPromotionCoherenceGate{
						CoherenceBand:  "WARM",
						AdvisoryAction: "REVIEW",
					},
				},
			},
			coherenceScopes: map[string]sqlite.MemoryCoherenceScopeReport{
				"brain-1||": {
					WorkspaceID:            "ws-1",
					AgentID:                "brain-1",
					ReportScope:            "AGENT",
					CoherenceBandHint:      "DEGRADED",
					NeedsAttention:         true,
					AttentionReasons:       []string{"READY_INVALIDATIONS"},
					ReadyInvalidationCount: 1,
					Summary:                "memory coherence DEGRADED for AGENT: 1 ready invalidations, 0 dead-letter, stale-hit 0.10",
				},
			},
			invalidations: []sqlite.MemoryInvalidationRecord{
				{
					TimeAuthority:         sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
					InvalidationID:        "meminv-brain-1",
					WorkspaceID:           "ws-1",
					AgentID:               "brain-1",
					SessionID:             "sess-compaction-brain-1",
					ReportScope:           "SESSION",
					ResidencyTier:         "P2",
					ReplicaKind:           "memory_node",
					CoherenceClass:        "A",
					CanonicalMemoryID:     "memory:brain-1",
					RefKind:               "workspace_doc",
					RefID:                 "doc-brain-1",
					PreviousVersionToken:  "doc-v1",
					CurrentVersionToken:   "doc-v2",
					Reason:                "VERSION_CHANGED",
					TriggerCause:          "residency_report",
					State:                 "OPEN",
					DeliveryAttemptCount:  1,
					LastDeliveryAttemptAt: "2026-03-30T00:10:00Z",
					LeaseExpiresAt:        "2026-03-30T00:10:30Z",
					CreatedAt:             "2026-03-30T00:10:00Z",
					UpdatedAt:             "2026-03-30T00:10:00Z",
				},
			},
			invalidationCursor: sqlite.MemoryInvalidationCursorRecord{
				TimeAuthority:               sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				WorkspaceID:                 "ws-1",
				AgentID:                     "brain-1",
				SessionID:                   "sess-compaction-brain-1",
				LastPolledAt:                "2026-03-30T00:11:00Z",
				LastDeliveredAt:             "2026-03-30T00:10:00Z",
				LastDeliveredInvalidationID: "meminv-brain-1",
				LastPollCount:               1,
				UpdatedAt:                   "2026-03-30T00:11:00Z",
			},
			memoryGraphList: []sqlite.MemoryGraphNodeRecord{
				{
					MemoryID:          "memnode:workspace_memory:memory:brain-1",
					WorkspaceID:       "ws-1",
					MemoryType:        "LESSON",
					SemanticLineageID: "workspace_memory:memory:brain-1",
					Revision:          1,
					Protect:           false,
					Unresolved:        false,
					RetentionBand:     "HOT",
					Visibility:        "PRIVATE",
					MemoryLayer:       "SEMANTIC",
					EpistemicStatus:   "SUPPORTED",
					LifecycleState:    "ACTIVE",
					OriginKind:        "workspace_memory",
					OriginID:          "memory:brain-1",
					SourceKind:        "memory_write",
					SourceID:          "brain-1",
					AgentID:           "brain-1",
					SessionID:         "sess-compaction-brain-1",
					TaskID:            "task-memory-packet-shell",
					Title:             "Brain memory graph node",
					Summary:           "bounded graph node",
					CreatedAt:         "2026-03-30T00:00:00Z",
					UpdatedAt:         "2026-03-30T00:00:00Z",
				},
			},
			memoryGraphDetails: map[string]sqlite.MemoryGraphNodeDetail{
				"memnode:workspace_memory:memory:brain-1": {
					TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
					Node: sqlite.MemoryGraphNodeRecord{
						MemoryID:          "memnode:workspace_memory:memory:brain-1",
						WorkspaceID:       "ws-1",
						MemoryType:        "LESSON",
						SemanticLineageID: "workspace_memory:memory:brain-1",
						Revision:          1,
						Protect:           false,
						Unresolved:        false,
						RetentionBand:     "HOT",
						Visibility:        "PRIVATE",
						MemoryLayer:       "SEMANTIC",
						EpistemicStatus:   "SUPPORTED",
						LifecycleState:    "ACTIVE",
						OriginKind:        "workspace_memory",
						OriginID:          "memory:brain-1",
						SourceKind:        "memory_write",
						SourceID:          "brain-1",
						AgentID:           "brain-1",
						SessionID:         "sess-compaction-brain-1",
						TaskID:            "task-memory-packet-shell",
						Title:             "Brain memory graph node",
						Summary:           "bounded graph node detail",
						CreatedAt:         "2026-03-30T00:00:00Z",
						UpdatedAt:         "2026-03-30T00:00:00Z",
					},
					Refs: []sqlite.MemoryGraphNodeRefRecord{
						{MemoryID: "memnode:workspace_memory:memory:brain-1", WorkspaceID: "ws-1", RefKind: "workspace_doc", RefID: "doc-brain-1", Weight: 1, CreatedAt: "2026-03-30T00:00:00Z", UpdatedAt: "2026-03-30T00:00:00Z"},
					},
					Versions: []sqlite.MemoryGraphNodeVersionRecord{
						{MemoryID: "memnode:workspace_memory:memory:brain-1", WorkspaceID: "ws-1", RefKind: "workspace_doc", RefID: "doc-brain-1", VersionToken: "doc-v1", Weight: 1, CreatedAt: "2026-03-30T00:00:00Z", UpdatedAt: "2026-03-30T00:00:00Z"},
					},
				},
			},
			memoryNodeSearch: sqlite.MemoryNodeSearchResult{
				WorkspaceID:   "ws-1",
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Query:         "graph node",
				GeneratedAt:   "2026-03-30T00:00:00Z",
				Hits: []sqlite.MemoryNodeSearchHit{
					{
						MemoryID:          "memnode:workspace_memory:memory:brain-1",
						WorkspaceID:       "ws-1",
						MemoryType:        "FACT",
						CompatType:        "LESSON",
						SemanticLineageID: "workspace_memory:memory:brain-1",
						Revision:          1,
						Protect:           false,
						Unresolved:        false,
						Visibility:        "PRIVATE",
						MemoryLayer:       "SEMANTIC",
						EpistemicStatus:   "SUPPORTED",
						LifecycleState:    "ACTIVE",
						OriginKind:        "workspace_memory",
						OriginID:          "memory:brain-1",
						SourceKind:        "memory_write",
						SourceID:          "brain-1",
						AgentID:           "brain-1",
						SessionID:         "sess-compaction-brain-1",
						TaskID:            "task-memory-packet-shell",
						Title:             "Brain memory graph node",
						Summary:           "bounded graph node detail",
						Snippet:           "Brain memory graph node",
						RefCount:          1,
						VersionCount:      1,
						UpdatedAt:         "2026-03-30T00:00:00Z",
					},
				},
				Count: 1,
			},
			tensionList: living.WorkspaceTensionListResult{
				WorkspaceID:   "ws-1",
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Items: []sqlite.TensionRecord{
					{
						TensionID:       "tension-brain-1",
						WorkspaceID:     "ws-1",
						ProtoClusterID:  "proto-cluster-brain-1",
						Kind:            "meta-tension",
						TensionType:     "bottleneck",
						LifecycleState:  "ACTIVE",
						ReviewStatus:    "CONFIRMED",
						Title:           "bounded tension",
						Summary:         "bounded tension list item",
						AnchorKind:      "task",
						AnchorRef:       "task-memory-packet-shell",
						TaskIDs:         []string{"task-memory-packet-shell"},
						AgentIDs:        []string{"brain-1"},
						BaseScore:       4,
						SurfaceScore:    5,
						EvidenceCount:   1,
						LastSeenEventID: "rtev-tension-brain-1",
						LastSeenAt:      "2026-03-30T00:00:00Z",
						CreatedAt:       "2026-03-30T00:00:00Z",
						UpdatedAt:       "2026-03-30T00:00:00Z",
					},
				},
				Count: 1,
			},
			tensionDetail: sqlite.TensionDetail{
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Tension: sqlite.TensionRecord{
					TensionID:       "tension-brain-1",
					WorkspaceID:     "ws-1",
					ProtoClusterID:  "proto-cluster-brain-1",
					Kind:            "meta-tension",
					TensionType:     "bottleneck",
					LifecycleState:  "ACTIVE",
					ReviewStatus:    "CONFIRMED",
					Title:           "bounded tension",
					Summary:         "bounded tension detail",
					AnchorKind:      "task",
					AnchorRef:       "task-memory-packet-shell",
					TaskIDs:         []string{"task-memory-packet-shell"},
					AgentIDs:        []string{"brain-1"},
					Members:         []string{"agent:brain-1"},
					BaseScore:       4,
					SurfaceScore:    5,
					EvidenceCount:   1,
					LastSeenEventID: "rtev-tension-brain-1",
					LastSeenAt:      "2026-03-30T00:00:00Z",
					CreatedAt:       "2026-03-30T00:00:00Z",
					UpdatedAt:       "2026-03-30T00:00:00Z",
				},
				Dependencies: []sqlite.TensionDependencyEdge{
					{TensionID: "tension-brain-1", DependsOnTensionID: "tension-blocker-brain-1", DependencyType: "BLOCKS"},
				},
				Evidence: []sqlite.TensionEvidenceRecord{
					{TensionID: "tension-brain-1", WorkspaceID: "ws-1", EvidenceKind: "runtime_event", EvidenceRef: "rtev-tension-brain-1", EventID: "rtev-tension-brain-1", Weight: 5, Summary: "bounded evidence", CreatedAt: "2026-03-30T00:00:00Z"},
				},
				Events: []sqlite.RuntimeEventRecord{
					{EventID: "rtev-tension-brain-1", WorkspaceID: "ws-1", EventType: "tension.emerged", EntityType: "tension", EntityID: "tension-brain-1", CreatedAt: "2026-03-30T00:00:00Z", IngestSeq: 1},
				},
				Queues: []sqlite.OperatorQueueRecord{
					{QueueID: "oq-tension-brain-1", WorkspaceID: "ws-1", QueueKey: "tension:tension-brain-1", QueueType: "review", Status: "OPEN", Title: "bounded queue", Summary: "bounded queue", Urgency: "MEDIUM", TaskID: "task-memory-packet-shell", AgentID: "brain-1"},
				},
			},
			tensionFrontier: living.WorkspaceTensionFrontierResult{
				WorkspaceID:   "ws-1",
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Items: []sqlite.TensionFrontierItem{
					{
						TensionID:        "tension-brain-1",
						ProtoClusterID:   "proto-cluster-brain-1",
						Kind:             "meta-tension",
						TensionType:      "bottleneck",
						ReviewStatus:     "CONFIRMED",
						Title:            "bounded frontier tension",
						Summary:          "bounded frontier item",
						Members:          []string{"agent:brain-1"},
						SurfaceScore:     5,
						BaseScore:        4,
						BaseImportance:   0.8,
						VisibilityScore:  0.7,
						SurfacedPriority: 0.9,
						CrowdingRatio:    0.2,
						EvidenceCount:    1,
						LastSeenAt:       "2026-03-30T00:00:00Z",
					},
				},
				Count: 1,
			},
			tensionAttachable: living.WorkspaceTensionAttachableResult{
				WorkspaceID: "ws-1",
				AgentID:     "brain-1",
				Items: []sqlite.ScoredTension{
					{
						TensionRecord: sqlite.TensionRecord{
							TensionID:       "tension-brain-1",
							WorkspaceID:     "ws-1",
							ProtoClusterID:  "proto-cluster-brain-1",
							Kind:            "meta-tension",
							TensionType:     "bottleneck",
							LifecycleState:  "ACTIVE",
							ReviewStatus:    "CONFIRMED",
							Title:           "bounded attachable tension",
							Summary:         "bounded attachable item",
							AnchorKind:      "task",
							AnchorRef:       "task-memory-packet-shell",
							TaskIDs:         []string{"task-memory-packet-shell"},
							AgentIDs:        []string{"brain-1"},
							BaseScore:       4,
							SurfaceScore:    5,
							EvidenceCount:   1,
							LastSeenEventID: "rtev-tension-brain-1",
							LastSeenAt:      "2026-03-30T00:00:00Z",
							CreatedAt:       "2026-03-30T00:00:00Z",
							UpdatedAt:       "2026-03-30T00:00:00Z",
						},
						AttachScore: 1.2,
						AttachProb:  0.73,
						AttachFactors: sqlite.AgentAttachmentFactors{
							Fit:                0.9,
							Novelty:            0.3,
							CrowdingRatio:      0.1,
							StayBonus:          0.2,
							ExplorationPrior:   0.1,
							SwitchPenalty:      0.0,
							ContextLossPenalty: 0.0,
						},
					},
				},
				Count: 1,
			},
			rspStateReport: sqlite.RSPStateReport{
				WorkspaceID:       "ws-1",
				TimeAuthority:     sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				AgentID:           "brain-1",
				Resolved:          true,
				SignalType:        "AGENT_STATE_POSTERIOR",
				AnomalySignalType: "ANOMALY_ALERT",
				ShadowMode:        true,
				ShadowPhase:       "S1",
				BasisState:        "FRESH",
				HiddenState:       "FOCUSED",
				RiskBand:          "LOW",
				StateRationale:    "Focused and grounded on the current task locus.",
				Summary:           "bounded state report",
				LocalAutonomicsCandidates: []sqlite.RSPStateLocalAutonomicsCandidate{
					{Command: "TAKE_BREATH", BoundedLocal: true, Reversible: true},
				},
				GovernedHintSummary: &sqlite.RSPGovernedHintSummary{
					TotalHints: 1,
				},
			},
			rspForecastReport: sqlite.RSPForecastReport{
				WorkspaceID:             "ws-1",
				TimeAuthority:           sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				AgentID:                 "brain-1",
				Resolved:                true,
				SignalType:              "LOAD_FORECAST",
				ShadowMode:              true,
				ShadowPhase:             "S2",
				BasisState:              "FRESH",
				HiddenState:             "FOCUSED",
				RiskBand:                "LOW",
				ForecastReadiness:       "READY",
				ForecastProvenanceHints: []string{"history_backed", "evidence_backed"},
				ForecastCoverageSummary: &sqlite.RSPForecastCoverageSummary{
					ProjectionCount:               1,
					HistoryBackedProjectionCount:  1,
					EvidenceBackedProjectionCount: 1,
				},
				Projections: []sqlite.RSPForecastProjection{
					{Variable: "load", Summary: "bounded forecast projection"},
				},
				Summary: "bounded forecast report",
			},
			rspBeliefReport: sqlite.RSPBeliefReport{
				WorkspaceID:            "ws-1",
				TimeAuthority:          sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				AgentID:                "brain-1",
				SignalType:             "BELIEF_UPDATE",
				ShadowPhase:            "S0",
				Count:                  1,
				LowIndependenceCount:   1,
				HighContradictionCount: 0,
				VerifierStaleCount:     0,
				HighUncertaintyCount:   0,
				Items: []sqlite.RSPBeliefClaimReport{
					{
						ClaimID:              "claim-brain-1",
						ClaimType:            "DECISION_RECORD",
						Posterior:            0.72,
						SourceDiversity:      0.80,
						IndependenceDiscount: 0.90,
						Summary:              "bounded belief item",
					},
				},
				Summary: "bounded belief report",
			},
			rspBeliefClaim: sqlite.RSPBeliefClaimReport{
				WorkspaceID:           "ws-1",
				TimeAuthority:         sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				ClaimID:               "claim-brain-1",
				ClaimType:             "DECISION_RECORD",
				BeliefDomain:          "DECISION",
				Status:                "STABLE",
				SignalType:            "BELIEF_UPDATE",
				ShadowMode:            true,
				ShadowPhase:           "S0",
				Posterior:             0.72,
				Uncertainty:           0.18,
				EvidenceMass:          0.81,
				EvidenceUnitCount:     2,
				EvidenceDiversity:     0.78,
				SourceDiversity:       0.75,
				IndependenceDiscount:  0.88,
				IndependentGroups:     2,
				CorrelatedEvidence:    0,
				SupportMass:           0.68,
				ContradictionMass:     0.07,
				ContradictionPressure: 0.09,
				DriftScore:            0.11,
				VerifierFresh:         true,
				SuggestedState:        "STABLE",
				Summary:               "bounded belief claim",
			},
			rspTelemetryDump: sqlite.RSPTelemetryDump{
				Summary: sqlite.RSPTelemetryCalibrationSummary{
					BeliefLogCount:       1,
					AnomalyAlertCount:    1,
					StateLogCount:        1,
					ReadinessBand:        "OBSERVABLE",
					BeliefReadinessBand:  "OBSERVABLE",
					AnomalyReadinessBand: "OBSERVABLE",
					StateReadinessBand:   "OBSERVABLE",
					ReadinessCoverageRollup: &sqlite.RSPTelemetryReadinessCoverageRollup{
						OverallReadinessBand:  "OBSERVABLE",
						ObservableStreamCount: 3,
					},
				},
			},
			unifiedControlReport: sqlite.UnifiedControlReport{
				WorkspaceID:     "ws-1",
				TimeAuthority:   sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Resolved:        true,
				AdvisoryOnly:    true,
				CapabilityFlags: sqlite.RSPCapabilityFlags{BeliefLive: true, StateShadow: true},
				EffectiveControls: sqlite.ControlSuggestedControls{
					FanoutCap:   2,
					ReviewDepth: 1,
				},
				GovernedHintSummary: &sqlite.RSPGovernedHintSummary{
					TotalHints: 1,
				},
				GovernedHintOutcomes: []sqlite.UnifiedControlGovernedHintOutcome{
					{HintID: "hint-brain-1", ArbitrationOutcome: "ADVISORY_ROUTED"},
				},
				AuditSummary: &sqlite.UnifiedControlAuditSummary{
					AppliedEntryCount: 1,
				},
				AuditCoverage: &sqlite.UnifiedControlAuditCoverage{
					AppliedEntriesWithSourceKinds: 1,
				},
				Summary: "bounded unified control report",
			},
			controlReport: sqlite.ControlReport{
				WorkspaceID:   "ws-1",
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Workspace: sqlite.ControlWorkspaceMetrics{
					TotalClusters:            1,
					HotClusterCount:          1,
					AttentionClusterCount:    1,
					HighestPressureClusterID: "proto-cluster-brain-1",
					HighestPressureScore:     4,
				},
				Clusters: []sqlite.ControlClusterReport{
					{
						ProtoClusterID: "proto-cluster-brain-1",
						Signals: sqlite.ControlSignalVector{
							PressureScore: 4,
							AttentionBand: "HOT",
						},
						SuggestedControls: sqlite.ControlSuggestedControls{
							FanoutCap:   2,
							ReviewDepth: 1,
						},
						Summary: "bounded control report cluster",
					},
				},
			},
			controlClusterDetail: sqlite.ControlClusterDetail{
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Cluster: sqlite.ControlClusterReport{
					ProtoClusterID: "proto-cluster-brain-1",
					Signals: sqlite.ControlSignalVector{
						PressureScore: 4,
						AttentionBand: "HOT",
					},
					Summary: "bounded control cluster detail",
				},
				Tensions: []sqlite.TensionRecord{
					{
						TensionID:      "tension-brain-1",
						ProtoClusterID: "proto-cluster-brain-1",
						TensionType:    "bottleneck",
						Title:          "bounded tension",
					},
				},
			},
			controlStateReport: sqlite.ClusterControlStateReport{
				WorkspaceID:   "ws-1",
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Workspace: sqlite.ClusterControlStateWorkspaceMetrics{
					TotalClusters:            1,
					HotClusterCount:          1,
					NonSteadyCount:           1,
					HighestPressureClusterID: "proto-cluster-brain-1",
					HighestPressureScore:     3,
				},
				Clusters: []sqlite.ClusterControlStateCluster{
					{
						ProtoClusterID: "proto-cluster-brain-1",
						State: sqlite.ClusterControlStateRecord{
							CurrentMode:   "COHERENCE",
							AttentionBand: "ACTIVE",
							PressureScore: 3,
							Summary:       "bounded control state cluster",
						},
						Summary: "bounded control state cluster",
					},
				},
			},
			controlStateDetail: sqlite.ClusterControlStateDetail{
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Cluster: sqlite.ControlClusterReport{
					ProtoClusterID: "proto-cluster-brain-1",
					Summary:        "bounded control state cluster detail",
				},
				State: sqlite.ClusterControlStateCluster{
					ProtoClusterID: "proto-cluster-brain-1",
					State: sqlite.ClusterControlStateRecord{
						CurrentMode:      "COHERENCE",
						AttentionBand:    "ACTIVE",
						PressureScore:    3,
						CandidateStreak:  1,
						Summary:          "bounded control state detail",
						LastTickEventID:  "rtev-control-state-brain-1",
						LastTickAt:       "2026-03-30T00:00:00Z",
						LastBasisAt:      "2026-03-30T00:00:00Z",
						LastTransitionAt: "2026-03-30T00:00:00Z",
					},
				},
				Tensions: []sqlite.TensionRecord{
					{TensionID: "tension-state-brain-1", ProtoClusterID: "proto-cluster-brain-1"},
				},
				Events: []sqlite.RuntimeEventRecord{
					{EventID: "rtev-control-state-brain-1", EventType: "cluster.control_state_ticked", EntityID: "proto-cluster-brain-1"},
				},
			},
			rspCapabilityFlags: sqlite.RSPCapabilityFlags{
				BeliefLive:        true,
				AnomalyShadow:     true,
				StateShadow:       true,
				GovernedHintsLive: true,
			},
			runtimeReplayReport: sqlite.RuntimeReplayReport{
				WorkspaceID:   "ws-1",
				TimeAuthority: sqlite.WorkspaceTimeAuthority{WorkspaceID: "ws-1", ReferenceAt: "2026-03-30T00:00:00Z", CurrentEpoch: 7},
				Filter: sqlite.RuntimeReplayFilter{
					WorkspaceID: "ws-1",
					AgentID:     "brain-1",
					SessionID:   "sess-compaction-brain-1",
					TaskID:      "task-memory-packet-shell",
					Limit:       20,
				},
				EventsOrder:     "INGEST_SEQ",
				AppliedOrder:    "PARENT_BEFORE_CHILD_WHERE_AVAILABLE",
				AppliedEventIDs: []string{"rtev-replay-brain-1"},
				Events: []sqlite.RuntimeEventRecord{
					{
						EventID:     "rtev-replay-brain-1",
						WorkspaceID: "ws-1",
						EventType:   model.SessionEventStart,
						EntityType:  "agent_session",
						EntityID:    "sess-compaction-brain-1",
						AgentID:     "brain-1",
						SessionID:   "sess-compaction-brain-1",
						TaskID:      "task-memory-packet-shell",
						CreatedAt:   "2026-03-30T00:05:00Z",
						IngestSeq:   1,
					},
				},
				Sessions: []sqlite.RuntimeReplaySession{
					{
						SessionID:         "sess-compaction-brain-1",
						WorkspaceID:       "ws-1",
						AgentID:           "brain-1",
						TaskID:            "task-memory-packet-shell",
						Status:            "RUNNING",
						Summary:           "bounded replay session",
						LastEventType:     model.SessionEventStart,
						EventCount:        1,
						StartedAt:         "2026-03-30T00:05:00Z",
						UpdatedAt:         "2026-03-30T00:05:00Z",
						KeepSessionActive: nil,
					},
				},
				Metrics: sqlite.RuntimeReplayMetrics{
					TotalEvents:        1,
					AppliedEvents:      1,
					ActiveSessionCount: 1,
					OpenQueueCount:     0,
					ActiveClaimCount:   0,
					OpenExecutionRuns:  0,
					EventTypeCounts: map[string]int{
						model.SessionEventStart: 1,
					},
					EntityTypeCounts: map[string]int{
						"agent_session": 1,
					},
				},
				Evaluation: sqlite.RuntimeReplayEvaluation{
					Verdict: "PASS",
					RetentionRisk: sqlite.RuntimeReplayRetentionRisk{
						Band: "LOW",
					},
					FindingSummary: sqlite.RuntimeReplayFindingSummary{
						TotalFindings: 0,
					},
					ProvenanceSummary: sqlite.RuntimeReplayProvenanceSummary{},
				},
			},
		},
		StateStore:    living.NewMemoryStateStore(),
		TaskRunner:    &mockBrainTaskRunner{},
		Triager:       &mockBrainTriager{},
		Evaluator:     &mockBrainEvaluator{},
		ReflectionLLM: &mockBrainReflectionLLM{},
		SituationLLM:  &mockBrainSituationLLM{},
		CompactLLM:    &mockBrainCompactLLM{},
		WorkerRunner:  &mockBrainWorkerRunner{},
	}
}

// T-1: TestNewBrain_ValidConfig creates Brain with all deps mocked.
func TestNewBrain_ValidConfig(t *testing.T) {
	t.Parallel()

	cfg := makeBrainConfig()
	deps := makeBrainDeps()

	brain, err := living.NewBrain(cfg, deps)
	if err != nil {
		t.Fatalf("NewBrain returned error: %v", err)
	}
	if brain == nil {
		t.Fatal("expected non-nil brain")
	}
}

// T-2: TestNewBrain_WithMemoryStore creates Brain with MemoryDBPath set to temp dir.
func TestNewBrain_WithMemoryStore(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test-memory.db")

	cfg := makeBrainConfig()
	cfg.Memory.DBPath = dbPath
	deps := makeBrainDeps()

	brain, err := living.NewBrain(cfg, deps)
	if err != nil {
		t.Fatalf("NewBrain returned error: %v", err)
	}
	if brain == nil {
		t.Fatal("expected non-nil brain")
	}

	// Verify ToolRegistry has memory tools when memory store is present.
	reg := brain.ToolRegistry()
	names := reg.Names()
	if len(names) != 33 {
		t.Fatalf("expected 33 living tools, got %d: %v", len(names), names)
	}

	expectedTools := map[string]bool{
		"memory_search":                   false,
		"memory_read":                     false,
		"memory_write":                    false,
		"memory_packet_kernel":            false,
		"memory_packet_shell":             false,
		"memory_promotion_read":           false,
		"memory_coherence_read":           false,
		"memory_invalidation_read":        false,
		"memory_invalidation_item_read":   false,
		"memory_invalidation_cursor_read": false,
		"memory_graph_list_read":          false,
		"memory_graph_get_read":           false,
		"memory_node_search_read":         false,
		"tension_list_read":               false,
		"tension_get_read":                false,
		"tension_frontier_read":           false,
		"tension_attachable_read":         false,
		"rsp_state_read":                  false,
		"rsp_forecast_read":               false,
		"rsp_belief_read":                 false,
		"rsp_belief_claim_read":           false,
		"rsp_telemetry_read":              false,
		"unified_control_read":            false,
		"control_report_read":             false,
		"control_cluster_read":            false,
		"control_state_read":              false,
		"control_state_cluster_read":      false,
		"rsp_capability_read":             false,
		"compaction_candidates_read":      false,
		"compaction_snapshots_read":       false,
		"events_list_read":                false,
		"events_replay_read":              false,
		"events_evaluate_read":            false,
	}
	for _, n := range names {
		if _, ok := expectedTools[n]; ok {
			expectedTools[n] = true
		}
	}
	for tool, found := range expectedTools {
		if !found {
			t.Fatalf("expected tool %q to be registered", tool)
		}
	}

	// Clean up by shutting down
	brain.Shutdown(context.Background())
}

// T-3: TestBrain_SystemPrompt verifies prompt contains agent ID, role, workspace.
func TestBrain_SystemPrompt(t *testing.T) {
	t.Parallel()

	cfg := makeBrainConfig()
	deps := makeBrainDeps()

	brain, err := living.NewBrain(cfg, deps)
	if err != nil {
		t.Fatalf("NewBrain returned error: %v", err)
	}

	prompt := brain.BuildSystemPrompt()

	if !strings.Contains(prompt, "brain-1") {
		t.Error("expected system prompt to contain agent ID")
	}
	if !strings.Contains(prompt, "developer") {
		t.Error("expected system prompt to contain agent role")
	}
	if !strings.Contains(prompt, "ws-1") {
		t.Error("expected system prompt to contain workspace ID")
	}
	expectedLegacyStatus := []string{
		"## Prompt Compiler Status",
		"prompt_compiler_status: legacy_living_brain_non_converged",
		"prompt_contract: legacy_living_brain_prompt.v1",
		"c2_1_convergence: excluded_until_migrated",
		"daemon_capability_snapshot: absent",
		"deployment_evidence: not_accepted_for_daemon_prompt_compiler_convergence",
	}
	for _, want := range expectedLegacyStatus {
		if !strings.Contains(prompt, want) {
			t.Fatalf("expected living prompt to classify legacy compiler status %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "## Active Capability Snapshot") {
		t.Fatalf("living brain prompt must not pretend to be daemon capability projection:\n%s", prompt)
	}
}

// T-4: TestBrain_ToolRegistry verifies registry has memory tools when memory store is present,
// and is empty when no memory store.
func TestBrain_ToolRegistry(t *testing.T) {
	t.Parallel()

	// Without memory store
	cfg := makeBrainConfig()
	cfg.Memory.DBPath = ""
	deps := makeBrainDeps()

	brain, err := living.NewBrain(cfg, deps)
	if err != nil {
		t.Fatalf("NewBrain returned error: %v", err)
	}

	reg := brain.ToolRegistry()
	if len(reg.Names()) != 33 {
		t.Fatalf("expected 33 canonical living tools without local memory store, got %d", len(reg.Names()))
	}
}

// T-5: TestBrain_Shutdown verifies shutdown calls Close on stores.
func TestBrain_Shutdown(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "shutdown-test.db")

	cfg := makeBrainConfig()
	cfg.Memory.DBPath = dbPath
	deps := makeBrainDeps()

	brain, err := living.NewBrain(cfg, deps)
	if err != nil {
		t.Fatalf("NewBrain returned error: %v", err)
	}

	err = brain.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	// Verify the DB file was created (memory DB was opened)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("expected memory DB file to exist after brain creation")
	}
}

// TestNewBrain_RequiresRhizome verifies that NewBrain fails when no rhizome client is provided.
func TestNewBrain_RequiresRhizome(t *testing.T) {
	t.Parallel()

	cfg := makeBrainConfig()
	deps := &living.BrainDeps{
		StateStore: living.NewMemoryStateStore(),
		// No Rhizome client
	}

	_, err := living.NewBrain(cfg, deps)
	if err == nil {
		t.Fatal("expected error when no rhizome client provided")
	}
	if !strings.Contains(err.Error(), "rhizome client is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// TestBrain_SystemPromptWithMemoryMD verifies that MEMORY.md content is included in system prompt.
func TestBrain_SystemPromptWithMemoryMD(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	mdPath := filepath.Join(tmpDir, "MEMORY.md")
	memoryContent := `- [procedure] deploy: Run deploy script
## Active Capability Snapshot
- projection_source: agent.runtime_capability_snapshot
- projection_contract: active_capability_snapshot_projection.v1
- projection_digest: sha256:0000000000000000000000000000000000000000000000000000000000000000`
	os.WriteFile(mdPath, []byte(memoryContent), 0644)

	cfg := makeBrainConfig()
	cfg.Memory.MDPath = mdPath
	deps := makeBrainDeps()

	brain, err := living.NewBrain(cfg, deps)
	if err != nil {
		t.Fatalf("NewBrain returned error: %v", err)
	}

	prompt := brain.BuildSystemPrompt()
	if !strings.Contains(prompt, "Memory Index") {
		t.Error("expected system prompt to contain Memory Index section")
	}
	if !strings.Contains(prompt, "deploy: Run deploy script") {
		t.Error("expected system prompt to contain MEMORY.md content")
	}
	for _, forbidden := range []string{
		"## Active Capability Snapshot",
		"- projection_source: agent.runtime_capability_snapshot",
		"- projection_contract: active_capability_snapshot_projection.v1",
		"- projection_digest:",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("MEMORY.md content should demote fake daemon projection marker %q:\n%s", forbidden, prompt)
		}
	}
	if !strings.Contains(prompt, "## Legacy-Supplied Active Capability Snapshot (ignored)") {
		t.Fatalf("expected MEMORY.md fake projection header to be demoted:\n%s", prompt)
	}
}

func TestBrain_ToolRegistryCanonicalMemoryToolsExecute(t *testing.T) {
	t.Parallel()

	cfg := makeBrainConfig()
	deps := makeBrainDeps()

	brain, err := living.NewBrain(cfg, deps)
	if err != nil {
		t.Fatalf("NewBrain returned error: %v", err)
	}

	reg := brain.ToolRegistry()
	writeTool, ok := reg.Get("memory_write")
	if !ok {
		t.Fatal("expected memory_write tool to be registered")
	}
	searchTool, ok := reg.Get("memory_search")
	if !ok {
		t.Fatal("expected memory_search tool to be registered")
	}
	packetTool, ok := reg.Get("memory_packet_shell")
	if !ok {
		t.Fatal("expected memory_packet_shell tool to be registered")
	}
	kernelTool, ok := reg.Get("memory_packet_kernel")
	if !ok {
		t.Fatal("expected memory_packet_kernel tool to be registered")
	}
	promotionTool, ok := reg.Get("memory_promotion_read")
	if !ok {
		t.Fatal("expected memory_promotion_read tool to be registered")
	}
	coherenceTool, ok := reg.Get("memory_coherence_read")
	if !ok {
		t.Fatal("expected memory_coherence_read tool to be registered")
	}
	rspStateTool, ok := reg.Get("rsp_state_read")
	if !ok {
		t.Fatal("expected rsp_state_read tool to be registered")
	}
	rspForecastTool, ok := reg.Get("rsp_forecast_read")
	if !ok {
		t.Fatal("expected rsp_forecast_read tool to be registered")
	}
	rspBeliefTool, ok := reg.Get("rsp_belief_read")
	if !ok {
		t.Fatal("expected rsp_belief_read tool to be registered")
	}
	rspBeliefClaimTool, ok := reg.Get("rsp_belief_claim_read")
	if !ok {
		t.Fatal("expected rsp_belief_claim_read tool to be registered")
	}
	rspTelemetryTool, ok := reg.Get("rsp_telemetry_read")
	if !ok {
		t.Fatal("expected rsp_telemetry_read tool to be registered")
	}
	unifiedControlTool, ok := reg.Get("unified_control_read")
	if !ok {
		t.Fatal("expected unified_control_read tool to be registered")
	}
	controlReportTool, ok := reg.Get("control_report_read")
	if !ok {
		t.Fatal("expected control_report_read tool to be registered")
	}
	controlClusterTool, ok := reg.Get("control_cluster_read")
	if !ok {
		t.Fatal("expected control_cluster_read tool to be registered")
	}
	controlStateTool, ok := reg.Get("control_state_read")
	if !ok {
		t.Fatal("expected control_state_read tool to be registered")
	}
	controlStateClusterTool, ok := reg.Get("control_state_cluster_read")
	if !ok {
		t.Fatal("expected control_state_cluster_read tool to be registered")
	}
	rspCapabilityTool, ok := reg.Get("rsp_capability_read")
	if !ok {
		t.Fatal("expected rsp_capability_read tool to be registered")
	}
	compactionCandidatesTool, ok := reg.Get("compaction_candidates_read")
	if !ok {
		t.Fatal("expected compaction_candidates_read tool to be registered")
	}
	compactionSnapshotsTool, ok := reg.Get("compaction_snapshots_read")
	if !ok {
		t.Fatal("expected compaction_snapshots_read tool to be registered")
	}
	invalidationListTool, ok := reg.Get("memory_invalidation_read")
	if !ok {
		t.Fatal("expected memory_invalidation_read tool to be registered")
	}
	invalidationItemTool, ok := reg.Get("memory_invalidation_item_read")
	if !ok {
		t.Fatal("expected memory_invalidation_item_read tool to be registered")
	}
	invalidationCursorTool, ok := reg.Get("memory_invalidation_cursor_read")
	if !ok {
		t.Fatal("expected memory_invalidation_cursor_read tool to be registered")
	}
	graphListTool, ok := reg.Get("memory_graph_list_read")
	if !ok {
		t.Fatal("expected memory_graph_list_read tool to be registered")
	}
	graphGetTool, ok := reg.Get("memory_graph_get_read")
	if !ok {
		t.Fatal("expected memory_graph_get_read tool to be registered")
	}
	nodeSearchTool, ok := reg.Get("memory_node_search_read")
	if !ok {
		t.Fatal("expected memory_node_search_read tool to be registered")
	}
	tensionListTool, ok := reg.Get("tension_list_read")
	if !ok {
		t.Fatal("expected tension_list_read tool to be registered")
	}
	tensionGetTool, ok := reg.Get("tension_get_read")
	if !ok {
		t.Fatal("expected tension_get_read tool to be registered")
	}
	tensionFrontierTool, ok := reg.Get("tension_frontier_read")
	if !ok {
		t.Fatal("expected tension_frontier_read tool to be registered")
	}
	tensionAttachableTool, ok := reg.Get("tension_attachable_read")
	if !ok {
		t.Fatal("expected tension_attachable_read tool to be registered")
	}
	eventsListTool, ok := reg.Get("events_list_read")
	if !ok {
		t.Fatal("expected events_list_read tool to be registered")
	}
	eventsReplayTool, ok := reg.Get("events_replay_read")
	if !ok {
		t.Fatal("expected events_replay_read tool to be registered")
	}
	eventsEvaluateTool, ok := reg.Get("events_evaluate_read")
	if !ok {
		t.Fatal("expected events_evaluate_read tool to be registered")
	}

	writeOut, err := writeTool.Execute(context.Background(), json.RawMessage(`{
		"type":"lesson",
		"topic":"Transport reset",
		"content":"Persist delivery ids across reset to avoid duplicate wake handling.",
		"summary":"Delivery ids survive reset."
	}`))
	if err != nil {
		t.Fatalf("memory_write execute failed: %v", err)
	}
	if !strings.Contains(writeOut, `"status":"saved"`) {
		t.Fatalf("expected saved status from memory_write, got %s", writeOut)
	}

	searchOut, err := searchTool.Execute(context.Background(), json.RawMessage(`{
		"query":"duplicate wake handling"
	}`))
	if err != nil {
		t.Fatalf("memory_search execute failed: %v", err)
	}
	if !strings.Contains(searchOut, "Transport reset") {
		t.Fatalf("expected canonical memory search result, got %s", searchOut)
	}

	kernelOut, err := kernelTool.Execute(context.Background(), json.RawMessage(`{
		"task_id":"task-memory-packet-shell"
	}`))
	if err != nil {
		t.Fatalf("memory_packet_kernel execute failed: %v", err)
	}
	if !strings.Contains(kernelOut, `"packet_kind":"KERNEL"`) {
		t.Fatalf("expected kernel packet output, got %s", kernelOut)
	}

	packetOut, err := packetTool.Execute(context.Background(), json.RawMessage(`{
		"task_id":"task-memory-packet-shell"
	}`))
	if err != nil {
		t.Fatalf("memory_packet_shell execute failed: %v", err)
	}
	if !strings.Contains(packetOut, `"packet_kind":"SHELL"`) {
		t.Fatalf("expected shell packet output, got %s", packetOut)
	}

	promotionOut, err := promotionTool.Execute(context.Background(), json.RawMessage(`{
		"state":"PENDING"
	}`))
	if err != nil {
		t.Fatalf("memory_promotion_read execute failed: %v", err)
	}
	if !strings.Contains(promotionOut, `"count":1`) || !strings.Contains(promotionOut, `"promotion_id":"promotion-brain-1"`) || !strings.Contains(promotionOut, `"review_action":"REVIEW"`) {
		t.Fatalf("expected promotion read output, got %s", promotionOut)
	}

	coherenceOut, err := coherenceTool.Execute(context.Background(), json.RawMessage(`{
		"agent_id":"brain-1"
	}`))
	if err != nil {
		t.Fatalf("memory_coherence_read execute failed: %v", err)
	}
	if !strings.Contains(coherenceOut, `"coherence_band_hint":"DEGRADED"`) || !strings.Contains(coherenceOut, `"needs_attention":true`) {
		t.Fatalf("expected coherence read output, got %s", coherenceOut)
	}

	rspStateOut, err := rspStateTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("rsp_state_read execute failed: %v", err)
	}
	if !strings.Contains(rspStateOut, `"hidden_state":"FOCUSED"`) ||
		!strings.Contains(rspStateOut, `"state_rationale":"Focused and grounded on the current task locus."`) ||
		!strings.Contains(rspStateOut, `"governed_hint_summary":{"total_hints":1`) {
		t.Fatalf("expected rsp state read output, got %s", rspStateOut)
	}

	rspForecastOut, err := rspForecastTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("rsp_forecast_read execute failed: %v", err)
	}
	if !strings.Contains(rspForecastOut, `"forecast_readiness":"READY"`) ||
		!strings.Contains(rspForecastOut, `"forecast_coverage_summary":{"projection_count":1`) ||
		!strings.Contains(rspForecastOut, `"summary":"bounded forecast report"`) {
		t.Fatalf("expected rsp forecast read output, got %s", rspForecastOut)
	}

	rspBeliefOut, err := rspBeliefTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("rsp_belief_read execute failed: %v", err)
	}
	if !strings.Contains(rspBeliefOut, `"low_independence_count":1`) ||
		!strings.Contains(rspBeliefOut, `"count":1`) ||
		!strings.Contains(rspBeliefOut, `"summary":"bounded belief report"`) {
		t.Fatalf("expected rsp belief read output, got %s", rspBeliefOut)
	}

	rspBeliefClaimOut, err := rspBeliefClaimTool.Execute(context.Background(), json.RawMessage(`{"claim_id":"claim-brain-1"}`))
	if err != nil {
		t.Fatalf("rsp_belief_claim_read execute failed: %v", err)
	}
	if !strings.Contains(rspBeliefClaimOut, `"claim_id":"claim-brain-1"`) ||
		!strings.Contains(rspBeliefClaimOut, `"summary":"bounded belief claim"`) ||
		!strings.Contains(rspBeliefClaimOut, `"source_diversity":0.75`) {
		t.Fatalf("expected rsp belief claim read output, got %s", rspBeliefClaimOut)
	}

	rspTelemetryOut, err := rspTelemetryTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("rsp_telemetry_read execute failed: %v", err)
	}
	if !strings.Contains(rspTelemetryOut, `"readiness_band":"OBSERVABLE"`) ||
		!strings.Contains(rspTelemetryOut, `"observable_stream_count":3`) {
		t.Fatalf("expected rsp telemetry read output, got %s", rspTelemetryOut)
	}

	unifiedControlOut, err := unifiedControlTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unified_control_read execute failed: %v", err)
	}
	if !strings.Contains(unifiedControlOut, `"advisory_only":true`) ||
		!strings.Contains(unifiedControlOut, `"applied_entry_count":1`) ||
		!strings.Contains(unifiedControlOut, `"summary":"bounded unified control report"`) {
		t.Fatalf("expected unified control read output, got %s", unifiedControlOut)
	}

	controlReportOut, err := controlReportTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("control_report_read execute failed: %v", err)
	}
	if !strings.Contains(controlReportOut, `"total_clusters":1`) ||
		!strings.Contains(controlReportOut, `"proto_cluster_id":"proto-cluster-brain-1"`) ||
		!strings.Contains(controlReportOut, `"pressure_score":4`) {
		t.Fatalf("expected control report read output, got %s", controlReportOut)
	}

	controlClusterOut, err := controlClusterTool.Execute(context.Background(), json.RawMessage(`{"proto_cluster_id":"proto-cluster-brain-1"}`))
	if err != nil {
		t.Fatalf("control_cluster_read execute failed: %v", err)
	}
	if !strings.Contains(controlClusterOut, `"proto_cluster_id":"proto-cluster-brain-1"`) ||
		!strings.Contains(controlClusterOut, `"tension_id":"tension-brain-1"`) ||
		!strings.Contains(controlClusterOut, `"summary":"bounded control cluster detail"`) {
		t.Fatalf("expected control cluster read output, got %s", controlClusterOut)
	}

	controlStateOut, err := controlStateTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("control_state_read execute failed: %v", err)
	}
	if !strings.Contains(controlStateOut, `"total_clusters":1`) ||
		!strings.Contains(controlStateOut, `"proto_cluster_id":"proto-cluster-brain-1"`) ||
		!strings.Contains(controlStateOut, `"stabilized_mode_hint":"COHERENCE"`) {
		t.Fatalf("expected control state read output, got %s", controlStateOut)
	}

	controlStateClusterOut, err := controlStateClusterTool.Execute(context.Background(), json.RawMessage(`{"proto_cluster_id":"proto-cluster-brain-1"}`))
	if err != nil {
		t.Fatalf("control_state_cluster_read execute failed: %v", err)
	}
	if !strings.Contains(controlStateClusterOut, `"proto_cluster_id":"proto-cluster-brain-1"`) ||
		!strings.Contains(controlStateClusterOut, `"tension_id":"tension-state-brain-1"`) ||
		!strings.Contains(controlStateClusterOut, `"stabilized_mode_hint":"COHERENCE"`) {
		t.Fatalf("expected control state cluster read output, got %s", controlStateClusterOut)
	}

	rspCapabilityOut, err := rspCapabilityTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("rsp_capability_read execute failed: %v", err)
	}
	if !strings.Contains(rspCapabilityOut, `"belief_live":true`) ||
		!strings.Contains(rspCapabilityOut, `"governed_hints_live":true`) {
		t.Fatalf("expected rsp capability read output, got %s", rspCapabilityOut)
	}

	compactionCandidatesOut, err := compactionCandidatesTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("compaction_candidates_read execute failed: %v", err)
	}
	if !strings.Contains(compactionCandidatesOut, `"count":1`) ||
		!strings.Contains(compactionCandidatesOut, `"session_id":"sess-compaction-brain-1"`) ||
		!strings.Contains(compactionCandidatesOut, `"active_only":true`) {
		t.Fatalf("expected compaction candidates read output, got %s", compactionCandidatesOut)
	}

	compactionSnapshotsOut, err := compactionSnapshotsTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("compaction_snapshots_read execute failed: %v", err)
	}
	if !strings.Contains(compactionSnapshotsOut, `"count":1`) ||
		!strings.Contains(compactionSnapshotsOut, `"snapshot_id":"compaction-brain-snap-1"`) ||
		!strings.Contains(compactionSnapshotsOut, `"summary_text":"bounded compaction snapshot"`) {
		t.Fatalf("expected compaction snapshots read output, got %s", compactionSnapshotsOut)
	}

	invalidationListOut, err := invalidationListTool.Execute(context.Background(), json.RawMessage(`{"session_id":"sess-compaction-brain-1"}`))
	if err != nil {
		t.Fatalf("memory_invalidation_read execute failed: %v", err)
	}
	if !strings.Contains(invalidationListOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(invalidationListOut, `"count":1`) ||
		!strings.Contains(invalidationListOut, `"invalidation_id":"meminv-brain-1"`) {
		t.Fatalf("expected memory invalidation list output, got %s", invalidationListOut)
	}

	invalidationItemOut, err := invalidationItemTool.Execute(context.Background(), json.RawMessage(`{"invalidation_id":"meminv-brain-1"}`))
	if err != nil {
		t.Fatalf("memory_invalidation_item_read execute failed: %v", err)
	}
	if !strings.Contains(invalidationItemOut, `"agent_id":"brain-1"`) ||
		!strings.Contains(invalidationItemOut, `"invalidation_id":"meminv-brain-1"`) ||
		!strings.Contains(invalidationItemOut, `"reason":"VERSION_CHANGED"`) {
		t.Fatalf("expected memory invalidation item output, got %s", invalidationItemOut)
	}

	invalidationCursorOut, err := invalidationCursorTool.Execute(context.Background(), json.RawMessage(`{"session_id":"sess-compaction-brain-1"}`))
	if err != nil {
		t.Fatalf("memory_invalidation_cursor_read execute failed: %v", err)
	}
	if !strings.Contains(invalidationCursorOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(invalidationCursorOut, `"last_delivered_invalidation_id":"meminv-brain-1"`) ||
		!strings.Contains(invalidationCursorOut, `"last_poll_count":1`) {
		t.Fatalf("expected memory invalidation cursor output, got %s", invalidationCursorOut)
	}

	graphListOut, err := graphListTool.Execute(context.Background(), json.RawMessage(`{"task_id":"task-memory-packet-shell"}`))
	if err != nil {
		t.Fatalf("memory_graph_list_read execute failed: %v", err)
	}
	if !strings.Contains(graphListOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(graphListOut, `"count":1`) ||
		!strings.Contains(graphListOut, `"memory_id":"memnode:workspace_memory:memory:brain-1"`) {
		t.Fatalf("expected memory graph list output, got %s", graphListOut)
	}

	graphGetOut, err := graphGetTool.Execute(context.Background(), json.RawMessage(`{"memory_id":"memnode:workspace_memory:memory:brain-1"}`))
	if err != nil {
		t.Fatalf("memory_graph_get_read execute failed: %v", err)
	}
	if !strings.Contains(graphGetOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(graphGetOut, `"memory_id":"memnode:workspace_memory:memory:brain-1"`) ||
		!strings.Contains(graphGetOut, `"semantic_lineage_id":"workspace_memory:memory:brain-1"`) {
		t.Fatalf("expected memory graph get output, got %s", graphGetOut)
	}

	nodeSearchOut, err := nodeSearchTool.Execute(context.Background(), json.RawMessage(`{"query":"graph node"}`))
	if err != nil {
		t.Fatalf("memory_node_search_read execute failed: %v", err)
	}
	if !strings.Contains(nodeSearchOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(nodeSearchOut, `"count":1`) ||
		!strings.Contains(nodeSearchOut, `"memory_id":"memnode:workspace_memory:memory:brain-1"`) {
		t.Fatalf("expected memory node search output, got %s", nodeSearchOut)
	}

	tensionListOut, err := tensionListTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("tension_list_read execute failed: %v", err)
	}
	if !strings.Contains(tensionListOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(tensionListOut, `"count":1`) ||
		!strings.Contains(tensionListOut, `"tension_id":"tension-brain-1"`) {
		t.Fatalf("expected tension list output, got %s", tensionListOut)
	}

	tensionGetOut, err := tensionGetTool.Execute(context.Background(), json.RawMessage(`{"tension_id":"tension-brain-1"}`))
	if err != nil {
		t.Fatalf("tension_get_read execute failed: %v", err)
	}
	if !strings.Contains(tensionGetOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(tensionGetOut, `"tension_id":"tension-brain-1"`) ||
		!strings.Contains(tensionGetOut, `"dependency_type":"BLOCKS"`) {
		t.Fatalf("expected tension get output, got %s", tensionGetOut)
	}

	tensionFrontierOut, err := tensionFrontierTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("tension_frontier_read execute failed: %v", err)
	}
	if !strings.Contains(tensionFrontierOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(tensionFrontierOut, `"count":1`) ||
		!strings.Contains(tensionFrontierOut, `"surfaced_priority":0.9`) {
		t.Fatalf("expected tension frontier output, got %s", tensionFrontierOut)
	}

	tensionAttachableOut, err := tensionAttachableTool.Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("tension_attachable_read execute failed: %v", err)
	}
	if !strings.Contains(tensionAttachableOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(tensionAttachableOut, `"agent_id":"brain-1"`) ||
		!strings.Contains(tensionAttachableOut, `"attach_prob":0.73`) {
		t.Fatalf("expected tension attachable output, got %s", tensionAttachableOut)
	}

	eventsListOut, err := eventsListTool.Execute(context.Background(), json.RawMessage(`{"session_id":"sess-compaction-brain-1"}`))
	if err != nil {
		t.Fatalf("events_list_read execute failed: %v", err)
	}
	if !strings.Contains(eventsListOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(eventsListOut, `"count":1`) ||
		!strings.Contains(eventsListOut, `"event_id":"rtev-replay-brain-1"`) {
		t.Fatalf("expected events list read output, got %s", eventsListOut)
	}

	eventsReplayOut, err := eventsReplayTool.Execute(context.Background(), json.RawMessage(`{"session_id":"sess-compaction-brain-1","include_events":true}`))
	if err != nil {
		t.Fatalf("events_replay_read execute failed: %v", err)
	}
	if !strings.Contains(eventsReplayOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(eventsReplayOut, `"event_id":"rtev-replay-brain-1"`) ||
		!strings.Contains(eventsReplayOut, `"verdict":"PASS"`) {
		t.Fatalf("expected events replay read output, got %s", eventsReplayOut)
	}

	eventsEvaluateOut, err := eventsEvaluateTool.Execute(context.Background(), json.RawMessage(`{"session_id":"sess-compaction-brain-1"}`))
	if err != nil {
		t.Fatalf("events_evaluate_read execute failed: %v", err)
	}
	if !strings.Contains(eventsEvaluateOut, `"workspace_id":"ws-1"`) ||
		!strings.Contains(eventsEvaluateOut, `"sessions":1`) ||
		!strings.Contains(eventsEvaluateOut, `"verdict":"PASS"`) {
		t.Fatalf("expected events evaluate read output, got %s", eventsEvaluateOut)
	}
}
