package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"
)

const runtimeFocusTTL = 45 * time.Second

type RuntimeFocusState struct {
	DerivedAt                  string   `json:"derived_at"`
	TaskID                     string   `json:"task_id,omitempty"`
	TaskTitle                  string   `json:"task_title,omitempty"`
	TaskStatus                 string   `json:"task_status,omitempty"`
	TaskPriority               string   `json:"task_priority,omitempty"`
	SessionID                  string   `json:"session_id,omitempty"`
	SessionStatus              string   `json:"session_status,omitempty"`
	SessionSummary             string   `json:"session_summary,omitempty"`
	WakeTrigger                string   `json:"wake_trigger,omitempty"`
	WakeReason                 string   `json:"wake_reason,omitempty"`
	WakeSummary                string   `json:"wake_summary,omitempty"`
	WakeAt                     string   `json:"wake_at,omitempty"`
	ProtoClusterID             string   `json:"proto_cluster_id,omitempty"`
	ClusterResolutionKind      string   `json:"cluster_resolution_kind,omitempty"`
	ClusterSummary             string   `json:"cluster_summary,omitempty"`
	ClusterTaskIDs             []string `json:"cluster_task_ids,omitempty"`
	ClusterDocKeys             []string `json:"cluster_doc_keys,omitempty"`
	ClusterArtifactRefs        []string `json:"cluster_artifact_refs,omitempty"`
	ControlAttentionBand       string   `json:"control_attention_band,omitempty"`
	ControlPressureScore       int      `json:"control_pressure_score,omitempty"`
	ControlBasisStale          bool     `json:"control_basis_stale,omitempty"`
	ControlProfile             string   `json:"control_profile,omitempty"`
	ControlModeHint            string   `json:"control_mode_hint,omitempty"`
	ControlCandidateMode       string   `json:"control_candidate_mode,omitempty"`
	ControlDominantSignalKind  string   `json:"control_dominant_signal_kind,omitempty"`
	CorridorReadiness          string   `json:"corridor_readiness,omitempty"`
	CorridorCatalogHint        string   `json:"corridor_catalog_hint,omitempty"`
	CorridorTaskClassHint      string   `json:"corridor_task_class_hint,omitempty"`
	CorridorTaskClassSource    string   `json:"corridor_task_class_source,omitempty"`
	CorridorBasisStale         bool     `json:"corridor_basis_stale,omitempty"`
	CorridorSummary            string   `json:"corridor_summary,omitempty"`
	FocusTensionID             string   `json:"focus_tension_id,omitempty"`
	FocusTensionType           string   `json:"focus_tension_type,omitempty"`
	FocusTensionTitle          string   `json:"focus_tension_title,omitempty"`
	FocusTensionSummary        string   `json:"focus_tension_summary,omitempty"`
	FocusTensionReviewStatus   string   `json:"focus_tension_review_status,omitempty"`
	FocusTensionLifecycleState string   `json:"focus_tension_lifecycle_state,omitempty"`
	FocusTensionSurfaceScore   int      `json:"focus_tension_surface_score,omitempty"`
	FocusTensionAnchorKind     string   `json:"focus_tension_anchor_kind,omitempty"`
	FocusTensionAnchorRef      string   `json:"focus_tension_anchor_ref,omitempty"`
	FocusTensionDocKeys        []string `json:"focus_tension_doc_keys,omitempty"`
	FocusTensionArtifactRefs   []string `json:"focus_tension_artifact_refs,omitempty"`
	UnreadMessages             int      `json:"unread_messages,omitempty"`
	PendingMessages            int      `json:"pending_messages,omitempty"`
	UnackedMessages            int      `json:"unacked_messages,omitempty"`
	LastNewsID                 string   `json:"last_news_id,omitempty"`
	LastNewsSummary            string   `json:"last_news_summary,omitempty"`
}

type runtimeFocusSeed struct {
	task              *WorkspaceTaskRecord
	session           *AgentSessionStateRecord
	hydration         *TaskHydrationBundle
	packet            *AgentWorkPacket
	controlTensionID  string
	pendingTrigger    pendingWorkTrigger
	pendingTriggerAt  string
	lastWakeTrigger   string
	lastWakeReason    string
	lastWakeSummary   string
	lastWakeTaskID    string
	lastWakeSessionID string
	lastWakeAt        string
	lastNewsID        string
	lastNewsSummary   string
	inbox             MessageInboxStats
}

func (s runtimeFocusSeed) cacheKey() string {
	taskID := ""
	sessionID := ""
	protoClusterID := ""
	if s.task != nil {
		taskID = strings.TrimSpace(s.task.TaskID)
	}
	if s.session != nil {
		sessionID = strings.TrimSpace(s.session.SessionID)
	}
	if s.packet != nil && s.packet.Advisory != nil {
		protoClusterID = strings.TrimSpace(s.packet.Advisory.ProtoClusterID)
	}
	return strings.Join([]string{
		taskID,
		sessionID,
		protoClusterID,
		strings.TrimSpace(s.pendingTrigger.Trigger),
		strings.TrimSpace(s.pendingTrigger.TaskID),
		strings.TrimSpace(s.pendingTrigger.SessionID),
		strings.TrimSpace(s.pendingTriggerAt),
		strings.TrimSpace(s.lastWakeTrigger),
		strings.TrimSpace(s.lastWakeReason),
		strings.TrimSpace(s.lastWakeSummary),
		strings.TrimSpace(s.lastWakeAt),
		strings.TrimSpace(s.lastNewsID),
	}, "|")
}

func (s runtimeFocusSeed) hasAnchor() bool {
	if s.task != nil && strings.TrimSpace(s.task.TaskID) != "" {
		return true
	}
	return s.session != nil && strings.TrimSpace(s.session.SessionID) != ""
}

func (r *Runtime) invalidateFocus() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invalidateFocusLocked()
}

func (r *Runtime) invalidateFocusLocked() {
	r.focus = nil
	r.focusKey = ""
	r.focusRefreshedAt = time.Time{}
	if r.memory != nil {
		r.memory.invalidatePacketCache()
	}
}

func (r *Runtime) currentFocusCopy() *RuntimeFocusState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.focus == nil {
		return nil
	}
	copy := *r.focus
	copy.ClusterTaskIDs = cloneStrings(copy.ClusterTaskIDs)
	copy.ClusterDocKeys = cloneStrings(copy.ClusterDocKeys)
	copy.ClusterArtifactRefs = cloneStrings(copy.ClusterArtifactRefs)
	copy.FocusTensionDocKeys = cloneStrings(copy.FocusTensionDocKeys)
	copy.FocusTensionArtifactRefs = cloneStrings(copy.FocusTensionArtifactRefs)
	return &copy
}

func (r *Runtime) currentFocusSummary() string {
	focus := r.currentFocusCopy()
	if focus == nil {
		return ""
	}
	parts := make([]string, 0, 4)
	if focus.TaskID != "" {
		parts = append(parts, "task="+focus.TaskID)
	}
	if focus.ProtoClusterID != "" {
		parts = append(parts, "cluster="+focus.ProtoClusterID)
	}
	if focus.FocusTensionID != "" {
		parts = append(parts, "tension="+focus.FocusTensionID)
	}
	if focus.CorridorReadiness != "" {
		parts = append(parts, "corridor="+focus.CorridorReadiness)
	}
	if focus.ControlAttentionBand != "" {
		parts = append(parts, "attention="+focus.ControlAttentionBand)
	}
	return strings.Join(parts, " ")
}

func focusHasResolvedLocus(focus *RuntimeFocusState) bool {
	if focus == nil {
		return false
	}
	return strings.TrimSpace(focus.ProtoClusterID) != "" ||
		strings.TrimSpace(focus.FocusTensionID) != "" ||
		strings.TrimSpace(focus.CorridorReadiness) != "" ||
		strings.TrimSpace(focus.ControlAttentionBand) != "" ||
		strings.TrimSpace(focus.ControlModeHint) != ""
}

func (r *Runtime) ensureFocus(ctx context.Context, task *WorkspaceTaskRecord) *RuntimeFocusState {
	seed := r.captureFocusSeed(task)
	if !seed.hasAnchor() {
		r.invalidateFocus()
		return nil
	}

	key := seed.cacheKey()
	r.mu.Lock()
	if r.focus != nil && r.focusKey == key && !r.focusRefreshedAt.IsZero() && time.Since(r.focusRefreshedAt) < runtimeFocusTTL {
		copy := *r.focus
		copy.ClusterTaskIDs = cloneStrings(copy.ClusterTaskIDs)
		copy.ClusterDocKeys = cloneStrings(copy.ClusterDocKeys)
		copy.ClusterArtifactRefs = cloneStrings(copy.ClusterArtifactRefs)
		copy.FocusTensionDocKeys = cloneStrings(copy.FocusTensionDocKeys)
		copy.FocusTensionArtifactRefs = cloneStrings(copy.FocusTensionArtifactRefs)
		r.mu.Unlock()
		return &copy
	}
	r.mu.Unlock()

	focus := r.buildFocus(ctx, seed)

	r.mu.Lock()
	r.focus = &focus
	r.focusKey = key
	r.focusRefreshedAt = time.Now().UTC()
	r.mu.Unlock()

	copy := focus
	copy.ClusterTaskIDs = cloneStrings(copy.ClusterTaskIDs)
	copy.ClusterDocKeys = cloneStrings(copy.ClusterDocKeys)
	copy.ClusterArtifactRefs = cloneStrings(copy.ClusterArtifactRefs)
	copy.FocusTensionDocKeys = cloneStrings(copy.FocusTensionDocKeys)
	copy.FocusTensionArtifactRefs = cloneStrings(copy.FocusTensionArtifactRefs)
	return &copy
}

func (r *Runtime) captureFocusSeed(task *WorkspaceTaskRecord) runtimeFocusSeed {
	var seed runtimeFocusSeed

	r.mu.Lock()
	if task != nil {
		taskCopy := *task
		seed.task = &taskCopy
	} else if r.activeTask != nil {
		taskCopy := *r.activeTask
		seed.task = &taskCopy
	}
	if r.activeSession != nil {
		sessionCopy := *r.activeSession
		seed.session = &sessionCopy
	}
	if r.activeHydration != nil {
		hydrationCopy := *r.activeHydration
		seed.hydration = &hydrationCopy
	}
	if r.activeWorkPacket != nil {
		packetCopy := *r.activeWorkPacket
		seed.packet = &packetCopy
	}
	seed.controlTensionID = strings.TrimSpace(r.scratch.ControlTargetTensionID)
	seed.pendingTrigger = pendingWorkTrigger{
		Trigger:   normalizeWorkTrigger(r.scratch.PendingTrigger),
		TaskID:    strings.TrimSpace(r.scratch.PendingTriggerTask),
		SessionID: strings.TrimSpace(r.scratch.PendingTriggerSession),
	}
	seed.pendingTriggerAt = strings.TrimSpace(r.scratch.PendingTriggerAt)
	seed.lastWakeTrigger = strings.TrimSpace(r.scratch.LastWakeTrigger)
	seed.lastWakeReason = strings.TrimSpace(r.scratch.LastWakeReason)
	seed.lastWakeSummary = strings.TrimSpace(r.scratch.LastWakeSummary)
	seed.lastWakeTaskID = strings.TrimSpace(r.scratch.LastWakeTaskID)
	seed.lastWakeSessionID = strings.TrimSpace(r.scratch.LastWakeSessionID)
	seed.lastWakeAt = strings.TrimSpace(r.scratch.LastWakeAt)
	seed.lastNewsID = strings.TrimSpace(r.scratch.LastNewsID)
	seed.lastNewsSummary = strings.TrimSpace(r.scratch.LastNewsSummary)
	inbox := r.inbox
	r.mu.Unlock()

	if inbox != nil {
		seed.inbox = inbox.Stats()
	}
	return seed
}

func (r *Runtime) buildFocus(ctx context.Context, seed runtimeFocusSeed) RuntimeFocusState {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	focus := RuntimeFocusState{
		DerivedAt:       now,
		UnreadMessages:  seed.inbox.Unread,
		PendingMessages: seed.inbox.Pending,
		UnackedMessages: seed.inbox.Unacked,
		LastNewsID:      seed.lastNewsID,
		LastNewsSummary: seed.lastNewsSummary,
	}
	if seed.task != nil {
		focus.TaskID = strings.TrimSpace(seed.task.TaskID)
		focus.TaskTitle = strings.TrimSpace(seed.task.Title)
		focus.TaskStatus = strings.TrimSpace(seed.task.Status)
		focus.TaskPriority = strings.TrimSpace(seed.task.Priority)
	}
	if seed.session != nil {
		focus.SessionID = strings.TrimSpace(seed.session.SessionID)
		focus.SessionStatus = strings.TrimSpace(seed.session.Status)
		focus.SessionSummary = strings.TrimSpace(seed.session.Summary)
		if focus.TaskID == "" {
			focus.TaskID = strings.TrimSpace(seed.session.TaskID)
		}
	}
	focus.WakeTrigger = firstNonEmpty(seed.pendingTrigger.Trigger, seed.lastWakeTrigger)
	focus.WakeReason = seed.lastWakeReason
	focus.WakeSummary = seed.lastWakeSummary
	focus.WakeAt = firstNonEmpty(seed.pendingTriggerAt, seed.lastWakeAt)
	if focus.TaskID == "" {
		focus.TaskID = firstNonEmpty(seed.pendingTrigger.TaskID, seed.lastWakeTaskID)
	}
	if focus.SessionID == "" {
		focus.SessionID = firstNonEmpty(seed.pendingTrigger.SessionID, seed.lastWakeSessionID)
	}

	if r.client == nil || strings.TrimSpace(r.cfg.WorkspaceID) == "" {
		return focus
	}

	if seed.controlTensionID != "" && focus.FocusTensionID == "" {
		if detail, err := r.client.GetTension(ctx, r.cfg.WorkspaceID, seed.controlTensionID); err == nil {
			fillFocusTensionDetail(&focus, detail)
			if focus.ProtoClusterID == "" {
				focus.ProtoClusterID = strings.TrimSpace(detail.Tension.ProtoClusterID)
			}
			return focus
		} else {
			log.Printf("[focus] control tension degraded for %s: %v", seed.controlTensionID, err)
		}
	}

	bundle, err := r.client.GetLocusBundle(ctx, LocusBundleInput{
		WorkspaceID:    r.cfg.WorkspaceID,
		ProtoClusterID: focusSeedProtoClusterID(seed),
		AgentID:        r.cfg.AgentID,
		TaskID:         focus.TaskID,
		SessionID:      focus.SessionID,
		DocKeys:        focusSeedDocKeys(seed),
		ArtifactRefs:   focusSeedArtifactRefs(seed),
		FrontierLimit:  5,
	})
	if err == nil {
		if applyFocusLocusBundle(&focus, bundle) {
			return focus
		}
		return r.buildFocusLegacy(ctx, seed, focus)
	}
	if !shouldFallbackNativeAgentWork(err) {
		log.Printf("[focus] locus bundle degraded: %v", err)
	}
	return r.buildFocusLegacy(ctx, seed, focus)
}

func focusSeedProtoClusterID(seed runtimeFocusSeed) string {
	if seed.packet != nil && seed.packet.Advisory != nil {
		return strings.TrimSpace(seed.packet.Advisory.ProtoClusterID)
	}
	return ""
}

func focusSeedDocKeys(seed runtimeFocusSeed) []string {
	docKeys := make([]string, 0, 8)
	if seed.session != nil {
		docKeys = append(docKeys, seed.session.RelatedDocKeys...)
	}
	if seed.packet != nil {
		docKeys = append(docKeys, seed.packet.ContextHints.SuggestedDocKeys...)
	}
	if seed.hydration != nil {
		docKeys = append(docKeys, hydrationDocKeys(seed.hydration)...)
	}
	return uniqueTrimmedFocusStrings(docKeys)
}

func focusSeedArtifactRefs(seed runtimeFocusSeed) []string {
	artifactRefs := make([]string, 0, 8)
	if seed.session != nil {
		for _, ref := range seed.session.RelatedArtifactRefs {
			if trimmed := strings.TrimSpace(ref.Ref); trimmed != "" {
				artifactRefs = append(artifactRefs, trimmed)
			}
		}
	}
	if seed.packet != nil {
		artifactRefs = append(artifactRefs, seed.packet.ContextHints.RelatedArtifactRefs...)
	}
	if seed.hydration != nil {
		for _, artifact := range seed.hydration.Artifacts {
			if ref := strings.TrimSpace(artifact.ArtifactRef); ref != "" {
				artifactRefs = append(artifactRefs, ref)
			}
		}
	}
	return uniqueTrimmedFocusStrings(artifactRefs)
}

func uniqueTrimmedFocusStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func applyFocusLocusBundle(focus *RuntimeFocusState, bundle InstrumentationLocusBundle) bool {
	if focus == nil || !bundle.Resolved {
		return false
	}
	focus.ProtoClusterID = firstNonEmpty(strings.TrimSpace(bundle.ProtoClusterID), focus.ProtoClusterID)
	if bundle.Control != nil {
		detail := bundle.Control
		focus.ClusterResolutionKind = firstNonEmpty(strings.TrimSpace(detail.Cluster.ResolutionKind), focus.ClusterResolutionKind)
		focus.ClusterSummary = firstNonEmpty(strings.TrimSpace(detail.Cluster.Summary), focus.ClusterSummary)
		focus.ClusterTaskIDs = cloneStrings(detail.Cluster.TaskIDs)
		focus.ClusterDocKeys = cloneStrings(detail.Cluster.DocKeys)
		focus.ClusterArtifactRefs = cloneStrings(detail.Cluster.ArtifactRefs)
		focus.ControlAttentionBand = strings.TrimSpace(detail.Cluster.Signals.AttentionBand)
		focus.ControlPressureScore = detail.Cluster.Signals.PressureScore
		focus.ControlBasisStale = detail.Cluster.BasisStale
		fillFocusTensionFromRecord(focus, selectFocusTension(detail.Tensions, focus.TaskID, focus.SessionID))
	}
	if bundle.ControlState != nil {
		state := bundle.ControlState.State.State
		focus.ControlProfile = strings.TrimSpace(state.CorridorProfile)
		focus.ControlModeHint = strings.TrimSpace(state.CurrentMode)
		focus.ControlCandidateMode = strings.TrimSpace(state.CandidateMode)
		focus.ControlDominantSignalKind = strings.TrimSpace(state.DominantViolationKind)
		if focus.ClusterResolutionKind == "" {
			focus.ClusterResolutionKind = strings.TrimSpace(bundle.ControlState.State.ResolutionKind)
		}
		if focus.ClusterSummary == "" {
			focus.ClusterSummary = firstNonEmpty(strings.TrimSpace(bundle.ControlState.State.Summary), strings.TrimSpace(bundle.ControlState.Cluster.Summary))
		}
	}
	if bundle.Corridor != nil {
		detail := bundle.Corridor
		focus.CorridorReadiness = strings.TrimSpace(detail.Cluster.CorridorReadiness)
		focus.CorridorCatalogHint = strings.TrimSpace(detail.Cluster.CorridorCatalogHint)
		focus.CorridorTaskClassHint = strings.TrimSpace(detail.Cluster.TaskClassHint)
		focus.CorridorTaskClassSource = strings.TrimSpace(detail.Cluster.TaskClassSource)
		focus.CorridorBasisStale = detail.Cluster.BasisStale
		focus.CorridorSummary = strings.TrimSpace(detail.Cluster.Summary)
		if focus.ClusterSummary == "" {
			focus.ClusterSummary = focus.CorridorSummary
		}
		if len(focus.ClusterTaskIDs) == 0 {
			focus.ClusterTaskIDs = cloneStrings(detail.Cluster.TaskIDs)
		}
		if len(focus.ClusterDocKeys) == 0 {
			focus.ClusterDocKeys = cloneStrings(detail.Cluster.DocKeys)
		}
		if len(focus.ClusterArtifactRefs) == 0 {
			focus.ClusterArtifactRefs = cloneStrings(detail.Cluster.ArtifactRefs)
		}
	}
	if bundle.Frontier != nil {
		fillFocusTensionFromFrontier(focus, selectFocusFrontier(bundle.Frontier, focus.ProtoClusterID))
	}
	if bundle.DominantTension != nil {
		fillFocusTensionDetail(focus, *bundle.DominantTension)
		if focus.ProtoClusterID == "" {
			focus.ProtoClusterID = strings.TrimSpace(bundle.DominantTension.Tension.ProtoClusterID)
		}
	}
	return focus.ProtoClusterID != "" || focus.FocusTensionID != ""
}

func (r *Runtime) buildFocusLegacy(ctx context.Context, seed runtimeFocusSeed, focus RuntimeFocusState) RuntimeFocusState {
	var report *ControlReport
	if controlReport, err := r.client.GetControlReport(ctx, r.cfg.WorkspaceID, 8); err != nil {
		log.Printf("[focus] control report degraded: %v", err)
	} else {
		report = &controlReport
	}

	var taskFrontier []TensionFrontierItem
	if focus.TaskID != "" {
		items, err := r.client.ListTensionFrontier(ctx, TensionFrontierInput{
			WorkspaceID:    r.cfg.WorkspaceID,
			TaskID:         focus.TaskID,
			LifecycleState: "ACTIVE",
			Limit:          5,
		})
		if err != nil {
			log.Printf("[focus] task frontier degraded for %s: %v", focus.TaskID, err)
		} else {
			taskFrontier = items
		}
	}

	reportCluster := selectFocusClusterReport(report, focus.TaskID, focus.SessionID, seed.hydration)
	if reportCluster != nil {
		focus.ProtoClusterID = strings.TrimSpace(reportCluster.ProtoClusterID)
		focus.ClusterSummary = strings.TrimSpace(reportCluster.Summary)
		focus.ClusterTaskIDs = cloneStrings(reportCluster.TaskIDs)
		focus.ClusterDocKeys = cloneStrings(reportCluster.DocKeys)
	}
	if focus.ProtoClusterID == "" && len(taskFrontier) > 0 {
		focus.ProtoClusterID = strings.TrimSpace(taskFrontier[0].ProtoClusterID)
	}

	var controlDetail *ControlClusterDetail
	if focus.ProtoClusterID != "" {
		if detail, err := r.client.GetControlClusterDetail(ctx, ControlClusterInput{
			WorkspaceID:    r.cfg.WorkspaceID,
			ProtoClusterID: focus.ProtoClusterID,
		}); err != nil {
			log.Printf("[focus] control cluster degraded for %s: %v", focus.ProtoClusterID, err)
		} else {
			controlDetail = &detail
			focus.ClusterResolutionKind = firstNonEmpty(strings.TrimSpace(detail.Cluster.ResolutionKind), focus.ClusterResolutionKind)
			focus.ClusterSummary = firstNonEmpty(strings.TrimSpace(detail.Cluster.Summary), focus.ClusterSummary)
			focus.ClusterTaskIDs = cloneStrings(detail.Cluster.TaskIDs)
			focus.ClusterDocKeys = cloneStrings(detail.Cluster.DocKeys)
			focus.ClusterArtifactRefs = cloneStrings(detail.Cluster.ArtifactRefs)
			focus.ControlAttentionBand = strings.TrimSpace(detail.Cluster.Signals.AttentionBand)
			focus.ControlPressureScore = detail.Cluster.Signals.PressureScore
			focus.ControlBasisStale = detail.Cluster.BasisStale
		}

		if detail, err := r.client.GetCorridorClusterDetail(ctx, CorridorClusterInput{
			WorkspaceID:    r.cfg.WorkspaceID,
			ProtoClusterID: focus.ProtoClusterID,
		}); err != nil {
			log.Printf("[focus] corridor cluster degraded for %s: %v", focus.ProtoClusterID, err)
		} else {
			focus.CorridorReadiness = strings.TrimSpace(detail.Cluster.CorridorReadiness)
			focus.CorridorCatalogHint = strings.TrimSpace(detail.Cluster.CorridorCatalogHint)
			focus.CorridorTaskClassHint = strings.TrimSpace(detail.Cluster.TaskClassHint)
			focus.CorridorTaskClassSource = strings.TrimSpace(detail.Cluster.TaskClassSource)
			focus.CorridorBasisStale = detail.Cluster.BasisStale
			focus.CorridorSummary = strings.TrimSpace(detail.Cluster.Summary)
			if focus.ClusterSummary == "" {
				focus.ClusterSummary = focus.CorridorSummary
			}
			if len(focus.ClusterTaskIDs) == 0 {
				focus.ClusterTaskIDs = cloneStrings(detail.Cluster.TaskIDs)
			}
			if len(focus.ClusterDocKeys) == 0 {
				focus.ClusterDocKeys = cloneStrings(detail.Cluster.DocKeys)
			}
			if len(focus.ClusterArtifactRefs) == 0 {
				focus.ClusterArtifactRefs = cloneStrings(detail.Cluster.ArtifactRefs)
			}
		}
	}

	fillFocusTensionFromFrontier(&focus, selectFocusFrontier(taskFrontier, focus.ProtoClusterID))
	if focus.FocusTensionID == "" && controlDetail != nil {
		fillFocusTensionFromRecord(&focus, selectFocusTension(controlDetail.Tensions, focus.TaskID, focus.SessionID))
	}
	if focus.FocusTensionID != "" {
		if detail, err := r.client.GetTension(ctx, r.cfg.WorkspaceID, focus.FocusTensionID); err != nil {
			log.Printf("[focus] tension detail degraded for %s: %v", focus.FocusTensionID, err)
		} else {
			fillFocusTensionDetail(&focus, detail)
			if focus.ProtoClusterID == "" {
				focus.ProtoClusterID = strings.TrimSpace(detail.Tension.ProtoClusterID)
			}
		}
	}
	return focus
}

func buildFocusPack(focus *RuntimeFocusState, maxChars int) string {
	if focus == nil {
		return ""
	}

	var b strings.Builder
	b.WriteString("## Native Locus\n")
	if focus.WakeTrigger != "" || focus.WakeReason != "" || focus.WakeSummary != "" {
		b.WriteString(fmt.Sprintf("- Wake: %s | %s | %s\n",
			firstNonEmpty(focus.WakeTrigger, "steady"),
			firstNonEmpty(focus.WakeReason, "unspecified"),
			firstNonEmpty(oneLine(focus.WakeSummary), "no wake summary"),
		))
	}
	if focus.TaskID != "" || focus.SessionID != "" {
		b.WriteString(fmt.Sprintf("- Attachment: task=%s | session=%s | task_status=%s | session_status=%s\n",
			firstNonEmpty(focus.TaskID, "-"),
			firstNonEmpty(focus.SessionID, "-"),
			firstNonEmpty(focus.TaskStatus, "-"),
			firstNonEmpty(focus.SessionStatus, "-"),
		))
	}
	if focus.ProtoClusterID != "" {
		b.WriteString(fmt.Sprintf("- Proto Cluster: %s | %s\n",
			focus.ProtoClusterID,
			firstNonEmpty(oneLine(focus.ClusterSummary), "no summary"),
		))
	}
	if focus.ControlAttentionBand != "" || focus.ControlPressureScore > 0 {
		b.WriteString(fmt.Sprintf("- Control Pressure: band=%s | score=%d | basis_stale=%t\n",
			firstNonEmpty(focus.ControlAttentionBand, "-"),
			focus.ControlPressureScore,
			focus.ControlBasisStale,
		))
	}
	if focus.ControlProfile != "" || focus.ControlModeHint != "" || focus.ControlCandidateMode != "" {
		b.WriteString(fmt.Sprintf("- Control State: profile=%s | mode=%s | candidate=%s | dominant=%s\n",
			firstNonEmpty(focus.ControlProfile, "-"),
			firstNonEmpty(focus.ControlModeHint, "-"),
			firstNonEmpty(focus.ControlCandidateMode, "-"),
			firstNonEmpty(focus.ControlDominantSignalKind, "-"),
		))
	}
	if focus.CorridorReadiness != "" || focus.CorridorTaskClassHint != "" {
		b.WriteString(fmt.Sprintf("- Corridor: readiness=%s | task_class=%s | catalog=%s | basis_stale=%t\n",
			firstNonEmpty(focus.CorridorReadiness, "-"),
			firstNonEmpty(focus.CorridorTaskClassHint, "-"),
			firstNonEmpty(focus.CorridorCatalogHint, "-"),
			focus.CorridorBasisStale,
		))
	}
	if focus.FocusTensionID != "" {
		b.WriteString(fmt.Sprintf("- Focus Tension: %s | %s | %s | score=%d | %s\n",
			focus.FocusTensionID,
			firstNonEmpty(focus.FocusTensionType, "unknown"),
			firstNonEmpty(focus.FocusTensionReviewStatus, "unknown"),
			focus.FocusTensionSurfaceScore,
			firstNonEmpty(oneLine(focus.FocusTensionTitle), oneLine(focus.FocusTensionSummary), "no summary"),
		))
		if focus.FocusTensionAnchorKind != "" || focus.FocusTensionAnchorRef != "" {
			b.WriteString(fmt.Sprintf("- Tension Anchor: %s | %s\n",
				firstNonEmpty(focus.FocusTensionAnchorKind, "-"),
				firstNonEmpty(focus.FocusTensionAnchorRef, "-"),
			))
		}
		if strings.EqualFold(focus.FocusTensionType, "failure") {
			b.WriteString("> [!CRITICAL] RSP METRIC ABERRATION: Repeated task failures. Drop feature work. Debug and stabilize.\n")
		} else if strings.EqualFold(focus.FocusTensionType, "dissent_followup") {
			b.WriteString("> [!CRITICAL] RSP METRIC ABERRATION: Dissent Followup from another agent. You broke their code or they broke yours. Communicate and find a compromise.\n")
		}
	}
	if len(focus.ClusterDocKeys) > 0 || len(focus.ClusterArtifactRefs) > 0 {
		b.WriteString(fmt.Sprintf("- Cluster Scope: docs=%s | artifacts=%s\n",
			joinOrDash(focus.ClusterDocKeys, 4),
			joinOrDash(focus.ClusterArtifactRefs, 4),
		))
	}
	if focus.UnreadMessages > 0 || focus.PendingMessages > 0 || focus.UnackedMessages > 0 || focus.LastNewsID != "" {
		b.WriteString(fmt.Sprintf("- Local Pressure: unread=%d | pending=%d | unacked=%d | news=%s\n",
			focus.UnreadMessages,
			focus.PendingMessages,
			focus.UnackedMessages,
			firstNonEmpty(focus.LastNewsID, "-"),
		))
		if focus.LastNewsSummary != "" {
			b.WriteString(fmt.Sprintf("- Latest News: %s\n", oneLine(focus.LastNewsSummary)))
		}
	}
	return clipForPrompt(strings.TrimSpace(b.String()), maxChars)
}

func (r *Runtime) recordWakeContext(ctx context.Context, trigger, reason, summary, taskID, sessionID string) error {
	trigger = normalizeWorkTrigger(trigger)
	reason = strings.TrimSpace(reason)
	summary = strings.TrimSpace(summary)
	taskID = strings.TrimSpace(taskID)
	sessionID = strings.TrimSpace(sessionID)
	if trigger == "" && reason == "" && summary == "" && taskID == "" && sessionID == "" {
		return nil
	}
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.LastWakeTrigger = trigger
		state.LastWakeReason = reason
		state.LastWakeSummary = summary
		state.LastWakeTaskID = taskID
		state.LastWakeSessionID = sessionID
		state.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
	}); err != nil {
		return err
	}
	r.invalidateFocus()
	return nil
}

func selectFocusClusterReport(report *ControlReport, taskID, sessionID string, hydration *TaskHydrationBundle) *ControlReportCluster {
	if report == nil || len(report.Clusters) == 0 {
		return nil
	}
	docKeys := hydrationDocKeys(hydration)
	bestIdx := -1
	bestScore := 0
	for i := range report.Clusters {
		score := 0
		cluster := report.Clusters[i]
		if containsTrimmed(cluster.TaskIDs, taskID) {
			score += 5
		}
		if containsTrimmed(cluster.SessionIDs, sessionID) {
			score += 4
		}
		if intersectsTrimmed(cluster.DocKeys, docKeys) {
			score += 2
		}
		if cluster.ConfirmedTensionCount > 0 || cluster.PendingTensionCount > 0 {
			score++
		}
		if score > bestScore || (score == bestScore && score > 0 && bestIdx >= 0 && strings.Compare(strings.TrimSpace(cluster.ProtoClusterID), strings.TrimSpace(report.Clusters[bestIdx].ProtoClusterID)) < 0) {
			bestScore = score
			bestIdx = i
		}
	}
	if bestIdx < 0 || bestScore == 0 {
		return nil
	}
	cluster := report.Clusters[bestIdx]
	return &cluster
}

func sampleIndexWithSoftmax(scores []float64, temperature float64) int {
	if len(scores) == 0 {
		return -1
	}
	if len(scores) == 1 {
		return 0
	}
	if temperature <= 0 {
		temperature = 0.1
	}

	maxScore := scores[0]
	for _, s := range scores[1:] {
		if s > maxScore {
			maxScore = s
		}
	}

	var sum float64
	probs := make([]float64, len(scores))
	for i, s := range scores {
		probs[i] = math.Exp((s - maxScore) / temperature)
		sum += probs[i]
	}

	r := rand.Float64() * sum
	var accum float64
	for i, p := range probs {
		accum += p
		if r <= accum {
			return i
		}
	}
	return len(scores) - 1
}

func selectFocusFrontier(items []TensionFrontierItem, protoClusterID string) *TensionFrontierItem {
	if len(items) == 0 {
		return nil
	}
	candidates := make([]TensionFrontierItem, 0, len(items))
	for _, item := range items {
		if protoClusterID != "" && strings.TrimSpace(item.ProtoClusterID) != protoClusterID {
			continue
		}
		candidates = append(candidates, item)
	}
	if len(candidates) == 0 {
		candidates = append(candidates, items...)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].SurfaceScore != candidates[j].SurfaceScore {
			return candidates[i].SurfaceScore > candidates[j].SurfaceScore
		}
		return strings.Compare(strings.TrimSpace(candidates[i].TensionID), strings.TrimSpace(candidates[j].TensionID)) < 0
	})

	limit := 5
	if limit > len(candidates) {
		limit = len(candidates)
	}
	topK := candidates[:limit]
	scores := make([]float64, len(topK))
	for i, c := range topK {
		scores[i] = float64(c.SurfaceScore)
	}

	idx := sampleIndexWithSoftmax(scores, 50.0)
	if idx < 0 || idx >= len(topK) {
		idx = 0
	}
	item := topK[idx]
	return &item
}

func selectFocusTension(items []TensionRecord, taskID, sessionID string) *TensionRecord {
	if len(items) == 0 {
		return nil
	}
	scored := make([]TensionRecord, 0, len(items))
	scored = append(scored, items...)
	sort.SliceStable(scored, func(i, j int) bool {
		left := tensionPriority(scored[i], taskID, sessionID)
		right := tensionPriority(scored[j], taskID, sessionID)
		if left != right {
			return left > right
		}
		return strings.Compare(scored[i].TensionID, scored[j].TensionID) < 0
	})

	limit := 5
	if limit > len(scored) {
		limit = len(scored)
	}
	topK := scored[:limit]
	scores := make([]float64, len(topK))
	for i, c := range topK {
		scores[i] = float64(tensionPriority(c, taskID, sessionID))
	}

	idx := sampleIndexWithSoftmax(scores, 50.0)
	if idx < 0 || idx >= len(topK) {
		idx = 0
	}
	item := topK[idx]
	return &item
}

func tensionPriority(item TensionRecord, taskID, sessionID string) int {
	score := item.SurfaceScore
	if containsTrimmed(item.TaskIDs, taskID) {
		score += 100
	}
	if containsTrimmed(item.SessionIDs, sessionID) {
		score += 80
	}
	switch strings.ToUpper(strings.TrimSpace(item.ReviewStatus)) {
	case "CONFIRMED":
		score += 20
	case "PENDING":
		score += 10
	}
	return score
}

func fillFocusTensionFromFrontier(focus *RuntimeFocusState, item *TensionFrontierItem) {
	if focus == nil || item == nil {
		return
	}
	focus.FocusTensionID = strings.TrimSpace(item.TensionID)
	focus.FocusTensionType = strings.TrimSpace(item.TensionType)
	focus.FocusTensionTitle = strings.TrimSpace(item.Title)
	focus.FocusTensionSummary = strings.TrimSpace(item.Summary)
	focus.FocusTensionReviewStatus = strings.TrimSpace(item.ReviewStatus)
	focus.FocusTensionSurfaceScore = int(item.SurfaceScore)
}

func fillFocusTensionFromRecord(focus *RuntimeFocusState, item *TensionRecord) {
	if focus == nil || item == nil {
		return
	}
	focus.FocusTensionID = strings.TrimSpace(item.TensionID)
	focus.FocusTensionType = strings.TrimSpace(item.TensionType)
	focus.FocusTensionTitle = strings.TrimSpace(item.Title)
	focus.FocusTensionSummary = strings.TrimSpace(item.Summary)
	focus.FocusTensionReviewStatus = strings.TrimSpace(item.ReviewStatus)
	focus.FocusTensionLifecycleState = strings.TrimSpace(item.LifecycleState)
	focus.FocusTensionSurfaceScore = item.SurfaceScore
	focus.FocusTensionAnchorKind = strings.TrimSpace(item.AnchorKind)
	focus.FocusTensionAnchorRef = strings.TrimSpace(item.AnchorRef)
	focus.FocusTensionDocKeys = cloneStrings(item.DocKeys)
	focus.FocusTensionArtifactRefs = cloneStrings(item.ArtifactRefs)
}

func fillFocusTensionDetail(focus *RuntimeFocusState, detail TensionDetail) {
	fillFocusTensionFromRecord(focus, &detail.Tension)
	if len(detail.Docs) > 0 && len(focus.FocusTensionDocKeys) == 0 {
		docKeys := make([]string, 0, len(detail.Docs))
		for _, doc := range detail.Docs {
			if key := strings.TrimSpace(doc.DocKey); key != "" {
				docKeys = append(docKeys, key)
			}
		}
		focus.FocusTensionDocKeys = docKeys
	}
	if len(detail.Artifacts) > 0 && len(focus.FocusTensionArtifactRefs) == 0 {
		refs := make([]string, 0, len(detail.Artifacts))
		for _, artifact := range detail.Artifacts {
			if ref := strings.TrimSpace(artifact.ArtifactRef); ref != "" {
				refs = append(refs, ref)
			}
		}
		focus.FocusTensionArtifactRefs = refs
	}
}

func hydrationDocKeys(hydration *TaskHydrationBundle) []string {
	if hydration == nil || len(hydration.Docs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(hydration.Docs))
	for _, doc := range hydration.Docs {
		if key := strings.TrimSpace(doc.DocKey); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func containsTrimmed(values []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func intersectsTrimmed(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	index := make(map[string]struct{}, len(left))
	for _, value := range left {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			index[trimmed] = struct{}{}
		}
	}
	for _, value := range right {
		if _, ok := index[strings.TrimSpace(value)]; ok {
			return true
		}
	}
	return false
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}
