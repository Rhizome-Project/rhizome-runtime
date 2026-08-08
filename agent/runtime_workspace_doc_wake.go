package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type workspaceDocWakeCandidate struct {
	DocKey  string
	Version string
}

type workspaceDocWakeTarget struct {
	TaskID    string
	SessionID string
	DocKey    string
	Version   string
}

func (r *Runtime) handleWorkspaceDocRuntimeEvent(ctx context.Context, evt RhizomeEvent) error {
	if r == nil || r.runtimePaused() {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(evt.Type), "workspace_doc.upserted") {
		return nil
	}
	if evt.WorkspaceID != "" && r.cfg.WorkspaceID != "" && evt.WorkspaceID != r.cfg.WorkspaceID {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(evt.AgentID), strings.TrimSpace(r.cfg.AgentID)) {
		return nil
	}
	candidates := workspaceDocWakeCandidatesFromEvent(evt)
	if len(candidates) == 0 {
		return nil
	}
	target, ok := r.blockedWorkspaceDocWakeTarget(candidates)
	if !ok {
		return nil
	}
	return r.queueWorkspaceDocWakeTrigger(ctx, target)
}

func (r *Runtime) reconcileSatisfiedWorkspaceDocBlockers(ctx context.Context) error {
	if r == nil || r.runtimePaused() {
		return nil
	}
	if trigger := r.currentPendingWorkTrigger(); trigger.Trigger != "" {
		return nil
	}
	if target, ok := r.satisfiedWorkspaceDocBlockerTarget(); ok {
		return r.queueWorkspaceDocWakeTrigger(ctx, target)
	}
	if !r.hasWorkspaceDocBlockedWaiters() {
		return nil
	}
	if err := r.refreshBootstrap(ctx); err != nil {
		return err
	}
	if trigger := r.currentPendingWorkTrigger(); trigger.Trigger != "" {
		return nil
	}
	if target, ok := r.satisfiedWorkspaceDocBlockerTarget(); ok {
		return r.queueWorkspaceDocWakeTrigger(ctx, target)
	}
	return nil
}

func (r *Runtime) satisfiedWorkspaceDocBlockerTarget() (workspaceDocWakeTarget, bool) {
	r.mu.Lock()
	candidates := workspaceDocWakeCandidatesFromSnapshot(r.bootstrap.Snapshot)
	r.mu.Unlock()
	if len(candidates) == 0 {
		return workspaceDocWakeTarget{}, false
	}
	return r.blockedWorkspaceDocWakeTarget(candidates)
}

func workspaceDocKeysFromSnapshot(snapshot WorkspaceSnapshot) []string {
	candidates := workspaceDocWakeCandidatesFromSnapshot(snapshot)
	keys := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		keys = append(keys, candidate.DocKey)
	}
	return keys
}

func workspaceDocWakeCandidatesFromSnapshot(snapshot WorkspaceSnapshot) []workspaceDocWakeCandidate {
	keys := make([]string, 0, len(snapshot.Docs))
	versions := make(map[string]string, len(snapshot.Docs))
	for _, doc := range snapshot.Docs {
		docKey := strings.TrimSpace(doc.DocKey)
		if docKey == "" || doc.ArchivedAt != nil {
			continue
		}
		keys = append(keys, docKey)
		versions[docKey] = firstNonEmpty(doc.SHA, doc.UpdatedAt, doc.CreatedAt)
	}
	keys = uniqueTrimmedFocusStrings(keys)
	candidates := make([]workspaceDocWakeCandidate, 0, len(keys))
	for _, key := range keys {
		candidates = append(candidates, workspaceDocWakeCandidate{DocKey: key, Version: versions[key]})
	}
	return candidates
}

func (r *Runtime) hasWorkspaceDocBlockedWaiters() bool {
	r.mu.Lock()
	var activeTask *WorkspaceTaskRecord
	if r.activeTask != nil {
		taskCopy := *r.activeTask
		activeTask = &taskCopy
	}
	var activeSession *AgentSessionStateRecord
	if r.activeSession != nil {
		sessionCopy := *r.activeSession
		sessionCopy.BlockedOn = append([]BlockedRef(nil), r.activeSession.BlockedOn...)
		sessionCopy.RelatedDocKeys = append([]string(nil), r.activeSession.RelatedDocKeys...)
		activeSession = &sessionCopy
	}
	packet := cloneAgentWorkPacket(r.activeWorkPacket)
	snapshotTasks := append([]WorkspaceTaskRecord(nil), r.bootstrap.Snapshot.Tasks...)
	snapshotSessions := append([]AgentSessionStateRecord(nil), r.bootstrap.Snapshot.Sessions...)
	agentID := strings.TrimSpace(r.cfg.AgentID)
	r.mu.Unlock()

	if activeSession != nil && strings.EqualFold(strings.TrimSpace(activeSession.Status), "BLOCKED") {
		if blockedStateHasWorkspaceDocCue(*activeSession, activeTask, packet) {
			return true
		}
	}
	for i := range snapshotSessions {
		session := snapshotSessions[i]
		if !strings.EqualFold(strings.TrimSpace(session.AgentID), agentID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(session.Status), "BLOCKED") {
			continue
		}
		if blockedStateHasWorkspaceDocCue(session, findWorkspaceDocWakeTask(snapshotTasks, session.TaskID), nil) {
			return true
		}
	}
	return false
}

func workspaceDocEventKeys(evt RhizomeEvent) []string {
	keys := make([]string, 0, 3)
	addKey := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		for _, existing := range keys {
			if strings.EqualFold(existing, value) {
				return
			}
		}
		keys = append(keys, value)
	}

	var payload map[string]any
	if raw := strings.TrimSpace(evt.PayloadJSON); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	for _, field := range []string{"doc_key", "docKey"} {
		if value, ok := payload[field].(string); ok {
			addKey(value)
		}
	}
	for _, field := range []string{"doc_keys", "docKeys", "related_doc_keys", "relatedDocKeys"} {
		addJSONStrings(payload[field], addKey)
	}
	return keys
}

func workspaceDocWakeCandidatesFromEvent(evt RhizomeEvent) []workspaceDocWakeCandidate {
	keys := workspaceDocEventKeys(evt)
	version := workspaceDocEventVersion(evt)
	candidates := make([]workspaceDocWakeCandidate, 0, len(keys))
	for _, key := range keys {
		candidates = append(candidates, workspaceDocWakeCandidate{DocKey: key, Version: version})
	}
	return candidates
}

func workspaceDocEventVersion(evt RhizomeEvent) string {
	var payload map[string]any
	if raw := strings.TrimSpace(evt.PayloadJSON); raw != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	for _, field := range []string{"sha", "content_sha", "updated_at", "updatedAt"} {
		if value, ok := payload[field].(string); ok {
			if value = strings.TrimSpace(value); value != "" {
				return value
			}
		}
	}
	return strings.TrimSpace(evt.Timestamp)
}

func addJSONStrings(value any, add func(string)) {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if str, ok := item.(string); ok {
				add(str)
			}
		}
	case []string:
		for _, item := range typed {
			add(item)
		}
	}
}

func (r *Runtime) blockedWorkspaceDocWakeTarget(candidates []workspaceDocWakeCandidate) (workspaceDocWakeTarget, bool) {
	candidates = filterRuntimeOwnedWorkspaceDocWakeCandidates(candidates, r.cfg.AgentID)
	if len(candidates) == 0 {
		return workspaceDocWakeTarget{}, false
	}

	r.mu.Lock()
	activeTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	activeSessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	wakeVersions := cloneStringMap(r.scratch.WorkspaceDocWakeVersions)
	var activeTask *WorkspaceTaskRecord
	if r.activeTask != nil {
		taskCopy := *r.activeTask
		activeTask = &taskCopy
	}
	var activeSession *AgentSessionStateRecord
	if r.activeSession != nil {
		sessionCopy := *r.activeSession
		sessionCopy.BlockedOn = append([]BlockedRef(nil), r.activeSession.BlockedOn...)
		sessionCopy.RelatedDocKeys = append([]string(nil), r.activeSession.RelatedDocKeys...)
		activeSession = &sessionCopy
	}
	packet := cloneAgentWorkPacket(r.activeWorkPacket)
	snapshotTasks := append([]WorkspaceTaskRecord(nil), r.bootstrap.Snapshot.Tasks...)
	snapshotSessions := append([]AgentSessionStateRecord(nil), r.bootstrap.Snapshot.Sessions...)
	r.mu.Unlock()

	if activeSession != nil && strings.EqualFold(strings.TrimSpace(activeSession.Status), "BLOCKED") {
		taskID := firstNonEmpty(activeSession.TaskID, activeTaskID)
		sessionID := firstNonEmpty(activeSession.SessionID, activeSessionID)
		if taskID != "" || sessionID != "" {
			if target, ok := matchingWorkspaceDocWakeTarget(*activeSession, activeTask, packet, candidates, wakeVersions, taskID, sessionID); ok {
				return target, true
			}
		}
	}

	for i := range snapshotSessions {
		session := snapshotSessions[i]
		if !strings.EqualFold(strings.TrimSpace(session.AgentID), strings.TrimSpace(r.cfg.AgentID)) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(session.Status), "BLOCKED") {
			continue
		}
		task := findWorkspaceDocWakeTask(snapshotTasks, session.TaskID)
		taskID := strings.TrimSpace(session.TaskID)
		sessionID := strings.TrimSpace(session.SessionID)
		if target, ok := matchingWorkspaceDocWakeTarget(session, task, nil, candidates, wakeVersions, taskID, sessionID); ok {
			return target, true
		}
	}

	return workspaceDocWakeTarget{}, false
}

func matchingWorkspaceDocWakeTarget(session AgentSessionStateRecord, task *WorkspaceTaskRecord, packet *AgentWorkPacket, candidates []workspaceDocWakeCandidate, wakeVersions map[string]string, taskID, sessionID string) (workspaceDocWakeTarget, bool) {
	for _, candidate := range candidates {
		docKey := strings.TrimSpace(candidate.DocKey)
		if docKey == "" {
			continue
		}
		target := workspaceDocWakeTarget{
			TaskID:    strings.TrimSpace(taskID),
			SessionID: strings.TrimSpace(sessionID),
			DocKey:    docKey,
			Version:   strings.TrimSpace(candidate.Version),
		}
		if workspaceDocWakeVersionSeen(wakeVersions, target) {
			continue
		}
		if blockedStateMentionsWorkspaceDoc(session, task, packet, []string{docKey}) {
			return target, true
		}
	}
	return workspaceDocWakeTarget{}, false
}

func (r *Runtime) queueWorkspaceDocWakeTrigger(ctx context.Context, target workspaceDocWakeTarget) error {
	target.TaskID = strings.TrimSpace(target.TaskID)
	target.SessionID = strings.TrimSpace(target.SessionID)
	target.DocKey = strings.TrimSpace(target.DocKey)
	target.Version = strings.TrimSpace(target.Version)
	if target.TaskID == "" && target.SessionID == "" {
		return nil
	}
	r.mu.Lock()
	alreadySeen := workspaceDocWakeVersionSeen(r.scratch.WorkspaceDocWakeVersions, target)
	r.mu.Unlock()
	if alreadySeen {
		return nil
	}
	if err := r.setPendingWorkTrigger(ctx, "request_resume", target.TaskID, target.SessionID); err != nil {
		return err
	}
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		if state.WorkspaceDocWakeVersions == nil {
			state.WorkspaceDocWakeVersions = map[string]string{}
		}
		if key := workspaceDocWakeStateKey(target); key != "" && target.Version != "" {
			state.WorkspaceDocWakeVersions[key] = target.Version
		}
	})
}

func workspaceDocWakeVersionSeen(versions map[string]string, target workspaceDocWakeTarget) bool {
	key := workspaceDocWakeStateKey(target)
	if key == "" || strings.TrimSpace(target.Version) == "" {
		return false
	}
	return strings.TrimSpace(versions[key]) == strings.TrimSpace(target.Version)
}

func workspaceDocWakeStateKey(target workspaceDocWakeTarget) string {
	docKey := strings.TrimSpace(target.DocKey)
	if docKey == "" {
		return ""
	}
	scope := firstNonEmpty(strings.TrimSpace(target.SessionID), strings.TrimSpace(target.TaskID))
	if scope == "" {
		return ""
	}
	return fmt.Sprintf("%s|%s", scope, docKey)
}

func findWorkspaceDocWakeTask(tasks []WorkspaceTaskRecord, taskID string) *WorkspaceTaskRecord {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil
	}
	for i := range tasks {
		if strings.TrimSpace(tasks[i].TaskID) == taskID {
			taskCopy := tasks[i]
			return &taskCopy
		}
	}
	return nil
}

func blockedStateMentionsWorkspaceDoc(session AgentSessionStateRecord, task *WorkspaceTaskRecord, packet *AgentWorkPacket, docKeys []string) bool {
	if len(docKeys) == 0 {
		return false
	}
	explicitDocKeys := append([]string(nil), session.RelatedDocKeys...)
	textParts := []string{session.Summary, session.TaskID, session.SessionID}
	for _, blocker := range session.BlockedOn {
		explicitDocKeys = append(explicitDocKeys, blocker.Detail)
		textParts = append(textParts, blocker.Kind, blocker.Detail)
	}
	if task != nil {
		textParts = append(textParts, task.TaskID, task.Title, task.Description, task.CloseReason)
	}
	if packet != nil {
		explicitDocKeys = append(explicitDocKeys, packet.ContextHints.SuggestedDocKeys...)
		for _, blocker := range packet.Blockers {
			explicitDocKeys = append(explicitDocKeys, blocker.Detail)
			textParts = append(textParts, blocker.Kind, blocker.Detail)
		}
		if packet.Gate != nil {
			textParts = append(textParts, packet.Gate.Summary)
		}
		if packet.Unblock != nil {
			textParts = append(textParts, packet.Unblock.Summary)
		}
	}
	for _, docKey := range docKeys {
		if matchesAnyDocKey(docKey, explicitDocKeys) || textMentionsDocKey(docKey, textParts) {
			return true
		}
	}
	return false
}

func filterRuntimeOwnedWorkspaceDocWakeCandidates(candidates []workspaceDocWakeCandidate, agentID string) []workspaceDocWakeCandidate {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" || len(candidates) == 0 {
		return candidates
	}
	selfContextDoc := agentContextDocKey(agentID)
	selfClaimedWorkDoc := claimedWorkDocKey(agentID)
	filtered := candidates[:0]
	for _, candidate := range candidates {
		docKey := strings.TrimSpace(candidate.DocKey)
		if strings.EqualFold(docKey, selfContextDoc) || strings.EqualFold(docKey, selfClaimedWorkDoc) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}

func blockedStateHasWorkspaceDocCue(session AgentSessionStateRecord, task *WorkspaceTaskRecord, packet *AgentWorkPacket) bool {
	return len(blockedStateWorkspaceDocCues(session, task, packet)) > 0
}

func blockedStateWorkspaceDocCues(session AgentSessionStateRecord, task *WorkspaceTaskRecord, packet *AgentWorkPacket) []string {
	candidates := append([]string(nil), session.RelatedDocKeys...)
	textParts := []string{session.Summary, session.TaskID, session.SessionID}
	for _, blocker := range session.BlockedOn {
		candidates = append(candidates, blocker.Detail)
		textParts = append(textParts, blocker.Kind, blocker.Detail)
	}
	if task != nil {
		candidates = append(candidates, docKeysMentionedByTask(*task)...)
		textParts = append(textParts, task.TaskID, task.Title, task.Description, task.CloseReason)
	}
	if packet != nil {
		candidates = append(candidates, packet.ContextHints.SuggestedDocKeys...)
		for _, blocker := range packet.Blockers {
			candidates = append(candidates, blocker.Detail)
			textParts = append(textParts, blocker.Kind, blocker.Detail)
		}
	}
	for _, part := range textParts {
		for _, match := range taskDocKeyCandidatePattern.FindAllString(part, -1) {
			key := strings.Trim(strings.TrimSpace(match), ".,;:()[]{}<>\"'")
			if strings.TrimSpace(key) == "" || !strings.Contains(key, ".") {
				continue
			}
			candidates = append(candidates, key)
		}
	}
	return uniqueTrimmedFocusStrings(candidates)
}

func matchesAnyDocKey(docKey string, candidates []string) bool {
	docKey = strings.TrimSpace(docKey)
	if docKey == "" {
		return false
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if strings.EqualFold(candidate, docKey) || strings.Contains(strings.ToLower(candidate), strings.ToLower(docKey)) {
			return true
		}
	}
	return false
}

func textMentionsDocKey(docKey string, textParts []string) bool {
	docKey = strings.TrimSpace(docKey)
	if docKey == "" {
		return false
	}
	needle := strings.ToLower(docKey)
	for _, part := range textParts {
		if strings.Contains(strings.ToLower(part), needle) {
			return true
		}
	}
	return false
}
