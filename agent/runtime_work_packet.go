package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (r *Runtime) prepareSessionForSelectedWork(ctx context.Context, task WorkspaceTaskRecord, work AgentWorkNextResult) (*AgentSessionStateRecord, string, error) {
	switch strings.TrimSpace(work.SessionAction) {
	case "", "start_new":
		return r.ensureSessionForTask(ctx, task)
	case "reuse_active":
		if work.Session == nil || !isSessionRunnable(work.Session) {
			return r.ensureSessionForTask(ctx, task)
		}
		sessionCopy := *work.Session
		runID, err := r.ensureExecutionRunForMaterializedSession(ctx, task, sessionCopy)
		if err != nil {
			return nil, "", err
		}
		return &sessionCopy, runID, nil
	case "resume_inactive":
		if work.Session == nil {
			return nil, "", fmt.Errorf("work packet requested resume_inactive without session")
		}
		return r.resumeSelectedSession(ctx, task, *work.Session, work)
	default:
		return nil, "", fmt.Errorf("unsupported work session_action %q", work.SessionAction)
	}
}

func (r *Runtime) resumeSelectedSession(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, work AgentWorkNextResult) (*AgentSessionStateRecord, string, error) {
	resumed, err := r.client.SessionEvent(ctx, "agent.session.status", SessionEventInput{
		WorkspaceID:       r.cfg.WorkspaceID,
		SessionID:         session.SessionID,
		AgentID:           r.cfg.AgentID,
		TaskID:            firstNonEmpty(task.TaskID, session.TaskID),
		Summary:           firstNonEmpty(work.ResumeSummary, "Session selected for deterministic resume"),
		Status:            "ACTIVE",
		OwnerScope:        firstNonEmpty(session.OwnerScope, "task/session"),
		KeepSessionActive: boolPtr(true),
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, "", err
	}
	runID, err := r.ensureExecutionRunForMaterializedSession(ctx, task, resumed)
	if err != nil {
		return nil, "", err
	}
	r.logSessionTransitionMemory("session_resume_selected", firstNonEmpty(task.TaskID, resumed.TaskID), resumed.SessionID, resumed.Status, resumed.Summary, "", "", nil)
	return &resumed, runID, nil
}

func (r *Runtime) ensureExecutionRunForMaterializedSession(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord) (string, error) {
	runID, err := r.ensureExecutionRun(ctx, task, session)
	if err == nil {
		return runID, nil
	}
	if endErr := r.endSessionAfterMaterializationFailure(ctx, task, session, err); endErr != nil {
		return "", fmt.Errorf("%w; session materialization repair end failed: %v", err, endErr)
	}
	return "", err
}

func (r *Runtime) endSessionAfterMaterializationFailure(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, cause error) error {
	if r == nil || r.client == nil || strings.TrimSpace(session.SessionID) == "" {
		return nil
	}
	reason := "session materialization failed before execution run was recorded"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		reason += ": " + oneLine(cause.Error())
	}
	keepActive := false
	_, err := r.client.SessionEvent(ctx, "agent.session.end", SessionEventInput{
		WorkspaceID:       r.cfg.WorkspaceID,
		SessionID:         session.SessionID,
		AgentID:           firstNonEmpty(strings.TrimSpace(session.AgentID), r.cfg.AgentID),
		TaskID:            firstNonEmpty(strings.TrimSpace(task.TaskID), strings.TrimSpace(session.TaskID)),
		Summary:           reason,
		Status:            "ENDED",
		OwnerScope:        firstNonEmpty(strings.TrimSpace(session.OwnerScope), "task/session"),
		KeepSessionActive: &keepActive,
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	})
	return err
}

func (r *Runtime) repairPostClaimSessionFailure(ctx context.Context, task WorkspaceTaskRecord, work AgentWorkNextResult, cause error) error {
	if r == nil || r.client == nil {
		return nil
	}
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return nil
	}
	reason := "post-claim session materialization failed"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		reason += ": " + oneLine(cause.Error())
	}
	if err := r.client.BlockTask(ctx, TaskBlockInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		TaskID:      taskID,
		Reason:      reason,
	}); err != nil {
		return err
	}

	r.mu.Lock()
	changed := false
	if r.activeTask != nil && strings.TrimSpace(r.activeTask.TaskID) == taskID {
		r.activeTask = nil
		r.activeSession = nil
		r.activeRunID = ""
		r.activeHydration = nil
		r.activeWorkPacket = nil
		changed = true
	}
	if strings.TrimSpace(r.scratch.ActiveTaskID) == taskID {
		r.scratch.ActiveTaskID = ""
		r.scratch.ActiveSessionID = ""
		r.scratch.ActiveRunID = ""
		changed = true
	}
	if strings.TrimSpace(r.scratch.PendingTriggerTask) == taskID {
		clearPendingTriggerFields(&r.scratch)
		changed = true
	}
	if changed {
		r.scratch.LastSummary = fmt.Sprintf("Task %s blocked after claim because session materialization failed", taskID)
		r.invalidateFocusLocked()
	}
	stateCopy := r.scratch
	r.mu.Unlock()
	if changed {
		return r.saveScratchState(ctx, stateCopy)
	}
	return nil
}
