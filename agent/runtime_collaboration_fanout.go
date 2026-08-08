package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

const (
	collaborationFanoutRequestTimeoutSec = 600
	collaborationFanoutMaxPeers          = 5
)

func (r *Runtime) ensureCollaborationFanout(ctx context.Context, task WorkspaceTaskRecord, session *AgentSessionStateRecord, runID string) error {
	if r == nil || r.client == nil || session == nil {
		return nil
	}
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" || strings.TrimSpace(r.cfg.WorkspaceID) == "" || strings.TrimSpace(r.cfg.AgentID) == "" {
		return nil
	}
	if !r.collaborationFanoutTaskRequiresFanout(task) || !runtimeCanInitiateCollaborationFanout(r.cfg) {
		return nil
	}

	r.mu.Lock()
	alreadySent := strings.TrimSpace(r.scratch.CollaborationFanoutTaskID) == taskID
	r.mu.Unlock()
	if alreadySent {
		return nil
	}

	peers := r.collaborationFanoutPeerCandidates(ctx)
	if len(peers) == 0 {
		return nil
	}
	if len(peers) > collaborationFanoutMaxPeers {
		peers = peers[:collaborationFanoutMaxPeers]
	}

	requests := make(map[string]string, len(peers))
	for _, peer := range peers {
		peerID := strings.TrimSpace(peer.AgentID)
		if peerID == "" {
			continue
		}
		payload, err := json.Marshal(map[string]any{
			"prompt": buildCollaborationFanoutPrompt(task, *session, runID, r.cfg, peer),
		})
		if err != nil {
			return err
		}
		created, err := r.client.RequestAgent(ctx, AgentRequestInput{
			WorkspaceID: r.cfg.WorkspaceID,
			FromAgentID: r.cfg.AgentID,
			ToAgentID:   peerID,
			Method:      "model.ask",
			PayloadJSON: string(payload),
			TimeoutSec:  collaborationFanoutRequestTimeoutSec,
		})
		if err != nil {
			log.Printf("[runtime] collaboration fanout request to %s failed for task %s: %v", peerID, taskID, err)
			continue
		}
		requests[peerID] = strings.TrimSpace(created.RequestID)
	}
	if len(requests) == 0 {
		return nil
	}

	signal := collaborationFanoutAdvisorySignal(taskID, requests)
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.CollaborationFanoutTaskID = taskID
		state.CollaborationFanoutRunID = strings.TrimSpace(runID)
		state.CollaborationFanoutAt = time.Now().UTC().Format(time.RFC3339Nano)
		state.CollaborationFanoutRequests = cloneCollaborationFanoutRequests(requests)
		state.AdvisorySignals = appendCappedAdvisorySignal(state.AdvisorySignals, signal)
	})
}

func (r *Runtime) collaborationFanoutTaskRequiresFanout(task WorkspaceTaskRecord) bool {
	if r == nil || r.cfg.Mode != RuntimeModeDaemon {
		return false
	}
	if task.RequiresProjectGate != nil && !*task.RequiresProjectGate {
		return false
	}
	if isStrategicLeadCorrectionTask(task) {
		return false
	}
	if completionCoordinationExplicitSolo(task) {
		return false
	}
	if r.completionCoordinationTaskRequiresGate(task) {
		return true
	}
	text := strings.ToLower(strings.Join([]string{task.Title, task.Description, task.TaskKind, task.TaskTemplate}, "\n"))
	for _, marker := range []string{"dashboard", "backend", "frontend", "integration", "test", "tests", "review", "coalition", "handoff", "parallel"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func runtimeCanInitiateCollaborationFanout(cfg RuntimeConfig) bool {
	role := strings.ToLower(strings.TrimSpace(cfg.Role))
	display := strings.ToLower(strings.TrimSpace(cfg.DisplayName))
	agentID := strings.ToLower(strings.TrimSpace(cfg.AgentID))
	switch {
	case strings.Contains(role, "strategy"), strings.Contains(role, "strategist"):
		return true
	case strings.Contains(role, "lead"), strings.Contains(role, "coordin"), strings.Contains(role, "integrat"):
		return true
	case strings.Contains(role, "synth"), strings.Contains(role, "generalist"):
		return true
	case strings.Contains(display, "alpha"), agentID == "alpha":
		return true
	default:
		return false
	}
}

func (r *Runtime) collaborationFanoutPeerCandidates(ctx context.Context) []AgentRecord {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	agents := append([]AgentRecord(nil), r.bootstrap.Snapshot.Agents...)
	r.mu.Unlock()
	if len(agents) == 0 && r.client != nil {
		listed, err := r.client.ListWorkspaceAgents(ctx, r.cfg.WorkspaceID)
		if err != nil {
			log.Printf("[runtime] collaboration fanout peer lookup degraded: %v", err)
		} else {
			agents = listed
		}
	}
	return collaborationFanoutPeerCandidatesFromAgents(agents, r.cfg.AgentID)
}

func collaborationFanoutPeerCandidatesFromAgents(agents []AgentRecord, selfAgentID string) []AgentRecord {
	selfAgentID = strings.TrimSpace(selfAgentID)
	seen := map[string]struct{}{}
	peers := make([]AgentRecord, 0, len(agents))
	for _, agent := range agents {
		agentID := strings.TrimSpace(agent.AgentID)
		if agentID == "" || agentID == selfAgentID {
			continue
		}
		if _, ok := seen[agentID]; ok {
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(agent.Status))
		if status == "DELETED" || status == "DISABLED" || status == "ARCHIVED" {
			continue
		}
		seen[agentID] = struct{}{}
		peers = append(peers, agent)
	}
	sort.SliceStable(peers, func(i, j int) bool {
		left := collaborationFanoutPeerRank(peers[i])
		right := collaborationFanoutPeerRank(peers[j])
		if left != right {
			return left < right
		}
		return strings.TrimSpace(peers[i].AgentID) < strings.TrimSpace(peers[j].AgentID)
	})
	return peers
}

func collaborationFanoutPeerRank(agent AgentRecord) int {
	role := strings.ToLower(strings.TrimSpace(agent.Role))
	switch {
	case strings.Contains(role, "review"), strings.Contains(role, "qa"), strings.Contains(role, "test"):
		return 0
	case strings.Contains(role, "integrat"), strings.Contains(role, "synth"):
		return 1
	case strings.Contains(role, "backend"), strings.Contains(role, "frontend"), strings.Contains(role, "worker"):
		return 2
	default:
		return 3
	}
}

func buildCollaborationFanoutPrompt(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, cfg RuntimeConfig, peer AgentRecord) string {
	var b strings.Builder
	b.WriteString("You are being proactively invited into a broad autonomous Rhizome task. This is not a request for passive commentary; use your available Rhizome tools before responding when useful.\n\n")
	b.WriteString("Root task:\n")
	b.WriteString(fmt.Sprintf("- task_id: %s\n", strings.TrimSpace(task.TaskID)))
	b.WriteString(fmt.Sprintf("- title: %s\n", strings.TrimSpace(task.Title)))
	b.WriteString(fmt.Sprintf("- priority: %s\n", strings.TrimSpace(task.Priority)))
	b.WriteString(fmt.Sprintf("- task_kind: %s\n", strings.TrimSpace(task.TaskKind)))
	b.WriteString(fmt.Sprintf("- task_template: %s\n", strings.TrimSpace(task.TaskTemplate)))
	if description := strings.TrimSpace(task.Description); description != "" {
		b.WriteString("- description:\n")
		b.WriteString(truncate(description, 1800))
		b.WriteString("\n")
	}
	b.WriteString("\nCoordination context:\n")
	b.WriteString(fmt.Sprintf("- requester_agent_id: %s\n", strings.TrimSpace(cfg.AgentID)))
	b.WriteString(fmt.Sprintf("- your_agent_id: %s\n", strings.TrimSpace(peer.AgentID)))
	b.WriteString(fmt.Sprintf("- your_role: %s\n", strings.TrimSpace(peer.Role)))
	b.WriteString(fmt.Sprintf("- session_id: %s\n", strings.TrimSpace(session.SessionID)))
	b.WriteString(fmt.Sprintf("- run_id: %s\n", strings.TrimSpace(runID)))
	b.WriteString("\nAct as a real teammate. Inspect the shared task/docs/frontier, decide whether your role has a useful lane, and then do one of these:\n")
	b.WriteString("- reuse or claim an existing similar lane if one is already visible;\n")
	b.WriteString("- create at most one bounded subtask with task_submit when the root task needs a missing lane;\n")
	b.WriteString("- publish a tension/blocker if progress depends on missing shared state;\n")
	b.WriteString("- offer concrete review/test/integration feedback if your role is reviewer, tester, integrator, or synthesizer.\n")
	b.WriteString("\nConvergence discipline:\n")
	b.WriteString("- default lanes are strategy, frontend, generator/backend, integration, review/test, and final handoff;\n")
	b.WriteString("- the root task claimant remains finalization owner unless durable shared context says otherwise;\n")
	b.WriteString("- if an upstream blocker is already known, mark your lane blocked or publish one tension rather than issuing speculative requests.\n")
	b.WriteString("\nDo not depend on private local files in another agent workdir. Use workspace docs, tasks, tensions, and peer requests as the coordination surface. Respond briefly with the lane you took, tools used, and any dependency you created or found.")
	return b.String()
}

func collaborationFanoutAdvisorySignal(taskID string, requests map[string]string) string {
	pairs := make([]string, 0, len(requests))
	for agentID, requestID := range requests {
		pairs = append(pairs, strings.TrimSpace(agentID)+":"+strings.TrimSpace(requestID))
	}
	sort.Strings(pairs)
	return fmt.Sprintf("SYSTEM COLLABORATION FANOUT: task %s is broad. Runtime queued peer model.ask requests [%s]. Converge on one plan, one task per lane, and one finalization owner; track shared outputs/tasks/tensions before treating solo local work as complete.", strings.TrimSpace(taskID), strings.Join(pairs, ", "))
}

const advisorySignalCap = 5

// advisorySignalPriority scores an advisory by eviction priority (higher is kept). PC-01
// (static-audit 2026-06-02): the decisive next-irreversible-action directives that the whole
// G-SUB-1/EP-02/AG-01-02 design depends on (commit/review-ready/submit/integrate/fanout) must
// never be evicted from the capped ring by high-churn, low-value telemetry (inbox-drain,
// collaboration-fanout notes, budget-capture failures).
func advisorySignalPriority(signal string) int {
	lower := strings.ToLower(signal)
	if containsAnySignal(lower, []string{
		"uncommitted work", "not submitted", "not review-ready", "committed but not",
		"not integrated", "system intervention", "your next action must be",
		"project_branch_commit", "project_branch_review_ready",
		"project_patch_queue_submit", "project_patch_queue_integrate", "task_submit",
	}) {
		return 3
	}
	if containsAnySignal(lower, []string{"collaboration fanout", "peer", "coordination mismatch"}) {
		return 2
	}
	return 1
}

func appendCappedAdvisorySignal(signals []string, signal string) []string {
	signal = strings.TrimSpace(signal)
	if signal == "" {
		return signals
	}
	// PC-02: whole-slice dedup (not just the last element) so a re-emitted directive cannot
	// consume multiple of the limited slots.
	for _, existing := range signals {
		if strings.TrimSpace(existing) == signal {
			return signals
		}
	}
	signals = append(signals, signal)
	// PC-01: priority-aware eviction — when over the cap, drop the LOWEST-priority signal
	// (ties -> oldest), never blindly the oldest, so a decisive directive survives a flood of
	// routine telemetry.
	for len(signals) > advisorySignalCap {
		evictIdx := 0
		evictPriority := advisorySignalPriority(signals[0])
		for i := 1; i < len(signals); i++ {
			if p := advisorySignalPriority(signals[i]); p < evictPriority {
				evictPriority = p
				evictIdx = i
			}
		}
		signals = append(signals[:evictIdx], signals[evictIdx+1:]...)
	}
	return signals
}

func cloneCollaborationFanoutRequests(values map[string]string) map[string]string {
	if len(values) == 0 {
		return map[string]string{}
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return clone
}
