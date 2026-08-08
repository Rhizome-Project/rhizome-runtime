package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

//go:embed docs/rhizome-native-agent-runtime-spec.md
var nativeRuntimeSpec string

const (
	projectCoordinationCheckoutPromptLimit = 10
	projectCoordinationBranchPromptLimit   = 10

	// TE-30 Packet Prompt Diet: coordination doc BODIES are a one-time
	// per-session cost, not coordination source-of-truth. Cap each inlined
	// body to a head excerpt and refer agents to the read-only doc-fetch tool
	// (workspace_doc_get) for the full content. Preserved coordination fields
	// (project/task/branch/checkout/patch-queue/role/blocker truth) are
	// rendered by projectCoordinationPromptSummary and are never diet-capped.
	promptDocBodyCapChars    = 2048
	promptDocBudgetCapChars  = 8192
	promptDocBodyExcerptHead = 480
)

func buildSpecPack(cfg RuntimeConfig, snapshot *WorkspaceSnapshot, hydration *TaskHydrationBundle, task *WorkspaceTaskRecord) string {
	var b strings.Builder

	spec := strings.TrimSpace(nativeRuntimeSpec)
	if spec != "" {
		b.WriteString("## RNAR Core Spec\n")
		b.WriteString(clipForPrompt(spec, cfg.MaxPromptSpecChars))
		b.WriteString("\n\n")
	}

	if hydration != nil {
		b.WriteString("## Hydrated Task Context\n")
		if hydration.Workspace != nil {
			b.WriteString(fmt.Sprintf("- Workspace: %s\n", firstNonEmpty(hydration.Workspace.Title, hydration.Workspace.WorkspaceID)))
			b.WriteString(fmt.Sprintf("- Workspace Status: %s\n", firstNonEmpty(hydration.Workspace.Status, "unknown")))
		}
		b.WriteString(fmt.Sprintf("- Task: %s\n", firstNonEmpty(hydration.Task.Title, hydration.Task.TaskID)))
		b.WriteString(fmt.Sprintf("- Related Tasks: %d\n", len(hydration.RelatedTasks)))
		b.WriteString(fmt.Sprintf("- Related Artifacts: %d\n", len(hydration.Artifacts)))
		b.WriteString(fmt.Sprintf("- Related Updates: %d\n", len(hydration.Updates)))
		b.WriteString(fmt.Sprintf("- Pending Side Effects: %d\n", len(hydration.SideEffects)))
		b.WriteString("\n")

		if len(hydration.RelatedTasks) > 0 {
			b.WriteString("### Related Tasks\n")
			for _, related := range hydration.RelatedTasks[:min(len(hydration.RelatedTasks), 8)] {
				b.WriteString(fmt.Sprintf("- %s | status=%s | priority=%s | %s\n",
					firstNonEmpty(related.TaskID, "task"),
					firstNonEmpty(related.Status, "unknown"),
					firstNonEmpty(related.Priority, "unknown"),
					firstNonEmpty(oneLine(related.Title), oneLine(related.Description), "no title"),
				))
			}
			b.WriteString("\n")
		}

		writeDietedDocs(&b, prioritizedDocs(hydration.Docs, task), cfg)

		if len(hydration.Updates) > 0 {
			b.WriteString("### Related Updates\n")
			for _, update := range hydration.Updates {
				b.WriteString(fmt.Sprintf("- %s [%s] %s\n", firstNonEmpty(update.AgentName, update.AgentID), update.UpdateType, oneLine(update.Summary)))
			}
			b.WriteString("\n")
		}
		if len(hydration.SideEffects) > 0 {
			b.WriteString("### Pending Side Effects\n")
			for _, effect := range hydration.SideEffects[:min(len(hydration.SideEffects), 8)] {
				b.WriteString(fmt.Sprintf("- %s | relation=%s | status=%s | decision=%s | artifact=%s | region=%s | next=classify_before_integration\n",
					firstNonEmpty(effect.SideEffectRef, "side-effect"),
					firstNonEmpty(effect.BoundaryRelation, "unknown"),
					firstNonEmpty(effect.IntegrationStatus, "unknown"),
					firstNonEmpty(effect.Decision, "none_pending"),
					firstNonEmpty(effect.ArtifactRef, "artifact"),
					firstNonEmpty(effect.RegionRef, "region"),
				))
			}
			b.WriteString("\n")
		}
		if len(hydration.Artifacts) > 0 {
			b.WriteString("### Related Artifacts\n")
			for _, artifact := range hydration.Artifacts {
				b.WriteString(fmt.Sprintf("- %s | %s | %s\n", firstNonEmpty(artifact.Title, artifact.ArtifactID), artifact.Kind, artifact.ArtifactRef))
			}
			b.WriteString("\n")
		}
	}

	if hydration == nil && snapshot == nil {
		return strings.TrimSpace(b.String())
	}

	if snapshot != nil {
		b.WriteString("## Workspace Snapshot\n")
		b.WriteString(fmt.Sprintf("- Workspace: %s\n", firstNonEmpty(snapshot.Workspace.Title, snapshot.Workspace.WorkspaceID)))
		b.WriteString(fmt.Sprintf("- Status: %s\n", firstNonEmpty(snapshot.Workspace.Status, "unknown")))
		b.WriteString(fmt.Sprintf("- Tasks Visible: %d\n", len(snapshot.Tasks)))
		b.WriteString(fmt.Sprintf("- Active Sessions Visible: %d\n", len(snapshot.Sessions)))
		b.WriteString(fmt.Sprintf("- Shared Docs Visible: %d\n", len(snapshot.Docs)))
		b.WriteString("\n")

		writeDietedDocs(&b, prioritizedDocs(snapshot.Docs, task), cfg)
	}

	return strings.TrimSpace(b.String())
}

func buildAgentRosterPack(agents []AgentRecord, selfAgentID string, maxChars int) string {
	if len(agents) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("## Workspace Agent Roster\n")
	b.WriteString("- Use exact agent_id values for agent_request.to_agent_id; do not infer peer IDs from display names.\n")
	b.WriteString("- Peer local workdirs are private. Before asking a peer to review or test a file you wrote locally, store the artifact with workspace_doc_put and reference its doc_key, or include the exact content/excerpt in agent_request.prompt.\n")
	b.WriteString("- If a peer reports a missing or inaccessible artifact, do not repeat the same local-path request; share the artifact through workspace docs or surface the dependency/tension.\n")

	targets := directAgentRequestTargets(agents, selfAgentID)
	if len(targets) > 0 {
		b.WriteString("- Direct agent_request targets:\n")
		for _, agent := range targets[:min(len(targets), 8)] {
			b.WriteString(fmt.Sprintf("  - %s | role=%s | status=%s | %s\n",
				strings.TrimSpace(agent.AgentID),
				firstNonEmpty(strings.TrimSpace(agent.Role), "unspecified"),
				agentRosterStatus(agent),
				firstNonEmpty(oneLine(agent.DisplayName), "unnamed agent"),
			))
		}
	}

	b.WriteString("- Visible agents:\n")
	for _, agent := range agents[:min(len(agents), 12)] {
		marker := ""
		if strings.TrimSpace(agent.AgentID) == strings.TrimSpace(selfAgentID) {
			marker = " self=true"
		}
		active := ""
		if len(agent.ActiveTasks) > 0 {
			active = fmt.Sprintf(" active_tasks=%d", len(agent.ActiveTasks))
		}
		b.WriteString(fmt.Sprintf("  - %s (%s): role=%s status=%s%s%s capabilities=%s\n",
			firstNonEmpty(strings.TrimSpace(agent.DisplayName), strings.TrimSpace(agent.AgentID), "agent"),
			strings.TrimSpace(agent.AgentID),
			firstNonEmpty(strings.TrimSpace(agent.Role), "unspecified"),
			agentRosterStatus(agent),
			active,
			marker,
			joinOrDash(agent.Capabilities, 4),
		))
	}

	return clipForPrompt(strings.TrimSpace(b.String()), maxChars)
}

func directAgentRequestTargets(agents []AgentRecord, selfAgentID string) []AgentRecord {
	selfAgentID = strings.TrimSpace(selfAgentID)
	out := make([]AgentRecord, 0, len(agents))
	for _, agent := range agents {
		agentID := strings.TrimSpace(agent.AgentID)
		if agentID == "" || agentID == selfAgentID {
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(agent.Status))
		if status == "DELETED" {
			continue
		}
		out = append(out, agent)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return directAgentRequestTargetRank(out[i]) < directAgentRequestTargetRank(out[j])
	})
	return out
}

func directAgentRequestTargetRank(agent AgentRecord) int {
	rank := completionCoordinationPeerRank(agent)
	if !agent.IsOnline {
		rank += 20
	}
	switch strings.ToUpper(strings.TrimSpace(agent.Status)) {
	case "", "ACTIVE", "ONLINE":
	default:
		rank += 10
	}
	if len(agent.ActiveTasks) > 0 {
		rank += 2
	}
	return rank
}

func agentRosterStatus(agent AgentRecord) string {
	status := firstNonEmpty(strings.TrimSpace(agent.Status), "unknown")
	if agent.IsOnline {
		return status + "/online"
	}
	return status + "/offline"
}

func appendSpecPack(base, extra string) string {
	base = strings.TrimSpace(base)
	extra = strings.TrimSpace(extra)
	switch {
	case base == "":
		return extra
	case extra == "":
		return base
	default:
		return base + "\n\n" + extra
	}
}

func buildCoordinationPack(report *ControlReport, frontier []TensionFrontierItem, maxChars int) string {
	if report == nil && len(frontier) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("## Coordination Pressure\n")

	if report != nil {
		b.WriteString(fmt.Sprintf("- Hot Clusters: %d\n", report.Workspace.HotClusterCount))
		b.WriteString(fmt.Sprintf("- Attention Clusters: %d\n", report.Workspace.AttentionClusterCount))
		if report.Workspace.HighestPressureClusterID != "" {
			b.WriteString(fmt.Sprintf("- Highest Pressure Cluster: %s (score %.2f)\n", report.Workspace.HighestPressureClusterID, report.Workspace.HighestPressureScore))
		}
		if len(report.Clusters) > 0 {
			b.WriteString("- Top Control Clusters:\n")
			for _, cluster := range report.Clusters {
				b.WriteString(fmt.Sprintf("  - %s | %s | tasks=%s | agents=%s | docs=%s | tensions=%d/%d\n",
					firstNonEmpty(cluster.ProtoClusterID, "cluster"),
					firstNonEmpty(oneLine(cluster.Summary), "no summary"),
					joinOrDash(cluster.TaskIDs, 3),
					joinOrDash(cluster.AgentIDs, 3),
					joinOrDash(cluster.DocKeys, 3),
					cluster.PendingTensionCount,
					cluster.ConfirmedTensionCount,
				))
				if summary := summarizeControlMaps(cluster.Signals, 2); summary != "" {
					b.WriteString(fmt.Sprintf("    signals: %s\n", summary))
				}
				if controls := summarizeControlMaps(cluster.SuggestedControls, 2); controls != "" {
					b.WriteString(fmt.Sprintf("    suggested_controls: %s\n", controls))
				}
			}
		}
	}

	if len(frontier) > 0 {
		b.WriteString("- Tension Frontier:\n")
		for _, item := range frontier {
			b.WriteString(fmt.Sprintf("  - %s | %s | %s | %s | score=%.2f | evidence=%d\n",
				firstNonEmpty(item.TensionID, "tension"),
				firstNonEmpty(item.TensionType, "unknown"),
				firstNonEmpty(item.ReviewStatus, "unknown"),
				firstNonEmpty(oneLine(item.Title), oneLine(item.Summary), "no summary"),
				item.SurfaceScore,
				item.EvidenceCount,
			))
		}
	}

	return clipForPrompt(strings.TrimSpace(b.String()), maxChars)
}

func buildWorkPacketPack(packet *AgentWorkPacket, maxChars int) string {
	if packet == nil {
		return ""
	}

	var b strings.Builder
	b.WriteString("## Work Packet\n")
	b.WriteString(fmt.Sprintf("- Work Type: %s\n", firstNonEmpty(packet.WorkType, "unknown")))
	if packet.WhyNow != "" || packet.PreferredTransition != "" || packet.CoordinationState != "" {
		b.WriteString(fmt.Sprintf("- Coordination: why_now=%s | transition=%s | state=%s\n",
			firstNonEmpty(packet.WhyNow, "-"),
			firstNonEmpty(packet.PreferredTransition, "-"),
			firstNonEmpty(packet.CoordinationState, "-"),
		))
	}
	if packet.ProjectID != "" || packet.TaskKind != "" || packet.ProjectLane != "" || packet.RequiresProjectGate != nil {
		b.WriteString(fmt.Sprintf("- Project: id=%s | task_kind=%s | lane=%s | requires_gate=%s\n",
			firstNonEmpty(packet.ProjectID, "-"),
			firstNonEmpty(packet.TaskKind, "-"),
			firstNonEmpty(packet.ProjectLane, "-"),
			projectGateBoolLabel(packet.RequiresProjectGate),
		))
	}
	if len(packet.ProjectGateBlock) > 0 {
		b.WriteString(fmt.Sprintf("- Project Gate Block: %s\n", truncate(oneLine(string(packet.ProjectGateBlock)), 300)))
	}
	if len(packet.ProjectCoordination) > 0 {
		b.WriteString(projectCoordinationPromptSummary(packet.ProjectCoordination, 6))
	}
	if packet.Frontier != nil {
		frontier := packet.Frontier
		b.WriteString(fmt.Sprintf("- Task Frontier: generation_id=%s | mode=%s | candidates=%d | roster=%d\n",
			firstNonEmpty(frontier.GenerationID, "-"),
			firstNonEmpty(frontier.SelectionMode, "-"),
			len(frontier.Candidates),
			len(frontier.Roster),
		))
		for i, candidate := range frontier.Candidates {
			if i >= 5 {
				break
			}
			block := "no"
			if candidate.Blocked {
				block = firstNonEmpty(candidate.BlockReason, "yes")
			}
			b.WriteString(fmt.Sprintf("  - Candidate: task_id=%s | fit=%s/%d | blocked=%s | title=%s\n",
				firstNonEmpty(candidate.Task.TaskID, "-"),
				firstNonEmpty(candidate.Fit.Level, "-"),
				candidate.Fit.Score,
				block,
				truncate(oneLine(candidate.Task.Title), 100),
			))
		}
	}
	if packet.Resume != nil {
		b.WriteString(fmt.Sprintf("- Resume: session=%s | %s\n",
			firstNonEmpty(packet.Resume.SessionID, "-"),
			firstNonEmpty(oneLine(packet.Resume.Summary), "no resume summary"),
		))
	}
	if packet.Decision != nil {
		b.WriteString(fmt.Sprintf("- Decision: needed_from=%s | type=%s\n",
			firstNonEmpty(packet.Decision.NeededFrom, "-"),
			firstNonEmpty(packet.Decision.DecisionType, "-"),
		))
	}
	if packet.Gate != nil {
		b.WriteString(fmt.Sprintf("- Gate: state=%s | type=%s | needed_from=%s | %s\n",
			firstNonEmpty(packet.Gate.GateState, "-"),
			firstNonEmpty(packet.Gate.GateType, "-"),
			firstNonEmpty(packet.Gate.NeededFrom, "-"),
			firstNonEmpty(oneLine(packet.Gate.Summary), "no summary"),
		))
	}
	if packet.Unblock != nil {
		b.WriteString(fmt.Sprintf("- Unblock: state=%s | trigger=%s | blocker_kinds=%s | %s\n",
			firstNonEmpty(packet.Unblock.UnblockState, "-"),
			firstNonEmpty(packet.Unblock.Trigger, "-"),
			joinOrDash(packet.Unblock.BlockerKinds, 3),
			firstNonEmpty(oneLine(packet.Unblock.Summary), "no summary"),
		))
	}
	if packet.Handoff != nil {
		b.WriteString(fmt.Sprintf("- Handoff: state=%s | to=%s | %s\n",
			firstNonEmpty(packet.Handoff.HandoffState, "-"),
			firstNonEmpty(packet.Handoff.ToAgentID, firstNonEmpty(packet.HandoffToAgentID, "-")),
			firstNonEmpty(oneLine(packet.Handoff.Summary), "no summary"),
		))
	}
	if len(packet.Blockers) > 0 {
		first := packet.Blockers[0]
		b.WriteString(fmt.Sprintf("- Blockers: %s\n", firstNonEmpty(oneLine(first.Kind+": "+first.Detail), oneLine(first.Detail), oneLine(first.Kind), "blocked")))
	}
	if len(packet.ContextHints.SuggestedDocKeys) > 0 || len(packet.ContextHints.RelatedArtifactRefs) > 0 {
		b.WriteString(fmt.Sprintf("- Context Hints: docs=%s | artifacts=%s\n",
			joinOrDash(packet.ContextHints.SuggestedDocKeys, 4),
			joinOrDash(packet.ContextHints.RelatedArtifactRefs, 4),
		))
	}
	if packet.PatchQueueSupersede != nil {
		pq := packet.PatchQueueSupersede
		b.WriteString(fmt.Sprintf("- Patch Queue Supersede: project_id=%s | queue_id=%s | item_id=%s | new_item_id=%s | branch_id=%s | head=%s | evidence_doc_key=%s\n",
			firstNonEmpty(pq.ProjectID, packet.ProjectID, "-"),
			firstNonEmpty(pq.QueueID, "-"),
			firstNonEmpty(pq.ItemID, "-"),
			firstNonEmpty(pq.NewItemID, "-"),
			firstNonEmpty(pq.BranchID, "-"),
			firstNonEmpty(pq.HeadSHA, "-"),
			firstNonEmpty(pq.EvidenceDocKey, "-"),
		))
	}
	if packet.PatchQueueClaim != nil {
		pq := packet.PatchQueueClaim
		claimState := "expired"
		if pq.ClaimActive {
			claimState = "active"
		}
		b.WriteString(fmt.Sprintf("- Patch Queue Claim Stewardship: project_id=%s | queue_id=%s | item_id=%s | branch_id=%s | head=%s | claimed_by=%s | claim=%s | operation_binding=%t | allowed=%s\n",
			firstNonEmpty(pq.ProjectID, packet.ProjectID, "-"),
			firstNonEmpty(pq.QueueID, "-"),
			firstNonEmpty(pq.ItemID, "-"),
			firstNonEmpty(pq.BranchID, "-"),
			firstNonEmpty(pq.HeadSHA, "-"),
			firstNonEmpty(pq.ClaimedBy, "-"),
			claimState,
			pq.OperationBindingPresent,
			joinOrDash(pq.AllowedActions, 8),
		))
	}
	if packet.Advisory != nil {
		b.WriteString(fmt.Sprintf("- Advisory Cluster: %s\n", firstNonEmpty(packet.Advisory.ProtoClusterID, "-")))
		if packet.Advisory.Control != nil {
			b.WriteString(fmt.Sprintf("- Advisory Control: band=%s | score=%d | %s\n",
				firstNonEmpty(packet.Advisory.Control.AttentionBand, "-"),
				packet.Advisory.Control.PressureScore,
				firstNonEmpty(oneLine(packet.Advisory.Control.Summary), "no summary"),
			))
		}
		if packet.Advisory.Corridor != nil {
			b.WriteString(fmt.Sprintf("- Advisory Corridor: readiness=%s | class=%s | catalog=%s\n",
				firstNonEmpty(packet.Advisory.Corridor.CorridorReadiness, "-"),
				firstNonEmpty(packet.Advisory.Corridor.TaskClassHint, "-"),
				firstNonEmpty(packet.Advisory.Corridor.CorridorCatalogHint, "-"),
			))
		}
		if len(packet.Advisory.Frontier) > 0 {
			item := packet.Advisory.Frontier[0]
			b.WriteString(fmt.Sprintf("- Advisory Frontier: %s | %s | %s\n",
				firstNonEmpty(item.TensionID, "-"),
				firstNonEmpty(item.TensionType, "-"),
				firstNonEmpty(oneLine(item.Title), oneLine(item.Summary), "no summary"),
			))
		}
	}
	return clipForPrompt(strings.TrimSpace(b.String()), maxChars)
}

func projectCoordinationPromptSummary(raw json.RawMessage, taskLimit int) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var coordination ProjectCoordinationRecord
	if err := json.Unmarshal(raw, &coordination); err != nil {
		return fmt.Sprintf("- Project Coordination: %s\n", truncate(oneLine(string(raw)), 300))
	}
	projectID := firstNonEmpty(coordination.Project.ProjectID, coordination.Profile.ProjectID, "-")
	phase := firstNonEmpty(coordination.GateStatus.CurrentPhase, coordination.Profile.CurrentPhase, "-")
	gate := firstNonEmpty(coordination.GateStatus.OverallState, "-")
	var b strings.Builder
	b.WriteString(fmt.Sprintf("- Project Coordination: project=%s | phase=%s | gate=%s | open_tasks=%d | tasks=%d | patch_queue=%d | checkouts=%d | branches=%d\n",
		projectID,
		phase,
		gate,
		coordination.OpenTaskCount,
		len(coordination.Tasks),
		len(coordination.PatchQueueItems),
		len(coordination.Checkouts),
		len(coordination.Branches),
	))
	if coordination.Profile.ProjectID != "" || coordination.Profile.RepoRequired || coordination.Profile.RepoStatus != "" || coordination.Profile.RepoURL != "" {
		b.WriteString(fmt.Sprintf("  - Project Profile: repo_required=%t | repo_status=%s | repo_url=%s | default_branch=%s | design=%s | implementation_plan=%s\n",
			coordination.Profile.RepoRequired,
			firstNonEmpty(coordination.Profile.RepoStatus, "-"),
			projectCoordinationPromptRef(coordination.Profile.RepoURL, 180),
			firstNonEmpty(coordination.Profile.RepoDefaultBranch, "-"),
			firstNonEmpty(coordination.Profile.DesignDocID, "-"),
			firstNonEmpty(coordination.Profile.ImplementationPlanDocID, "-"),
		))
		if projectID != "-" {
			b.WriteString(fmt.Sprintf("  - Planning Docs To Inspect: product_contract=project.%s.product_contract | acceptance_criteria=project.%s.acceptance_criteria | plan_review=project.%s.plan_review | reflection_board=project.%s.reflection_board\n",
				projectID, projectID, projectID, projectID))
		}
		if strings.EqualFold(strings.TrimSpace(coordination.Profile.RepoStatus), "READY") && strings.TrimSpace(coordination.Profile.RepoURL) != "" {
			b.WriteString("  - Registry Truth: repository is READY in current project coordination; older blocker memories or updates saying repo REQUESTED/materialization/payment-gated are stale unless an active gate in this packet says otherwise.\n")
		}
	}
	if len(coordination.Repositories) > 0 {
		b.WriteString("  - Repositories:\n")
		limit := len(coordination.Repositories)
		if limit > 4 {
			limit = 4
		}
		for _, repo := range coordination.Repositories[:limit] {
			b.WriteString(fmt.Sprintf("    - %s | canonical=%t | status=%s | remote=%s | branch=%s | updated_by=%s\n",
				firstNonEmpty(repo.RepoID, "repo"),
				repo.IsCanonical,
				firstNonEmpty(repo.RepoStatus, "-"),
				projectCoordinationPromptRef(repo.RemoteURL, 160),
				firstNonEmpty(repo.DefaultBranch, "-"),
				firstNonEmpty(repo.UpdatedBy, "-"),
			))
		}
		if len(coordination.Repositories) > limit {
			b.WriteString(fmt.Sprintf("    - ... %d more repositories omitted\n", len(coordination.Repositories)-limit))
		}
	}
	if len(coordination.Checkouts) > 0 {
		b.WriteString("  - Project Checkouts:\n")
		b.WriteString("    - ranking: non-main, headed, dirty, or task-bound checkouts are surfaced first for publication repair and provisional QA; canonical acceptance still requires patch queue/canonical evidence.\n")
		checkouts := projectCoordinationPromptRankedCheckouts(coordination.Checkouts, projectCoordinationCheckoutPromptLimit)
		for _, checkout := range checkouts {
			head := firstNonEmpty(checkout.HeadSHA, "-")
			if len(head) > 12 {
				head = head[:12]
			}
			b.WriteString(fmt.Sprintf("    - %s | agent=%s | status=%s | branch=%s | dirty=%s | task=%s | path=%s | path_ref=%s | head=%s | updated=%s\n",
				firstNonEmpty(checkout.CheckoutID, "checkout"),
				firstNonEmpty(checkout.AgentID, "-"),
				firstNonEmpty(checkout.Status, checkout.DerivedStatus, "-"),
				firstNonEmpty(checkout.BranchName, "-"),
				firstNonEmpty(checkout.DirtyState, "-"),
				firstNonEmpty(checkout.ActiveTaskID, "-"),
				projectCoordinationPromptRef(checkout.LocalPath, 180),
				projectCoordinationPromptLocalPathRef(checkout.LocalPath),
				head,
				firstNonEmpty(checkout.UpdatedAt, "-"),
			))
		}
		if len(coordination.Checkouts) > len(checkouts) {
			b.WriteString(fmt.Sprintf("    - ... %d lower-signal checkouts omitted\n", len(coordination.Checkouts)-len(checkouts)))
		}
	}
	if len(coordination.Branches) > 0 {
		b.WriteString("  - Project Branches:\n")
		branches := projectCoordinationPromptRankedBranches(coordination.Branches, projectCoordinationBranchPromptLimit)
		for _, branch := range branches {
			head := firstNonEmpty(branch.HeadSHA, "-")
			if len(head) > 12 {
				head = head[:12]
			}
			b.WriteString(fmt.Sprintf("    - %s | agent=%s | status=%s | branch=%s | task=%s | head=%s | review=%s | updated=%s\n",
				firstNonEmpty(branch.BranchID, "branch"),
				firstNonEmpty(branch.AgentID, "-"),
				firstNonEmpty(branch.Status, "-"),
				firstNonEmpty(branch.BranchName, "-"),
				firstNonEmpty(branch.ActiveTaskID, "-"),
				head,
				projectCoordinationPromptText(firstNonEmpty(branch.ReviewDocKey, "-"), 160),
				firstNonEmpty(branch.UpdatedAt, "-"),
			))
		}
		if len(coordination.Branches) > len(branches) {
			b.WriteString(fmt.Sprintf("    - ... %d lower-signal branches omitted\n", len(coordination.Branches)-len(branches)))
		}
	}
	if len(coordination.ServiceRuns) > 0 {
		b.WriteString("  - Service Runs:\n")
		runs := append([]ServiceRunRecord(nil), coordination.ServiceRuns...)
		sort.SliceStable(runs, func(i, j int) bool {
			return serviceRunPromptRank(runs[i].Status) < serviceRunPromptRank(runs[j].Status)
		})
		limit := len(runs)
		if limit > 6 {
			limit = 6
		}
		for _, run := range runs[:limit] {
			next := serviceRunPromptNextAction(run)
			b.WriteString(fmt.Sprintf("    - %s | status=%s | candidate=%s | deploy=%s | public_url=%s | budget_account=%s | cap=%d | credential_policy=%s | next=%s\n",
				firstNonEmpty(run.RunID, "service-run"),
				firstNonEmpty(run.Status, "-"),
				firstNonEmpty(run.CandidateID, "-"),
				firstNonEmpty(run.DeployTarget, "-"),
				projectCoordinationPromptRef(run.PublicURL, 160),
				firstNonEmpty(run.BudgetAccountID, "-"),
				run.BudgetCapMicros,
				firstNonEmpty(run.CredentialPolicy, "-"),
				next,
			))
		}
		if len(runs) > limit {
			b.WriteString(fmt.Sprintf("    - ... %d more service runs omitted\n", len(runs)-limit))
		}
	}
	if len(coordination.Tasks) > 0 {
		b.WriteString("  - Project Tasks:\n")
		limit := taskLimit
		if limit <= 0 || limit > len(coordination.Tasks) {
			limit = len(coordination.Tasks)
		}
		for _, task := range coordination.Tasks[:limit] {
			b.WriteString(fmt.Sprintf("    - %s | %s | lane=%s | priority=%s | claim=%s | %s\n",
				firstNonEmpty(task.TaskID, "task"),
				firstNonEmpty(task.Status, "-"),
				firstNonEmpty(task.ProjectLane, "-"),
				firstNonEmpty(task.Priority, "-"),
				projectCoordinationTaskClaimSummary(task),
				projectCoordinationPromptText(firstNonEmpty(task.Title, task.Description, "no title"), 140),
			))
		}
		if len(coordination.Tasks) > limit {
			b.WriteString(fmt.Sprintf("    - ... %d more project tasks omitted\n", len(coordination.Tasks)-limit))
		}
	}
	if len(coordination.PatchQueueItems) > 0 {
		b.WriteString("  - Patch Queue:\n")
		branchesByID := map[string]ProjectBranchRecord{}
		for _, branch := range coordination.Branches {
			if branchID := strings.TrimSpace(branch.BranchID); branchID != "" {
				branchesByID[branchID] = branch
			}
		}
		limit := len(coordination.PatchQueueItems)
		if limit > 4 {
			limit = 4
		}
		for _, item := range coordination.PatchQueueItems[:limit] {
			b.WriteString(fmt.Sprintf("    - %s/%s | %s | branch=%s | claimed_by=%s | decision=%s\n",
				firstNonEmpty(item.QueueID, "queue"),
				firstNonEmpty(item.ItemID, "item"),
				firstNonEmpty(item.State, "-"),
				firstNonEmpty(item.BranchID, "-"),
				firstNonEmpty(item.ClaimedBy, "-"),
				projectCoordinationPromptText(firstNonEmpty(item.DecisionSummary, "-"), 160),
			))
			if strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
				head := firstNonEmpty(strings.TrimSpace(item.HeadSHA), "-")
				if len(head) > 12 {
					head = head[:12]
				}
				if acceptedPatchQueueItemNeedsIntegration(item, branchesByID) {
					b.WriteString(fmt.Sprintf("      next: ACCEPTED is reviewed evidence, not canonical baseline; use project_patch_queue_integrate before treating default branch as product truth | head=%s\n", head))
				} else {
					b.WriteString(fmt.Sprintf("      next: ACCEPTED item has no visible unmerged source branch in current coordination; inspect the canonical target branch and create concrete validation/improvement follow-ups instead of reintegrating this item | head=%s\n", head))
				}
			}
		}
		if len(coordination.PatchQueueItems) > limit {
			b.WriteString(fmt.Sprintf("    - ... %d more patch queue items omitted\n", len(coordination.PatchQueueItems)-limit))
		}
	}
	return b.String()
}

func projectCoordinationPromptRankedCheckouts(checkouts []ProjectCheckoutRecord, limit int) []ProjectCheckoutRecord {
	ranked := append([]ProjectCheckoutRecord(nil), checkouts...)
	sort.SliceStable(ranked, func(i, j int) bool {
		leftScore := projectCoordinationCheckoutPromptScore(ranked[i])
		rightScore := projectCoordinationCheckoutPromptScore(ranked[j])
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		leftUpdated := strings.TrimSpace(ranked[i].UpdatedAt)
		rightUpdated := strings.TrimSpace(ranked[j].UpdatedAt)
		if leftUpdated != rightUpdated {
			return leftUpdated > rightUpdated
		}
		return strings.TrimSpace(ranked[i].CheckoutID) < strings.TrimSpace(ranked[j].CheckoutID)
	})
	if limit <= 0 || limit >= len(ranked) {
		return ranked
	}
	return ranked[:limit]
}

func projectCoordinationCheckoutPromptScore(checkout ProjectCheckoutRecord) int {
	score := 0
	switch strings.ToUpper(strings.TrimSpace(firstNonEmpty(checkout.Status, checkout.DerivedStatus))) {
	case "ACTIVE", "READY", "READY_FOR_REVIEW", "RESERVED", "BLOCKED":
		score += 100
	case "ABANDONED", "ARCHIVED", "CANCELLED", "CLOSED", "INTEGRATED", "MERGED":
		score -= 50
	}
	if branch := strings.TrimSpace(checkout.BranchName); branch != "" && !projectCoordinationDefaultBranch(branch) {
		score += 90
	}
	if strings.TrimSpace(checkout.HeadSHA) != "" {
		score += 80
	}
	dirty := strings.ToUpper(strings.TrimSpace(checkout.DirtyState))
	if dirty != "" && dirty != "CLEAN" && dirty != "UNKNOWN" {
		score += 70
	}
	if strings.TrimSpace(checkout.ActiveTaskID) != "" {
		score += 60
	}
	switch strings.ToLower(strings.TrimSpace(checkout.CheckoutKind)) {
	case "implementation", "execution", "authoring":
		score += 30
	case "review", "validation":
		score -= 15
	}
	if strings.TrimSpace(checkout.LocalPath) != "" {
		score += 10
	}
	return score
}

func projectCoordinationPromptRankedBranches(branches []ProjectBranchRecord, limit int) []ProjectBranchRecord {
	ranked := append([]ProjectBranchRecord(nil), branches...)
	sort.SliceStable(ranked, func(i, j int) bool {
		leftScore := projectCoordinationBranchPromptScore(ranked[i])
		rightScore := projectCoordinationBranchPromptScore(ranked[j])
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		leftUpdated := strings.TrimSpace(ranked[i].UpdatedAt)
		rightUpdated := strings.TrimSpace(ranked[j].UpdatedAt)
		if leftUpdated != rightUpdated {
			return leftUpdated > rightUpdated
		}
		return strings.TrimSpace(ranked[i].BranchID) < strings.TrimSpace(ranked[j].BranchID)
	})
	if limit <= 0 || limit >= len(ranked) {
		return ranked
	}
	return ranked[:limit]
}

func projectCoordinationBranchPromptScore(branch ProjectBranchRecord) int {
	score := 0
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case "READY_FOR_REVIEW":
		score += 140
	case "ACTIVE", "RESERVED", "BLOCKED":
		score += 110
	case "ACCEPTED":
		score += 100
	case "ABANDONED", "ARCHIVED", "CANCELLED", "CLOSED", "INTEGRATED", "MERGED":
		score -= 50
	}
	if branchName := strings.TrimSpace(branch.BranchName); branchName != "" && !projectCoordinationDefaultBranch(branchName) {
		score += 70
	}
	if strings.TrimSpace(branch.HeadSHA) != "" {
		score += 80
	}
	if strings.TrimSpace(branch.ReviewDocKey) != "" {
		score += 50
	}
	if strings.TrimSpace(branch.ActiveTaskID) != "" {
		score += 40
	}
	if strings.TrimSpace(branch.CheckoutID) != "" {
		score += 20
	}
	return score
}

func projectCoordinationDefaultBranch(branch string) bool {
	switch strings.ToLower(strings.TrimSpace(branch)) {
	case "", "default", "main", "master", "trunk":
		return true
	default:
		return false
	}
}

func serviceRunPromptRank(status string) int {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE", "DEPLOYED", "MEASURING", "BLOCKED":
		return 0
	case "PLANNED":
		return 1
	case "COMPLETED", "KILLED", "CANCELLED":
		return 3
	default:
		return 2
	}
}

func serviceRunPromptNextAction(run ServiceRunRecord) string {
	switch strings.ToUpper(strings.TrimSpace(run.Status)) {
	case "PLANNED":
		return "bootstrap/decompose project tasks"
	case "ACTIVE":
		return "drive implementation/deploy evidence"
	case "DEPLOYED":
		return "collect public health/analytics/spend evidence"
	case "MEASURING":
		return "continue measurement or create iteration tasks"
	case "BLOCKED":
		return "resolve blocker or record kill/hold evidence"
	case "COMPLETED":
		return "inspect outcome and consider next candidate"
	case "KILLED", "CANCELLED":
		return "record learning and consider next candidate"
	default:
		return "inspect service.coordination.get"
	}
}

func acceptedPatchQueueItemNeedsIntegration(item ProjectPatchQueueItemRecord, branchesByID map[string]ProjectBranchRecord) bool {
	if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
		return false
	}
	branchID := strings.TrimSpace(item.BranchID)
	if branchID == "" {
		return false
	}
	branch, ok := branchesByID[branchID]
	if !ok || strings.TrimSpace(branch.BranchID) == "" {
		return false
	}
	return !projectBranchLooksIntegrated(branch)
}

func projectCoordinationTaskClaimSummary(task WorkspaceTaskRecord) string {
	if task.ClaimAgentID == nil || strings.TrimSpace(*task.ClaimAgentID) == "" {
		return "-"
	}
	status := "-"
	if task.ClaimStatus != nil && strings.TrimSpace(*task.ClaimStatus) != "" {
		status = strings.TrimSpace(*task.ClaimStatus)
	}
	return fmt.Sprintf("%s/%s", strings.TrimSpace(*task.ClaimAgentID), status)
}

func projectCoordinationPromptText(value string, limit int) string {
	value = oneLine(strings.TrimSpace(value))
	if value == "" {
		return "-"
	}
	return truncate(value, limit)
}

func projectCoordinationPromptRef(value string, limit int) string {
	value = oneLine(strings.TrimSpace(value))
	if value == "" {
		return "-"
	}
	if projectCoordinationLooksLocalRef(value) {
		return "[local-ref-redacted]"
	}
	return truncate(value, limit)
}

func projectCoordinationPromptLocalPathRef(value string) string {
	if !projectCoordinationLooksLocalRef(value) {
		return "-"
	}
	ref := internalHeartbeatLocalPathRef(value)
	if strings.TrimSpace(ref) == "" {
		return "<local-checkout>"
	}
	return ref
}

func projectCoordinationLooksLocalRef(value string) bool {
	trimmed := strings.TrimSpace(value)
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "file://") {
		return true
	}
	if strings.HasPrefix(trimmed, "/") || strings.HasPrefix(trimmed, `\\`) {
		return true
	}
	if len(trimmed) >= 3 && ((trimmed[0] >= 'A' && trimmed[0] <= 'Z') || (trimmed[0] >= 'a' && trimmed[0] <= 'z')) && trimmed[1] == ':' && (trimmed[2] == '\\' || trimmed[2] == '/') {
		return true
	}
	return false
}

func projectGateBoolLabel(value *bool) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprint(*value)
}

// writeDietedDocs renders coordination doc bodies under the TE-30 Packet
// Prompt Diet. Docs arrive already ordered by relevance (prioritizedDocs).
// Bodies are capped two ways:
//   - per-doc: a body longer than promptDocBodyCapChars is replaced by a head
//     excerpt + its byte length + the canonical doc key + a one-line
//     instruction to fetch the full body with the read-only workspace_doc_get
//     tool.
//   - cumulative: once promptDocBudgetCapChars of body bytes have been emitted,
//     remaining docs are listed as doc-key references only (no body), still
//     pointing at workspace_doc_get.
//
// The per-session byte cost is bounded without dropping any doc key, so an
// agent can always recover the full body on demand. Doc bodies are not
// coordination source-of-truth, so capping them is safety-preserving.
func writeDietedDocs(b *strings.Builder, docs []WorkspaceDocRecord, cfg RuntimeConfig) {
	bodyCap := promptDocBodyCapChars
	if cfg.MaxPromptDocChars > 0 && cfg.MaxPromptDocChars/2 < bodyCap {
		bodyCap = cfg.MaxPromptDocChars / 2
	}
	budget := promptDocBudgetCapChars
	if cfg.MaxPromptDocChars > 0 && cfg.MaxPromptDocChars < budget {
		budget = cfg.MaxPromptDocChars
	}
	spent := 0
	for _, doc := range docs {
		body := strings.TrimSpace(doc.Content)
		title := firstNonEmpty(doc.Title, doc.DocKey, "doc")
		docKey := strings.TrimSpace(doc.DocKey)
		if body == "" {
			continue
		}
		b.WriteString("### Doc: ")
		b.WriteString(title)
		b.WriteString("\n")
		if spent >= budget {
			// Docs budget exhausted: reference-only, no body bytes.
			b.WriteString(promptDocFetchReference(docKey, len(body), 0))
			b.WriteString("\n\n")
			continue
		}
		emitCap := bodyCap
		if remaining := budget - spent; remaining < emitCap {
			emitCap = remaining
		}
		if len(body) <= emitCap {
			b.WriteString(body)
			b.WriteString("\n\n")
			spent += len(body)
			continue
		}
		head := promptDocBodyExcerptHead
		if head > emitCap {
			head = emitCap
		}
		excerpt := strings.TrimSpace(body[:head])
		b.WriteString(excerpt)
		b.WriteString("\n")
		b.WriteString(promptDocFetchReference(docKey, len(body), head))
		b.WriteString("\n\n")
		spent += head
	}
}

// promptDocFetchReference renders the diet reference line for a capped doc
// body: total body byte length, the canonical doc key, and a one-line
// instruction to fetch the full body via the read-only workspace_doc_get tool.
func promptDocFetchReference(docKey string, bodyBytes, excerptBytes int) string {
	key := strings.TrimSpace(docKey)
	if key == "" {
		key = "(no doc_key)"
	}
	if excerptBytes <= 0 {
		return fmt.Sprintf("[doc body diet] body=%d bytes omitted | doc_key=%s | fetch full body with workspace_doc_get doc_key=%s",
			bodyBytes, key, key)
	}
	return fmt.Sprintf("...[doc body diet] showing first %d of %d bytes | doc_key=%s | fetch full body with workspace_doc_get doc_key=%s",
		excerptBytes, bodyBytes, key, key)
}

func prioritizedDocs(docs []WorkspaceDocRecord, task *WorkspaceTaskRecord) []WorkspaceDocRecord {
	if len(docs) == 0 {
		return nil
	}
	primaryKeys := []string{}
	if task != nil && strings.TrimSpace(task.TaskID) != "" {
		primaryKeys = append(primaryKeys, defaultHydrationTaskDocKeys(task.TaskID)...)
	}
	genericKeys := []string{
		"charter",
		"current_context",
		"decisions",
		"open_questions",
		"handoff",
		"tooling",
		"autonomy_policy",
	}

	prioritized := make([]WorkspaceDocRecord, 0, len(docs))
	seen := map[string]struct{}{}
	appendExactKeys := func(keys []string) {
		for _, key := range keys {
			for _, doc := range docs {
				docKey := strings.TrimSpace(strings.ToLower(doc.DocKey))
				if docKey != strings.ToLower(key) {
					continue
				}
				if _, ok := seen[doc.DocKey]; ok {
					continue
				}
				seen[doc.DocKey] = struct{}{}
				prioritized = append(prioritized, doc)
			}
		}
	}
	appendExactKeys(primaryKeys)
	for _, doc := range docs {
		if _, ok := seen[doc.DocKey]; ok {
			continue
		}
		if !isPromptEvidenceDocKey(doc.DocKey) {
			continue
		}
		if !promptEvidenceDocMatchesTaskProject(doc.DocKey, task) {
			continue
		}
		seen[doc.DocKey] = struct{}{}
		prioritized = append(prioritized, doc)
	}
	appendExactKeys(genericKeys)
	for _, doc := range docs {
		if _, ok := seen[doc.DocKey]; ok {
			continue
		}
		if !promptEvidenceDocMatchesTaskProject(doc.DocKey, task) {
			continue
		}
		prioritized = append(prioritized, doc)
	}
	return prioritized
}

func isPromptEvidenceDocKey(docKey string) bool {
	docKey = strings.ToLower(strings.TrimSpace(docKey))
	if strings.HasPrefix(docKey, "project.") {
		for _, suffix := range []string{".acceptance_criteria", ".product_contract", ".plan_review", ".planning_review", ".strategy_review", ".reflection_board", ".quality_reflection"} {
			if strings.HasSuffix(docKey, suffix) {
				return true
			}
		}
	}
	for _, suffix := range []string{".result", ".evidence_gap", ".review_feedback", ".implementation_evidence", ".verification"} {
		if strings.HasPrefix(docKey, "task.") && strings.HasSuffix(docKey, suffix) {
			return true
		}
	}
	return false
}

func promptEvidenceDocMatchesTaskProject(docKey string, task *WorkspaceTaskRecord) bool {
	projectID, _, ok := parseProjectPromptDocKey(docKey)
	if !ok {
		return true
	}
	if task == nil || strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	return sameWorkspaceDocProjectID(projectID, task.ProjectID)
}

func parseProjectPromptDocKey(docKey string) (projectID, suffix string, ok bool) {
	parts := strings.Split(strings.TrimSpace(docKey), ".")
	if len(parts) < 3 || !strings.EqualFold(parts[0], "project") {
		return "", "", false
	}
	projectID = strings.TrimSpace(parts[1])
	suffix = strings.TrimSpace(strings.Join(parts[2:], "."))
	return projectID, suffix, projectID != "" && suffix != ""
}

func clipForPrompt(value string, max int) string {
	value = strings.TrimSpace(value)
	if max <= 0 || len(value) <= max {
		return value
	}
	return strings.TrimSpace(value[:max]) + "\n...[truncated]"
}

func joinOrDash(values []string, limit int) string {
	if len(values) == 0 {
		return "-"
	}
	if limit <= 0 || len(values) <= limit {
		return strings.Join(values, ", ")
	}
	return strings.Join(values[:limit], ", ") + ", ..."
}

func summarizeControlMaps(items []map[string]any, limit int) string {
	if len(items) == 0 {
		return ""
	}
	parts := make([]string, 0, min(len(items), limit))
	for _, item := range items {
		if limit > 0 && len(parts) >= limit {
			break
		}
		label := firstString(item, "summary", "title", "signal_type", "control_type", "type")
		if label == "" {
			continue
		}
		parts = append(parts, oneLine(label))
	}
	return strings.Join(parts, "; ")
}

func firstString(item map[string]any, keys ...string) string {
	for _, key := range keys {
		raw, ok := item[key]
		if !ok {
			continue
		}
		if text, ok := raw.(string); ok && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
