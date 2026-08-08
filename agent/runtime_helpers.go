package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func boolPtr(v bool) *bool {
	return &v
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstPositiveInt(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func firstPositiveInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func envInt64(name string) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func parseRuntimeBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func isProfileGateClosedWorkNext(work AgentWorkNextResult) bool {
	return work.ProfileGateBlockedWork || strings.EqualFold(strings.TrimSpace(work.Reason), "profile_gate_closed")
}

func isClosedGateWorkNext(work AgentWorkNextResult) bool {
	if isProfileGateClosedWorkNext(work) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(work.Reason), "project_gate_closed") {
		return true
	}
	return work.Packet != nil && work.Packet.Gate != nil && strings.TrimSpace(work.Packet.Gate.GateState) != ""
}

func closedGateWorkPacket(work AgentWorkNextResult) *AgentWorkPacket {
	if isProfileGateClosedWorkNext(work) {
		return profileGateClosedWorkPacket(work)
	}
	packet := cloneAgentWorkPacket(work.Packet)
	if packet == nil {
		packet = &AgentWorkPacket{}
	}
	packet.WorkType = firstNonEmpty(packet.WorkType, work.Reason, "gate_closed")
	packet.CoordinationState = firstNonEmpty(packet.CoordinationState, work.Reason, "gate_closed")
	packet.PreferredTransition = firstNonEmpty(packet.PreferredTransition, "resolve_gate")
	packet.WhyNow = firstNonEmpty(packet.WhyNow, work.Reason)
	if packet.Gate == nil {
		packet.Gate = &AgentWorkGate{}
	}
	packet.Gate.GateState = firstNonEmpty(packet.Gate.GateState, "closed")
	packet.Gate.GateType = firstNonEmpty(packet.Gate.GateType, work.Reason, "work_gate")
	packet.Gate.Summary = firstNonEmpty(packet.Gate.Summary, work.Reason)
	return packet
}

func closedGateWorkReason(work AgentWorkNextResult, packet *AgentWorkPacket) string {
	if isProfileGateClosedWorkNext(work) {
		return firstNonEmpty(work.Reason, "profile_gate_closed")
	}
	if packet != nil && packet.Gate != nil {
		return firstNonEmpty(work.Reason, packet.Gate.GateType, "gate_closed")
	}
	return firstNonEmpty(work.Reason, "gate_closed")
}

func closedGateWorkSummary(work AgentWorkNextResult, packet *AgentWorkPacket) string {
	if isProfileGateClosedWorkNext(work) {
		return firstNonEmpty(work.ProfileGateSummary, work.ProfileGateReason, work.Reason)
	}
	if packet != nil && packet.Gate != nil {
		return firstNonEmpty(packet.Gate.Summary, packet.WhyNow, work.Reason)
	}
	return firstNonEmpty(work.Reason, "gate closed")
}

func profileGateClosedWorkPacket(work AgentWorkNextResult) *AgentWorkPacket {
	packet := cloneAgentWorkPacket(work.Packet)
	if packet == nil {
		packet = &AgentWorkPacket{}
	}
	packet.WorkType = firstNonEmpty(packet.WorkType, "profile_gate_closed")
	packet.CoordinationState = firstNonEmpty(packet.CoordinationState, "profile_gate_closed")
	packet.PreferredTransition = firstNonEmpty(packet.PreferredTransition, "agent_profile_update")
	packet.WhyNow = firstNonEmpty(packet.WhyNow, work.ProfileGateReason, work.Reason)
	if packet.Gate == nil {
		packet.Gate = &AgentWorkGate{}
	}
	packet.Gate.GateState = firstNonEmpty(packet.Gate.GateState, "closed")
	packet.Gate.GateType = firstNonEmpty(packet.Gate.GateType, "profile_autonomous_execution")
	packet.Gate.NeededFrom = firstNonEmpty(packet.Gate.NeededFrom, "agent.profile.update")
	packet.Gate.Summary = firstNonEmpty(packet.Gate.Summary, work.ProfileGateSummary, work.ProfileGateReason)
	return packet
}

func cloneAgentWorkPacket(packet *AgentWorkPacket) *AgentWorkPacket {
	if packet == nil {
		return nil
	}
	copyPacket := *packet
	if packet.Resume != nil {
		resume := *packet.Resume
		copyPacket.Resume = &resume
	}
	if packet.Decision != nil {
		decision := *packet.Decision
		copyPacket.Decision = &decision
	}
	if packet.Gate != nil {
		gate := *packet.Gate
		copyPacket.Gate = &gate
	}
	if packet.Unblock != nil {
		unblock := *packet.Unblock
		copyPacket.Unblock = &unblock
		copyPacket.Unblock.BlockerKinds = append([]string(nil), packet.Unblock.BlockerKinds...)
	}
	if packet.Handoff != nil {
		handoff := *packet.Handoff
		copyPacket.Handoff = &handoff
	}
	if packet.OwnerBound != nil {
		ownerBound := *packet.OwnerBound
		copyPacket.OwnerBound = &ownerBound
	}
	if packet.PatchQueueSupersede != nil {
		supersede := *packet.PatchQueueSupersede
		copyPacket.PatchQueueSupersede = &supersede
	}
	if packet.PatchQueueClaim != nil {
		claim := *packet.PatchQueueClaim
		claim.AllowedActions = append([]string(nil), packet.PatchQueueClaim.AllowedActions...)
		copyPacket.PatchQueueClaim = &claim
	}
	if packet.Frontier != nil {
		frontier := *packet.Frontier
		frontier.Candidates = append([]AgentWorkTaskFrontierCandidate(nil), packet.Frontier.Candidates...)
		for i := range frontier.Candidates {
			frontier.Candidates[i].Fit.Reasons = append([]string(nil), packet.Frontier.Candidates[i].Fit.Reasons...)
			frontier.Candidates[i].Fit.RequiredWorkModes = append([]string(nil), packet.Frontier.Candidates[i].Fit.RequiredWorkModes...)
			frontier.Candidates[i].Fit.PreferredWorkModes = append([]string(nil), packet.Frontier.Candidates[i].Fit.PreferredWorkModes...)
			frontier.Candidates[i].Fit.PreferredSkills = append([]string(nil), packet.Frontier.Candidates[i].Fit.PreferredSkills...)
			frontier.Candidates[i].Fit.PreferredTools = append([]string(nil), packet.Frontier.Candidates[i].Fit.PreferredTools...)
			frontier.Candidates[i].Fit.AdvisoryRoleTypes = append([]string(nil), packet.Frontier.Candidates[i].Fit.AdvisoryRoleTypes...)
		}
		frontier.Roster = append([]AgentWorkRosterAgent(nil), packet.Frontier.Roster...)
		for i := range frontier.Roster {
			frontier.Roster[i].CurrentTaskIDs = append([]string(nil), packet.Frontier.Roster[i].CurrentTaskIDs...)
			frontier.Roster[i].Capabilities = append([]string(nil), packet.Frontier.Roster[i].Capabilities...)
			frontier.Roster[i].ProfileTags = append([]string(nil), packet.Frontier.Roster[i].ProfileTags...)
			frontier.Roster[i].ToolsAccess = append([]string(nil), packet.Frontier.Roster[i].ToolsAccess...)
			frontier.Roster[i].ActiveTasks = append([]AgentCurrentTask(nil), packet.Frontier.Roster[i].ActiveTasks...)
		}
		copyPacket.Frontier = &frontier
	}
	if packet.RequiresProjectGate != nil {
		requiresProjectGate := *packet.RequiresProjectGate
		copyPacket.RequiresProjectGate = &requiresProjectGate
	}
	copyPacket.Blockers = append([]BlockedRef(nil), packet.Blockers...)
	copyPacket.ProjectGateBlock = append([]byte(nil), packet.ProjectGateBlock...)
	copyPacket.ProjectCoordination = append([]byte(nil), packet.ProjectCoordination...)
	copyPacket.ContextHints.SuggestedDocKeys = append([]string(nil), packet.ContextHints.SuggestedDocKeys...)
	copyPacket.ContextHints.RelatedArtifactRefs = append([]string(nil), packet.ContextHints.RelatedArtifactRefs...)
	copyPacket.ContextHints.AnchorTaskIDs = append([]string(nil), packet.ContextHints.AnchorTaskIDs...)
	copyPacket.ContextHints.AnchorConflictTaskIDs = append([]string(nil), packet.ContextHints.AnchorConflictTaskIDs...)
	copyPacket.ContextHints.AnchorBranchIDs = append([]string(nil), packet.ContextHints.AnchorBranchIDs...)
	copyPacket.ContextHints.AnchorSessionIDs = append([]string(nil), packet.ContextHints.AnchorSessionIDs...)
	if packet.Advisory != nil {
		advisory := *packet.Advisory
		copyPacket.Advisory = &advisory
		if packet.Advisory.Control != nil {
			control := *packet.Advisory.Control
			copyPacket.Advisory.Control = &control
		}
		if packet.Advisory.Corridor != nil {
			corridor := *packet.Advisory.Corridor
			copyPacket.Advisory.Corridor = &corridor
		}
		copyPacket.Advisory.Frontier = append([]TensionFrontierItem(nil), packet.Advisory.Frontier...)
	}
	return &copyPacket
}

func normalizeOutcome(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "completed", "complete", "done", "resolved", "resolve", "success", "succeeded":
		return "completed"
	case "blocked":
		return "blocked"
	case "continue":
		return "continue"
	case "failed":
		return "failed"
	default:
		return "continue"
	}
}

var structuredTaskResultAllowedFields = map[string]struct{}{
	"outcome":        {},
	"summary":        {},
	"details":        {},
	"next_action":    {},
	"reflection":     {},
	"requires_human": {},
	"owner_action":   {},
	"human_reason":   {},
	"decision_type":  {},
	"blocked_on":     {},
	"memory_title":   {},
	"memory_body":    {},
	"memory_type":    {},
	"materialize":    {},
}

var structuredTaskResultMaterializeFields = map[string]struct{}{
	"doc_key":     {},
	"doc_title":   {},
	"doc_content": {},
}

var structuredTaskResultReflectionFields = map[string]struct{}{
	"current_intent":    {},
	"fresh_evidence":    {},
	"blocker_freshness": {},
	"next_useful_move":  {},
}

var structuredTaskResultBlockedRefFields = map[string]struct{}{
	"kind":   {},
	"detail": {},
}

func normalizeStructuredTaskResult(result *StructuredTaskResult) {
	if result == nil {
		return
	}
	result.Outcome = normalizeOutcome(result.Outcome)
	result.Summary = strings.TrimSpace(result.Summary)
	result.Details = strings.TrimSpace(result.Details)
	result.NextAction = strings.TrimSpace(result.NextAction)
	if result.Reflection != nil {
		result.Reflection.CurrentIntent = strings.TrimSpace(result.Reflection.CurrentIntent)
		result.Reflection.FreshEvidence = strings.TrimSpace(result.Reflection.FreshEvidence)
		result.Reflection.BlockerFreshness = strings.TrimSpace(result.Reflection.BlockerFreshness)
		result.Reflection.NextUsefulMove = strings.TrimSpace(result.Reflection.NextUsefulMove)
		if result.Reflection.CurrentIntent == "" &&
			result.Reflection.FreshEvidence == "" &&
			result.Reflection.BlockerFreshness == "" &&
			result.Reflection.NextUsefulMove == "" {
			result.Reflection = nil
		}
	}
	result.OwnerAction = strings.TrimSpace(result.OwnerAction)
	result.HumanReason = strings.TrimSpace(result.HumanReason)
	result.DecisionType = strings.TrimSpace(result.DecisionType)
	result.MemoryTitle = strings.TrimSpace(result.MemoryTitle)
	result.MemoryBody = strings.TrimSpace(result.MemoryBody)
	result.MemoryType = strings.TrimSpace(result.MemoryType)
	result.Materialize.DocKey = strings.TrimSpace(result.Materialize.DocKey)
	result.Materialize.DocTitle = strings.TrimSpace(result.Materialize.DocTitle)
	result.Materialize.DocContent = strings.TrimSpace(result.Materialize.DocContent)
	if result.Summary == "" {
		result.Summary = firstNonEmpty(result.Details, "task cycle finished")
	}
}

func isNullJSON(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func validateStructuredTaskResultObject(raw json.RawMessage, allowed map[string]struct{}, fieldName string) error {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("invalid %s: %w", fieldName, err)
	}
	for key := range envelope {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("unexpected %s field %q", fieldName, key)
		}
	}
	return nil
}

func parseStructuredTaskResultStrict(raw string) (StructuredTaskResult, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return StructuredTaskResult{}, fmt.Errorf("empty structured task result")
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &envelope); err != nil {
		return StructuredTaskResult{}, err
	}
	for key := range envelope {
		if _, ok := structuredTaskResultAllowedFields[key]; !ok {
			return StructuredTaskResult{}, fmt.Errorf("unexpected structured task result field %q", key)
		}
	}

	if rawMaterialize, ok := envelope["materialize"]; ok && !isNullJSON(rawMaterialize) {
		if err := validateStructuredTaskResultObject(rawMaterialize, structuredTaskResultMaterializeFields, "materialize"); err != nil {
			return StructuredTaskResult{}, err
		}
	}
	if rawReflection, ok := envelope["reflection"]; ok && !isNullJSON(rawReflection) {
		if err := validateStructuredTaskResultObject(rawReflection, structuredTaskResultReflectionFields, "reflection"); err != nil {
			return StructuredTaskResult{}, err
		}
	}
	if rawBlockedOn, ok := envelope["blocked_on"]; ok && !isNullJSON(rawBlockedOn) {
		var items []map[string]json.RawMessage
		if err := json.Unmarshal(rawBlockedOn, &items); err != nil {
			return StructuredTaskResult{}, fmt.Errorf("invalid blocked_on: %w", err)
		}
		for idx, item := range items {
			for key := range item {
				if _, ok := structuredTaskResultBlockedRefFields[key]; !ok {
					return StructuredTaskResult{}, fmt.Errorf("unexpected blocked_on[%d] field %q", idx, key)
				}
			}
		}
	}

	var result StructuredTaskResult
	if err := json.Unmarshal([]byte(trimmed), &result); err != nil {
		return StructuredTaskResult{}, err
	}
	normalizeStructuredTaskResult(&result)
	return result, nil
}

func parseStructuredTaskResultStrictOrFirstObject(raw string) (StructuredTaskResult, bool, error) {
	result, strictErr := parseStructuredTaskResultStrict(raw)
	if strictErr == nil {
		return result, false, nil
	}
	var salvaged []StructuredTaskResult
	for _, candidate := range extractJSONObjects(raw) {
		if strings.TrimSpace(candidate) == strings.TrimSpace(raw) {
			continue
		}
		parsed, err := parseStructuredTaskResultStrict(candidate)
		if err != nil {
			continue
		}
		if !structuredTaskResultSalvageCandidate(candidate, parsed) {
			continue
		}
		salvaged = append(salvaged, parsed)
	}
	switch len(salvaged) {
	case 0:
		return StructuredTaskResult{}, false, strictErr
	case 1:
		return salvaged[0], true, nil
	default:
		return StructuredTaskResult{}, false, fmt.Errorf("ambiguous structured task result: found %d valid JSON objects after strict parse failed: %w", len(salvaged), strictErr)
	}
}

func extractFirstJSONObject(raw string) (string, bool) {
	candidates := extractJSONObjects(raw)
	if len(candidates) == 0 {
		return "", false
	}
	return candidates[0], true
}

func extractJSONObjects(raw string) []string {
	trimmed := strings.TrimSpace(raw)
	var out []string
	for searchStart := 0; searchStart < len(trimmed); {
		relStart := strings.IndexByte(trimmed[searchStart:], '{')
		if relStart < 0 {
			break
		}
		start := searchStart + relStart
		end, ok := matchingJSONObjectEnd(trimmed, start)
		if ok {
			out = append(out, trimmed[start:end])
			searchStart = end
			continue
		}
		searchStart = start + 1
	}
	return out
}

func matchingJSONObjectEnd(raw string, start int) (int, bool) {
	if start < 0 || start >= len(raw) || raw[start] != '{' {
		return 0, false
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(raw); i++ {
		c := raw[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i + 1, true
			}
		}
	}
	return 0, false
}

func structuredTaskResultSalvageCandidate(raw string, result StructuredTaskResult) bool {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &envelope); err != nil {
		return false
	}
	for _, field := range []string{"outcome", "summary", "details", "next_action", "owner_action", "human_reason", "decision_type", "memory_title", "memory_body", "memory_type"} {
		if nonEmptyJSONString(envelope[field]) {
			return true
		}
	}
	if result.Reflection != nil {
		return true
	}
	if len(result.BlockedOn) > 0 {
		return true
	}
	return strings.TrimSpace(result.Materialize.DocKey) != "" ||
		strings.TrimSpace(result.Materialize.DocTitle) != "" ||
		strings.TrimSpace(result.Materialize.DocContent) != ""
}

func nonEmptyJSONString(raw json.RawMessage) bool {
	if isNullJSON(raw) {
		return false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	return strings.TrimSpace(value) != ""
}

func parseStructuredTaskResult(raw string) StructuredTaskResult {
	var result StructuredTaskResult
	if parsed, err := parseStructuredTaskResultStrict(raw); err == nil {
		return parsed
	}

	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &result); err == nil {
		normalizeStructuredTaskResult(&result)
		return result
	}

	return StructuredTaskResult{
		Outcome: "continue",
		Summary: firstNonEmpty(raw, "task cycle finished"),
		Details: strings.TrimSpace(raw),
	}
}

func isSessionRunnable(session *AgentSessionStateRecord) bool {
	if session == nil {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(session.Status)) {
	case "BLOCKED", "WAITING_DECISION", "HANDOFF_PENDING", "ENDED":
		return false
	default:
		return true
	}
}

func isClaimOwnedBy(task WorkspaceTaskRecord, agentID string) bool {
	return strings.TrimSpace(pointerValue(task.ClaimAgentID)) == strings.TrimSpace(agentID)
}

func taskClaimStatus(task WorkspaceTaskRecord) string {
	return strings.ToUpper(strings.TrimSpace(pointerValue(task.ClaimStatus)))
}

func hasClaimedOwnership(task WorkspaceTaskRecord, agentID string) bool {
	return isClaimOwnedBy(task, agentID) && taskClaimStatus(task) == "CLAIMED"
}

func taskClaimStatusIsActiveOwnership(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "CLAIMED", "RUNNING", "ACTIVE", "BLOCKED":
		return true
	default:
		return false
	}
}

func taskClaimStatusIsActiveAuthorityTransitionOwnership(task WorkspaceTaskRecord) bool {
	if !taskClaimStatusIsActiveOwnership(taskClaimStatus(task)) {
		return false
	}
	return !taskClaimLeaseExpired(task, time.Now().UTC())
}

func taskClaimLeaseExpired(task WorkspaceTaskRecord, now time.Time) bool {
	expiresAt := strings.TrimSpace(pointerValue(task.ClaimExpiresAt))
	if expiresAt == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return false
	}
	return !parsed.After(now)
}

func claimShouldBeReused(task WorkspaceTaskRecord, work AgentWorkNextResult, agentID string) bool {
	if !isClaimOwnedBy(task, agentID) {
		return false
	}
	if strings.TrimSpace(work.ClaimAction) != "reuse_claim" {
		return false
	}
	switch strings.TrimSpace(work.SessionAction) {
	case "reuse_active", "resume_inactive":
		return taskClaimStatus(task) == "CLAIMED" && work.Session != nil
	default:
		return false
	}
}

func nonRunnableClaimRequiresWake(task WorkspaceTaskRecord, agentID string) bool {
	if !isClaimOwnedBy(task, agentID) {
		return false
	}
	switch taskClaimStatus(task) {
	case "BLOCKED":
		return true
	default:
		return false
	}
}

func workHasExplicitResumeWake(work AgentWorkNextResult, trigger pendingWorkTrigger) bool {
	if normalizeWorkTrigger(trigger.Trigger) != "" {
		return true
	}
	if work.Packet == nil || work.Packet.Unblock == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(work.Packet.Unblock.UnblockState), "wake_selected")
}

func taskActivationSummary(task WorkspaceTaskRecord, fallback ...string) string {
	values := make([]string, 0, len(fallback)+3)
	values = append(values,
		strings.TrimSpace(task.Title),
		strings.TrimSpace(task.Description),
	)
	values = append(values, fallback...)
	values = append(values, "task claimed")
	return firstNonEmpty(values...)
}

func shouldRefreshActivationSummary(previousTaskID, previousSessionID, previousRunID, taskID, sessionID, runID, currentSummary string) bool {
	if strings.TrimSpace(currentSummary) == "" {
		return true
	}
	if strings.TrimSpace(previousTaskID) != strings.TrimSpace(taskID) {
		return true
	}
	if strings.TrimSpace(previousSessionID) != strings.TrimSpace(sessionID) {
		return true
	}
	return strings.TrimSpace(previousRunID) != strings.TrimSpace(runID)
}

func isTaskCandidate(task WorkspaceTaskRecord, agentID string) bool {
	status := strings.ToUpper(strings.TrimSpace(task.Status))
	if status == "RESOLVED" || status == "FAILED" || status == "CANCELLED" {
		return false
	}
	if task.ClaimAgentID == nil {
		return true
	}
	if isClaimOwnedBy(task, agentID) {
		switch taskClaimStatus(task) {
		case "", "CLAIMED", "RELEASED":
			return true
		default:
			return false
		}
	}
	return false
}

func chooseNextTask(tasks []WorkspaceTaskRecord, agentID string) *WorkspaceTaskRecord {
	candidates := make([]WorkspaceTaskRecord, 0, len(tasks))
	for _, task := range tasks {
		if isTaskCandidate(task, agentID) {
			candidates = append(candidates, task)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		leftOwned := isClaimOwnedBy(candidates[i], agentID)
		rightOwned := isClaimOwnedBy(candidates[j], agentID)
		if leftOwned != rightOwned {
			return leftOwned
		}
		leftPriority := taskPriorityRank(candidates[i].Priority)
		rightPriority := taskPriorityRank(candidates[j].Priority)
		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		return candidates[i].LinkedAt < candidates[j].LinkedAt
	})
	chosen := candidates[0]
	return &chosen
}

func chooseNextRunnableTask(tasks []WorkspaceTaskRecord, sessions []AgentSessionStateRecord, agentID string) *WorkspaceTaskRecord {
	candidates := make([]WorkspaceTaskRecord, 0, len(tasks))
	for _, task := range tasks {
		if !isTaskCandidate(task, agentID) {
			continue
		}
		if taskBlockedBySession(task, sessions, agentID) {
			continue
		}
		candidates = append(candidates, task)
	}
	return chooseNextTask(candidates, agentID)
}

func profileAllowsAutonomousExecutionClaim(profile AgentProfile) bool {
	primary := strings.ToLower(strings.TrimSpace(profile.PrimarySpecialization))
	role := strings.ToLower(strings.TrimSpace(profile.Role))
	mode := strings.ToLower(strings.TrimSpace(profile.DefaultWorkMode))
	mission := strings.ToLower(strings.TrimSpace(profile.Mission))

	if strings.Contains(primary, "meta-analysis") || strings.Contains(primary, "meta analysis") {
		return false
	}
	if strings.Contains(role, "observer") || strings.Contains(mode, "observer") {
		return false
	}
	if strings.Contains(mission, "without direct participation") {
		return false
	}
	if strings.Contains(mission, "do not solve problems") {
		return false
	}
	return true
}

func taskPriorityRank(priority string) int {
	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "CRITICAL":
		return 0
	case "HIGH":
		return 1
	case "MEDIUM":
		return 2
	case "LOW":
		return 3
	default:
		return 4
	}
}

func taskBlockedBySession(task WorkspaceTaskRecord, sessions []AgentSessionStateRecord, agentID string) bool {
	for _, session := range sessions {
		if strings.TrimSpace(session.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if strings.TrimSpace(session.TaskID) != strings.TrimSpace(task.TaskID) {
			continue
		}
		return !isSessionRunnable(&session)
	}
	return false
}

func updateTypeForResult(result StructuredTaskResult) string {
	if result.RequiresHuman {
		return "decision"
	}
	switch normalizeOutcome(result.Outcome) {
	case "completed":
		return "milestone"
	case "blocked", "failed":
		return "issue"
	default:
		return "progress"
	}
}

func coordinationStatusForResult(result StructuredTaskResult) string {
	if result.RequiresHuman {
		return "WAITING_DECISION"
	}
	switch normalizeOutcome(result.Outcome) {
	case "completed":
		return "COMPLETED"
	case "blocked":
		return "BLOCKED"
	case "failed":
		return "FAILED"
	default:
		return "ACTIVE"
	}
}

func claimTypeForMemoryType(memoryType string) string {
	switch strings.ToUpper(strings.TrimSpace(memoryType)) {
	case "DECISION":
		return "DECISION"
	case "LESSON":
		return "LESSON"
	case "PROCEDURE":
		return "PROCEDURE"
	case "INCIDENT":
		return "INCIDENT"
	default:
		return ""
	}
}

func toolCapabilityForName(name string) string {
	switch strings.TrimSpace(name) {
	case "shell":
		return "local.shell"
	case "read_file", "list_directory", "memory_read":
		return "local.fs.read"
	case "write_file", "memory_write", "daily_note":
		return "local.fs.write"
	default:
		return "tool.call"
	}
}

func queueUrgencyForPriority(priority string) string {
	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "CRITICAL":
		return "CRITICAL"
	case "HIGH":
		return "HIGH"
	case "MEDIUM":
		return "NORMAL"
	case "LOW":
		return "LOW"
	default:
		return "NORMAL"
	}
}
