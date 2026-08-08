package server

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
)

// ── RPC Schema / Describe ───────────────────────────────────────────

// ParamSchema describes a single RPC method parameter.
type ParamSchema struct {
	Type        string   `json:"type"`
	Required    bool     `json:"required"`
	Description string   `json:"description,omitempty"`
	Enum        []string `json:"enum,omitempty"`
	Default     string   `json:"default,omitempty"`
}

// MethodSchema describes an RPC method.
type MethodSchema struct {
	Method      string                 `json:"method"`
	Description string                 `json:"description"`
	Params      map[string]ParamSchema `json:"params"`
}

// rpcMethodSchemas is the registry of all RPC method schemas.
var rpcMethodSchemas = map[string]MethodSchema{
	// ── Agent operations ──
	"agent.register": {
		Method:      "agent.register",
		Description: "Register or refresh an agent's identity and capability metadata in a workspace. This does not establish online presence; the agent remains offline until agent.heartbeat succeeds. On re-register, omitted metadata fields preserve the current persisted value.",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true, Description: "Workspace identifier"},
			"agent_id":         {Type: "string", Required: true, Description: "Unique agent identifier"},
			"group_id":         {Type: "string", Required: false, Description: "Optional shared limit group identifier for provider-scoped budgeting"},
			"display_name":     {Type: "string", Required: false, Description: "Human-readable agent name. Required on first registration; omitted re-register preserves the current value."},
			"role":             {Type: "string", Required: false, Description: "Agent role. Omitted re-register preserves the current value.", Default: "generalist"},
			"owner_user_id":    {Type: "string", Required: false, Description: "Owner user who registered this agent. Required on first registration; omitted re-register preserves the current value."},
			"capabilities":     {Type: "string", Required: false, Description: "Comma-separated list of capabilities. Omitted re-register preserves the current value."},
			"summary":          {Type: "string", Required: false, Description: "Current agent summary. Omitted re-register preserves the current value."},
			"status":           {Type: "string", Required: false, Description: "Agent status metadata to persist at registration time. Online presence still requires heartbeat. Omitted re-register preserves the current value.", Enum: []string{"REGISTERED", "ACTIVE", "PAUSED", "BLOCKED", "OFFLINE"}, Default: "REGISTERED"},
			"protocol_version": {Type: "string", Required: false, Description: "Protocol version. Omitted re-register preserves the current value.", Default: "workspace-bootstrap/v1"},
		},
	},
	"agent.heartbeat": {
		Method:      "agent.heartbeat",
		Description: "Send heartbeat to establish or maintain online presence and refresh status/summary. Call every 2-5 minutes.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true, Description: "Workspace identifier"},
			"agent_id":     {Type: "string", Required: true, Description: "Agent identifier"},
			"status":       {Type: "string", Required: true, Description: "Current agent status", Enum: []string{"REGISTERED", "ACTIVE", "PAUSED", "BLOCKED", "OFFLINE"}},
			"summary":      {Type: "string", Required: false, Description: "What the agent is currently doing"},
		},
	},
	"agent.bootstrap": {
		Method:      "agent.bootstrap",
		Description: "Get full workspace context on agent startup: config, agents, tasks, docs, messages, updates",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true, Description: "Workspace identifier"},
			"agent_id":      {Type: "string", Required: true, Description: "Agent identifier"},
			"updates_limit": {Type: "integer", Required: false, Description: "Max updates to return", Default: "10"},
		},
	},
	"agent.update.post": {
		Method:      "agent.update.post",
		Description: "Post a status update visible to all workspace participants",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"agent_id":       {Type: "string", Required: true},
			"update_type":    {Type: "string", Required: true, Enum: []string{"progress", "milestone", "issue", "question", "decision"}},
			"summary":        {Type: "string", Required: true, Description: "Update text"},
			"payload_json":   {Type: "string", Required: false, Description: "Additional payload as JSON string"},
			"requires_human": {Type: "boolean", Required: false, Description: "Whether this requires human attention"},
		},
	},
	"agent.session.start": {
		Method:      "agent.session.start",
		Description: "Declare ownership of an active work session",
		Params:      sessionEventParamSchema(),
	},
	"agent.session.status": {
		Method:      "agent.session.status",
		Description: "Refresh the current state of an active work session",
		Params:      sessionEventParamSchema(),
	},
	"agent.session.blocked": {
		Method:      "agent.session.blocked",
		Description: "Mark an active work session as blocked",
		Params:      sessionEventParamSchema(),
	},
	"agent.session.decision_needed": {
		Method:      "agent.session.decision_needed",
		Description: "Mark an active work session as waiting on a decision",
		Params:      sessionEventParamSchema(),
	},
	"agent.session.keepalive": {
		Method:      "agent.session.keepalive",
		Description: "Explicitly signal whether another agent should keep their own session active",
		Params:      sessionEventParamSchema(),
	},
	"agent.session.end": {
		Method:      "agent.session.end",
		Description: "Close an active work session",
		Params:      sessionEventParamSchema(),
	},
	"agent.session.takeover": {
		Method:      "agent.session.takeover",
		Description: "Atomically close an active session and open a successor session for a new owner",
		Params: map[string]ParamSchema{
			"workspace_id":         {Type: "string", Required: true},
			"session_id":           {Type: "string", Required: true},
			"takeover_agent_id":    {Type: "string", Required: true},
			"summary":              {Type: "string", Required: true, Description: "Why the takeover is happening"},
			"successor_session_id": {Type: "string", Required: false, Description: "Optional stable session id for the successor"},
			"successor_summary":    {Type: "string", Required: false, Description: "Optional summary for the successor session"},
			"updated_at":           {Type: "string", Required: false},
		},
	},
	"agent.profile.update": {
		Method:      "agent.profile.update",
		Description: "Update an agent profile and record durable authority-bound evidence because profile fields affect autonomous work selection.",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true, Description: "Workspace identifier"},
			"agent_id":       {Type: "string", Required: true, Description: "Agent profile to update"},
			"actor_id":       {Type: "string", Required: true, Description: "Authenticated actor initiating the profile mutation"},
			"bio":            {Type: "string", Required: false, Description: "Agent profile bio"},
			"specialization": {Type: "string", Required: false, Description: "Agent specialization; observer/meta-analysis values can suppress autonomous work assignment"},
			"owner_name":     {Type: "string", Required: false},
			"owner_contact":  {Type: "string", Required: false},
			"avatar_url":     {Type: "string", Required: false},
			"links":          {Type: "array[string]", Required: false},
			"tags":           {Type: "array[string]", Required: false, Description: "Profile tags; observer/meta-analysis tags can suppress autonomous work assignment"},
			"tools_access":   {Type: "array[string]", Required: false},
			"metadata":       {Type: "object", Required: false, Description: "Profile metadata; default_work_mode=observer can suppress autonomous work assignment"},
		},
	},
	"agent.profile.get": {
		Method:      "agent.profile.get",
		Description: "Get agent profile",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
		},
	},
	"agent.delete": {
		Method:      "agent.delete",
		Description: "Delete/unregister an agent from the workspace and record durable runtime evidence",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true, Description: "Workspace identifier"},
			"agent_id":     {Type: "string", Required: true, Description: "Agent identifier to delete"},
			"actor":        {Type: "string", Required: true, Description: "Authenticated actor initiating the deletion"},
		},
	},

	// ── Agent Tasks ──
	"agent.task.claim": {
		Method:      "agent.task.claim",
		Description: "Claim a task for this agent to work on",
		Params: map[string]ParamSchema{
			"workspace_id":           {Type: "string", Required: true},
			"agent_id":               {Type: "string", Required: true},
			"task_id":                {Type: "string", Required: true},
			"project_role_id":        {Type: "string", Required: false, Description: "Optional active project role proving this agent's project responsibility"},
			"repo_id":                {Type: "string", Required: false, Description: "Optional repository binding for project implementation admission"},
			"checkout_id":            {Type: "string", Required: false, Description: "Optional checkout/worktree binding; must belong to the same project and agent"},
			"branch_id":              {Type: "string", Required: false, Description: "Optional project branch binding; must belong to the same project and agent"},
			"write_scope_json":       {Type: "string", Required: false, Description: "Optional JSON string describing the intended write scope for this claim"},
			"coordination_mode":      {Type: "string", Required: false, Description: "Optional coordination mode. trust_first treats project claim admission gates as advisory telemetry.", Enum: []string{"strict", "trust_first"}, Default: "strict"},
			"summary":                {Type: "string", Required: false, Description: "Initial claim summary"},
			"selected_from_frontier": {Type: "boolean", Required: false, Default: "false", Description: "True when the agent chose this task from an autonomous task frontier"},
			"frontier_generation_id": {Type: "string", Required: false, Description: "Generation id of the task frontier used for self-selection"},
			"self_fit_summary":       {Type: "string", Required: false, Description: "Agent-authored reason this task fits its current role, tools, and workload"},
		},
	},
	"agent.task_frontier.decision": {
		Method:      "agent.task_frontier.decision",
		Description: "Persist the agent's decision for a previously offered autonomous task frontier before claiming or declining work.",
		Params: map[string]ParamSchema{
			"workspace_id":           {Type: "string", Required: true},
			"agent_id":               {Type: "string", Required: true},
			"frontier_generation_id": {Type: "string", Required: true, Description: "Generation id from the task_frontier_available packet"},
			"decision_state":         {Type: "string", Required: true, Enum: []string{"selected", "declined", "model_failed", "hydration_failed", "claim_failed", "admission_failed"}},
			"selected_task_id":       {Type: "string", Required: false, Description: "Required for selected and hydration_failed decisions"},
			"summary":                {Type: "string", Required: false, Description: "Short decision evidence or failure summary"},
		},
	},
	"agent.task.hydrate": {
		Method:      "agent.task.hydrate",
		Description: "Return a task-scoped hydration bundle using the existing workspace/task context store",
		Params: map[string]ParamSchema{
			"workspace_id":       {Type: "string", Required: false, Description: "Optional workspace override; when omitted the task's primary workspace is resolved automatically"},
			"task_id":            {Type: "string", Required: true},
			"doc_keys":           {Type: "array[string]", Required: false, Description: "Optional explicit doc keys to include"},
			"include_all_docs":   {Type: "boolean", Required: false, Default: "true"},
			"updates_limit":      {Type: "integer", Required: false, Default: "20"},
			"artifact_limit":     {Type: "integer", Required: false, Default: "20"},
			"related_task_limit": {Type: "integer", Required: false, Default: "10"},
		},
	},
	"agent.task.release": {
		Method:      "agent.task.release",
		Description: "Release a claimed task back to the pool",
		Params: map[string]ParamSchema{
			"workspace_id":            {Type: "string", Required: true},
			"agent_id":                {Type: "string", Required: true},
			"task_id":                 {Type: "string", Required: true},
			"reason":                  {Type: "string", Required: false},
			"session_transition_kind": {Type: "string", Required: false, Description: "Optional release transition mode for reclaiming same-agent claims after the owning session has already ended.", Enum: []string{"reclaim_release"}},
		},
	},
	"agent.task.complete": {
		Method:      "agent.task.complete",
		Description: "Mark a claimed task as completed",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true},
			"summary":      {Type: "string", Required: false, Description: "Completion summary"},
		},
	},
	"agent.task.block": {
		Method:      "agent.task.block",
		Description: "Mark a task as blocked (needs human input or external dependency)",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false, Description: "Why the task is blocked"},
		},
	},

	// ── Agent Nodes (DAG) ──
	"agent.work.next": {
		Method:      "agent.work.next",
		Description: "Select the next unit of work for an agent, preferring resumable sessions and claimed tasks before new pending work; can also carry an explicit wake trigger for targeted session resume",
		Params: map[string]ParamSchema{
			"workspace_id":         {Type: "string", Required: true},
			"agent_id":             {Type: "string", Required: true},
			"include_hydration":    {Type: "boolean", Required: false, Default: "false"},
			"include_packet":       {Type: "boolean", Required: false, Default: "false", Description: "Attach a typed work packet with coordination and context hints"},
			"include_advisory":     {Type: "boolean", Required: false, Default: "false", Description: "Best-effort scoped advisory hints for the selected work packet"},
			"enable_task_frontier": {Type: "boolean", Required: false, Default: "false", Description: "Return a task_frontier_available packet for agent self-selection instead of preselecting fresh pending work"},
			"frontier_limit":       {Type: "integer", Required: false, Default: "3", Description: "Limit for scoped advisory or autonomous task frontier hints"},
			"doc_keys":             {Type: "array[string]", Required: false, Description: "Optional explicit doc keys to include when hydration is requested"},
			"include_all_docs":     {Type: "boolean", Required: false, Default: "true"},
			"updates_limit":        {Type: "integer", Required: false, Default: "20"},
			"artifact_limit":       {Type: "integer", Required: false, Default: "20"},
			"related_task_limit":   {Type: "integer", Required: false, Default: "10"},
			"session_limit":        {Type: "integer", Required: false, Default: "100"},
			"coordination_mode":    {Type: "string", Required: false, Description: "Optional coordination mode. trust_first keeps profile, budget, phase, and role gates advisory where possible.", Enum: []string{"strict", "trust_first"}, Default: "strict"},
			"trigger":              {Type: "string", Required: false, Description: "Optional wake trigger such as inbound_message or runtime_resume"},
			"candidate_task_id":    {Type: "string", Required: false, Description: "Optional task hint for targeted wake/resume selection"},
			"candidate_session_id": {Type: "string", Required: false, Description: "Optional session hint for targeted wake/resume selection"},
		},
	},
	"agent.node.claim": {
		Method:      "agent.node.claim",
		Description: "Claim a specific DAG node within a task",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true},
			"node_id":      {Type: "string", Required: true},
			"summary":      {Type: "string", Required: false, Description: "Initial claim summary"},
		},
	},
	"agent.node.release": {
		Method:      "agent.node.release",
		Description: "Release a claimed DAG node back to the pool",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true},
			"node_id":      {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
		},
	},
	"agent.node.complete": {
		Method:      "agent.node.complete",
		Description: "Mark a claimed node as completed (triggers DAG engine)",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true},
			"node_id":      {Type: "string", Required: true},
			"summary":      {Type: "string", Required: false, Description: "Completion summary"},
		},
	},

	// ── Coalition Engine ──
	"coalition.offer": {
		Method:      "coalition.offer",
		Description: "Offer to join the coalition anchored to a task or tension. The response includes an attach decision envelope (`allowed`, `guarded`, or `rejected`) derived from fit/novelty/crowding signals.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true, Description: "Task ID or tension ID to resolve into a coalition target"},
			"agent_id":     {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true, Description: "Acting agent ID; must match agent_id for offer."},
			"role":         {Type: "string", Required: true, Enum: []string{"PRIMARY", "REVIEWER", "SUPPORT"}, Description: "Advisory requested role. Actual coalition membership role is system-normalized."},
		},
	},
	"coalition.leave": {
		Method:      "coalition.leave",
		Description: "Leave an active coalition. The caller must already be an active coalition member, and stale/non-canonical coalition IDs must fail closed rather than silently mutating membership.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"coalition_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true, Description: "Acting agent ID; must match agent_id for leave."},
			"reason":       {Type: "string", Required: false},
		},
	},
	"coalition.status": {
		Method:      "coalition.status",
		Description: "Get the live coalitions in a workspace, optionally filtering to one coalition",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"coalition_id": {Type: "string", Required: false},
		},
	},
	"coalition.seek": {
		Method:      "coalition.seek",
		Description: "Seek coalition matches grounded in live tension data. Each match surfaces an attach decision envelope (`allowed`, `guarded`, or `rejected`) in addition to raw score/probability factors, plus a coalition_integrity hint so canonical-looking matches do not silently hide duplicate-live drift.",
		Params: map[string]ParamSchema{
			"workspace_id":    {Type: "string", Required: true},
			"task_id":         {Type: "string", Required: false, Description: "Optional task or tension anchor to constrain matches"},
			"agent_id":        {Type: "string", Required: true},
			"role":            {Type: "string", Required: false, Enum: []string{"PRIMARY", "REVIEWER", "SUPPORT"}},
			"required_skills": {Type: "array", Required: false, Description: "Optional advisory skills list echoed in seek payload"},
			"reason":          {Type: "string", Required: false},
			"limit":           {Type: "integer", Required: false, Default: "20"},
		},
	},
	"coalition.invite": {
		Method:      "coalition.invite",
		Description: "Invite an agent to join a coalition. The inviter must already be an active coalition member. Any requested role is advisory only; final coalition membership role remains system-normalized.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"coalition_id": {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true, Description: "Inviter agent ID; must match the canonical agent_id or legacy invited_by field."},
			"agent_id":     {Type: "string", Required: true, Description: "Inviter agent ID for the canonical client shape; also used as a legacy target if target_id is omitted"},
			"target_id":    {Type: "string", Required: false, Description: "Explicit invitee agent ID"},
			"invited_by":   {Type: "string", Required: false, Description: "Legacy inviter field"},
			"role":         {Type: "string", Required: false, Description: "Advisory requested role for the invitee. It is preserved as informational intent, but actual coalition membership role is system-normalized."},
		},
	},
	"coalition.kick": {
		Method:      "coalition.kick",
		Description: "Kick an agent from a coalition. The kicker must already be an active coalition member; self-removal should use coalition.leave. Any reason is preserved as operator note only and does not change membership policy semantics.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"coalition_id": {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true, Description: "Kicker agent ID; must match the canonical agent_id or legacy kicked_by field."},
			"agent_id":     {Type: "string", Required: true, Description: "Kicker agent ID for the canonical client shape; also used as a legacy target if target_id is omitted"},
			"target_id":    {Type: "string", Required: false, Description: "Explicit kicked agent ID"},
			"kicked_by":    {Type: "string", Required: false, Description: "Legacy kicker field"},
			"reason":       {Type: "string", Required: false, Description: "Informational operator note only; it does not alter membership policy or force semantics."},
		},
	},

	// ── Reviewer Mesh ──
	"reviewer.route": {
		Method:      "reviewer.route",
		Description: "Route verifier tier plus advisory near/far reviewer suggestions from caller-supplied bundle evidence. The supplied candidate pool is intersected with registered online non-generator workspace agents; the response exposes machine-readable near-reviewer status, eligibility, and fallback fields plus a far-reviewer outcome status for the current route. A concrete near reviewer is only emitted when workspace-scoped open session-event decision/handoff demand provides observed collaboration evidence; otherwise the surface stays advisory, partial, and may omit near reviewer rather than materialize a heuristic-only winner. Far reviewer is omitted when current route evidence does not support a distance-backed suggestion.",
		Params: map[string]ParamSchema{
			"workspace_id":            {Type: "string", Required: true},
			"bundle_id":               {Type: "string", Required: false},
			"generator_agent_id":      {Type: "string", Required: true},
			"available_reviewers":     {Type: "array[string]", Required: true, Description: "Caller-supplied candidate pool; store filters this against workspace registration and live evidence"},
			"is_multi_patch":          {Type: "boolean", Required: true},
			"impact_score":            {Type: "number", Required: true, Description: "0..1 caller-supplied route evidence"},
			"contradiction_pressure":  {Type: "number", Required: true, Description: "0..1 caller-supplied route evidence"},
			"has_active_dissent":      {Type: "boolean", Required: true},
			"touches_hard_constraint": {Type: "boolean", Required: true},
			"cluster_mode":            {Type: "string", Required: true, Description: "Caller-supplied route evidence, e.g. explore or stabilize"},
			"merge_risk":              {Type: "number", Required: true, Description: "0..1 caller-supplied route evidence"},
		},
	},
	"reviewer.scarcity": {
		Method:      "reviewer.scarcity",
		Description: "Return persisted reviewer-load and workspace-scoped open session-collaboration-load evidence for the current mesh. This surface is intentionally partial, prefers an online typed-reviewer upper bound when typed reviewer evidence exists, falls back to an online-agent upper bound only when no online typed reviewers are available, may stay UNKNOWN on low-utilization upper-bound paths, and can still surface SCARCE when reviewer load, generalist fallback assignments, or open session-collaboration load is highly concentrated.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},

	// ── Agent Messaging ──
	"agent.message.send": {
		Method:      "agent.message.send",
		Description: "Send a message to another agent or broadcast to all",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"from_agent_id": {Type: "string", Required: true, Description: "Sender agent ID"},
			"to_agent_id":   {Type: "string", Required: false, Description: "Recipient agent ID. Empty = broadcast"},
			"channel":       {Type: "string", Required: false, Description: "Message channel", Default: "default"},
			"content":       {Type: "string", Required: true, Description: "Message body"},
			"content_type":  {Type: "string", Required: false, Default: "text/plain"},
			"metadata_json": {Type: "string", Required: false, Description: "Additional metadata as JSON string"},
		},
	},
	"agent.message.poll": {
		Method:      "agent.message.poll",
		Description: "Long-poll for incoming messages (waits up to timeout_sec for new messages)",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"agent_id":         {Type: "string", Required: true},
			"after_created_at": {Type: "string", Required: false, Description: "Opaque cursor from the previous poll page's next_cursor (legacy RFC3339 timestamps still work for compatibility but can re-deliver same-timestamp rows)"},
			"limit":            {Type: "integer", Required: false, Description: "Maximum number of messages to return", Default: "20"},
			"timeout_sec":      {Type: "integer", Required: false, Description: "Long-poll timeout in seconds", Default: "30"},
			"lookback_hours":   {Type: "integer", Required: false, Description: "Default history window when after_created_at is omitted", Default: "24"},
		},
	},
	"agent.message.ack": {
		Method:      "agent.message.ack",
		Description: "Mark messages as read",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"message_ids":  {Type: "array[string]", Required: true, Description: "Array of message IDs to acknowledge"},
		},
	},

	// ── Agent-to-Agent RPC ──
	"agent.request": {
		Method:      "agent.request",
		Description: "Send a direct request to another agent (triggers SSE notification)",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"from_agent_id": {Type: "string", Required: true},
			"to_agent_id":   {Type: "string", Required: true},
			"method":        {Type: "string", Required: true, Description: "Request method name"},
			"payload_json":  {Type: "string", Required: false, Description: "Request payload as JSON string"},
		},
	},
	"agent.respond": {
		Method:      "agent.respond",
		Description: "Respond to a pending agent request",
		Params: map[string]ParamSchema{
			"request_id": {Type: "string", Required: true},
			"response":   {Type: "string", Required: true, Description: "Response payload as JSON string"},
		},
	},
	"agent.request.result": {
		Method:      "agent.request.result",
		Description: "Get the result of a previously sent agent request",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"request_id":   {Type: "string", Required: true},
		},
	},
	"agent.request.list": {
		Method:      "agent.request.list",
		Description: "Atomically claim pending incoming requests for an agent and return them",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
		},
	},
	"agent.request.open.list": {
		Method:      "agent.request.open.list",
		Description: "Read open incoming requests for clean-stop/preflight inspection without claiming or mutating them",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"limit":        {Type: "integer", Required: false, Default: "100"},
		},
	},

	// ── Agent State (Memory) ──
	"agent.state.get": {
		Method:      "agent.state.get",
		Description: "Get a saved state value by key",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"key":          {Type: "string", Required: true},
		},
	},
	"agent.state.set": {
		Method:      "agent.state.set",
		Description: "Save a state value (persistent memory across sessions)",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"key":          {Type: "string", Required: true},
			"value":        {Type: "string", Required: true},
		},
	},
	"agent.state.list": {
		Method:      "agent.state.list",
		Description: "List all saved state keys for an agent",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
		},
	},
	"agent.state.delete": {
		Method:      "agent.state.delete",
		Description: "Delete a state key",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"key":          {Type: "string", Required: true},
		},
	},

	// ── Workspace ──
	"agent.heartbeat_lease.acquire": {
		Method:      "agent.heartbeat_lease.acquire",
		Description: "Acquire a durable lease for one agent heartbeat run and its named locks",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"heartbeat_id": {Type: "string", Required: true},
			"owner_id":     {Type: "string", Required: true},
			"lease_token":  {Type: "string", Required: true},
			"locks":        {Type: "array", Required: false},
			"ttl_sec":      {Type: "number", Required: false, Default: "60"},
		},
	},
	"agent.heartbeat_lease.refresh": {
		Method:      "agent.heartbeat_lease.refresh",
		Description: "Refresh a durable heartbeat lease when the caller still owns the token",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"heartbeat_id": {Type: "string", Required: true},
			"owner_id":     {Type: "string", Required: true},
			"lease_token":  {Type: "string", Required: true},
			"locks":        {Type: "array", Required: false},
			"ttl_sec":      {Type: "number", Required: false, Default: "60"},
		},
	},
	"agent.heartbeat_lease.release": {
		Method:      "agent.heartbeat_lease.release",
		Description: "Release a durable heartbeat lease when the token matches",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"heartbeat_id": {Type: "string", Required: true},
			"lease_token":  {Type: "string", Required: true},
		},
	},

	"workspace.doc.put": {
		Method:      "workspace.doc.put",
		Description: "Create or update a shared workspace document",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true, Description: "Workspace identifier"},
			"doc_key":      {Type: "string", Required: true, Description: "Unique document key (e.g. 'charter', 'current_context')"},
			"title":        {Type: "string", Required: true, Description: "Document title"},
			"content":      {Type: "string", Required: true, Description: "Document content (markdown)"},
			"updated_by":   {Type: "string", Required: true, Description: "Author agent_id or user_id"},
			"expected_sha": {Type: "string", Required: false, Description: "SHA256 of previous content for optimistic locking"},
		},
	},
	"workspace.doc.get": {
		Method:      "workspace.doc.get",
		Description: "Get a workspace document by key",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"doc_key":      {Type: "string", Required: true},
		},
	},
	"workspace.doc.list": {
		Method:      "workspace.doc.list",
		Description: "List all workspace documents",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"include_archived": {Type: "boolean", Required: false, Description: "Include archived docs in results"},
		},
	},
	"workspace.artifact.write": {
		Method:      "workspace.artifact.write",
		Description: "Record a first-class workspace artifact reference tied to a task, update, or runtime deliverable",
		Params: map[string]ParamSchema{
			"artifact_id":   {Type: "string", Required: false, Description: "Optional stable artifact identifier for idempotent writes"},
			"workspace_id":  {Type: "string", Required: true},
			"task_id":       {Type: "string", Required: false},
			"update_id":     {Type: "string", Required: false},
			"title":         {Type: "string", Required: true},
			"artifact_ref":  {Type: "string", Required: true, Description: "Canonical reference such as doc:key, file:path, run:id, or URL"},
			"kind":          {Type: "string", Required: false, Description: "Artifact category such as workspace_doc, file, report, patch, or reference", Default: "reference"},
			"content_type":  {Type: "string", Required: false, Description: "MIME type for the artifact payload or target", Default: "application/octet-stream"},
			"created_by":    {Type: "string", Required: true, Description: "Agent or user that published the artifact"},
			"metadata_json": {Type: "string", Required: false, Description: "Optional structured metadata envelope as JSON string"},
		},
	},
	"workspace.artifact.list": {
		Method:      "workspace.artifact.list",
		Description: "List workspace artifact references, optionally filtered by task or update",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: false},
			"update_id":    {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.segment.list": {
		Method:      "workspace.segment.list",
		Description: "List derived workspace segments for docs or artifacts. Read-only structural view over doc headings and text-artifact sections; no policy authority.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"doc_key":      {Type: "string", Required: false, Description: "Restrict to one workspace doc"},
			"artifact_ref": {Type: "string", Required: false, Description: "Restrict to one workspace artifact ref"},
			"segment_ref":  {Type: "string", Required: false, Description: "Resolve the source implied by one concrete segment_ref; cannot be combined with doc_key or artifact_ref"},
			"limit":        {Type: "integer", Required: false, Default: "200"},
		},
	},
	"workspace.segment.get": {
		Method:      "workspace.segment.get",
		Description: "Resolve one derived workspace segment by segment_ref. Read-only structural lookup for operator inspection and tension evidence anchoring.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"segment_ref":  {Type: "string", Required: true},
		},
	},
	"workspace.doc.archive": {
		Method:      "workspace.doc.archive",
		Description: "Archive a document without deleting its content/history",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"doc_key":      {Type: "string", Required: true},
			"archived_by":  {Type: "string", Required: true},
		},
	},
	"workspace.doc.delete": {
		Method:      "workspace.doc.delete",
		Description: "Delete a document from active workspace docs",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"doc_key":      {Type: "string", Required: true},
			"deleted_by":   {Type: "string", Required: true},
		},
	},
	"workspace.doc.history": {
		Method:      "workspace.doc.history",
		Description: "Get version history of a document",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"doc_key":      {Type: "string", Required: true},
		},
	},
	"workspace.memory.write": {
		Method:      "workspace.memory.write",
		Description: "Record or update durable workspace memory sourced from the current runtime state",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"memory_id":    {Type: "string", Required: false, Description: "Optional stable memory identifier for updates"},
			"memory_type":  {Type: "string", Required: false, Description: "Canonical memory category for the current direct durable write boundary", Enum: append([]string(nil), currentDirectWorkspaceMemoryTypes...), Default: "NOTE"},
			"title":        {Type: "string", Required: false},
			"body":         {Type: "string", Required: true, Description: "Canonical memory body"},
			"summary":      {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"source_kind":  {Type: "string", Required: false, Description: "Origin such as manual, compaction, reflection or import", Default: "manual"},
			"source_id":    {Type: "string", Required: false},
			"tags":         {Type: "array[string]", Required: false},
			"importance":   {Type: "number", Required: false, Description: "0..1 weighting"},
			"confidence":   {Type: "number", Required: false, Description: "0..1 confidence"},
		},
	},
	"workspace.memory.node.write": {
		Method:      "workspace.memory.node.write",
		Description: "Write a memory node through canonical workspace_memory and return the derived memory-graph node projection",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"node_id":      {Type: "string", Required: false, Description: "Optional memnode:workspace_memory:* id or backing memory id for updates"},
			"memory_id":    {Type: "string", Required: false, Description: "Transitional alias for the backing workspace memory id; if both ids are provided they must agree"},
			"memory_type":  {Type: "string", Required: false, Description: "Canonical memory category for the current direct workspace-memory/node write boundary", Enum: append([]string(nil), currentDirectWorkspaceMemoryTypes...), Default: "NOTE"},
			"title":        {Type: "string", Required: false},
			"body":         {Type: "string", Required: true, Description: "Canonical memory body; graph node fields are derived from this canonical record"},
			"summary":      {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"source_kind":  {Type: "string", Required: false, Description: "Origin such as manual, compaction, reflection or import", Default: "manual"},
			"source_id":    {Type: "string", Required: false},
			"tags":         {Type: "array[string]", Required: false},
			"importance":   {Type: "number", Required: false, Description: "0..1 weighting"},
			"confidence":   {Type: "number", Required: false, Description: "0..1 confidence"},
		},
	},
	"workspace.memory.list": {
		Method:      "workspace.memory.list",
		Description: "List recent durable workspace memory records",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"memory_type":      {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"source_kind":      {Type: "string", Required: false},
			"include_archived": {Type: "boolean", Required: false, Description: "Include archived/tombstoned memory records"},
			"limit":            {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.memory.search": {
		Method:      "workspace.memory.search",
		Description: "Search durable workspace memory using lexical retrieval without embeddings",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"query":            {Type: "string", Required: true},
			"memory_type":      {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"source_kind":      {Type: "string", Required: false},
			"include_archived": {Type: "boolean", Required: false, Description: "Include archived/tombstoned memory records"},
			"limit":            {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.memory.packet.kernel.get": {
		Method:      "workspace.memory.packet.kernel.get",
		Description: "Build a read-only shared kernel packet with the mandatory coordination set for a task or session scope",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"task_id":          {Type: "string", Required: false, Description: "Task anchor; required when session_id is omitted"},
			"session_id":       {Type: "string", Required: false, Description: "Optional session anchor used to resolve the current task and handoff context"},
			"agent_id":         {Type: "string", Required: false, Description: "Optional agent hint for scoped cluster resolution only; not an authority input"},
			"doc_keys":         {Type: "array[string]", Required: false, Description: "Optional explicit docs to include in the packet kernel"},
			"artifact_refs":    {Type: "array[string]", Required: false, Description: "Optional explicit artifact refs to bias segment and locus assembly"},
			"include_all_docs": {Type: "boolean", Required: false, Default: "false", Description: "When true, include all workspace docs visible to the task hydration bundle"},
			"budget":           {Type: "object", Required: false, Description: "Structured budget for retrieval lanes instead of scalar limits"},
		},
	},
	"workspace.memory.packet.shell.get": {
		Method:      "workspace.memory.packet.shell.get",
		Description: "Build a read-only agent-scoped differential shell packet layered on top of the shared task kernel",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"agent_id":         {Type: "string", Required: true, Description: "Owning agent for the shell packet"},
			"task_id":          {Type: "string", Required: false, Description: "Task anchor; required when session_id is omitted"},
			"session_id":       {Type: "string", Required: false, Description: "Optional current session anchor; if present it must belong to agent_id"},
			"doc_keys":         {Type: "array[string]", Required: false},
			"artifact_refs":    {Type: "array[string]", Required: false},
			"include_all_docs": {Type: "boolean", Required: false, Default: "false"},
			"budget":           {Type: "object", Required: false, Description: "Structured budget for retrieval lanes instead of scalar limits"},
		},
	},
	"workspace.memory.pack.list": {
		Method:      "workspace.memory.pack.list",
		Description: "List canonical memory packs through the memory-service namespace using episode packs as the current backing source",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"pack_type":    {Type: "string", Required: false},
			"pack_mode":    {Type: "string", Required: false, Enum: []string{"COMPLETE", "DETERMINISTIC_FALLBACK"}},
			"session_id":   {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.memory.pack.write": {
		Method:      "workspace.memory.pack.write",
		Description: "Write a memory pack through canonical session compaction snapshots and return the derived episode pack projection",
		Params: map[string]ParamSchema{
			"workspace_id":             {Type: "string", Required: true},
			"session_id":               {Type: "string", Required: true},
			"snapshot_id":              {Type: "string", Required: false, Description: "Optional idempotency-style canonical snapshot identifier"},
			"agent_id":                 {Type: "string", Required: false, Description: "Optional agent anchor; if omitted it is derived from session_id"},
			"trigger_kind":             {Type: "string", Required: false, Description: "Compaction cause only", Enum: []string{"manual_compaction", "token_budget_exceeded"}},
			"pack_mode":                {Type: "string", Required: false, Enum: []string{"COMPLETE", "DETERMINISTIC_FALLBACK"}},
			"source_window_digest":     {Type: "string", Required: false},
			"token_budget":             {Type: "integer", Required: false},
			"message_count_before":     {Type: "integer", Required: false},
			"message_count_after":      {Type: "integer", Required: false},
			"message_tokens_before":    {Type: "integer", Required: false},
			"message_tokens_after":     {Type: "integer", Required: false},
			"total_input_tokens":       {Type: "integer", Required: false},
			"total_output_tokens":      {Type: "integer", Required: false},
			"summary_text":             {Type: "string", Required: false},
			"summary_workspace_memory": {Type: "string", Required: false, Description: "Active compaction-backed SUMMARY workspace_memory id"},
		},
	},
	"workspace.memory.promotion.enqueue": {
		Method:      "workspace.memory.promotion.enqueue",
		Description: "Enqueue a review-gated memory promotion candidate without creating durable workspace truth yet",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"promotion_id":   {Type: "string", Required: false, Description: "Optional stable promotion id for replay"},
			"candidate_kind": {Type: "string", Required: false, Default: "WORKSPACE_MEMORY", Enum: []string{"WORKSPACE_MEMORY"}},
			"memory_type":    {Type: "string", Required: false, Description: "Typed durable memory category; defaults to NOTE"},
			"title":          {Type: "string", Required: false},
			"body":           {Type: "string", Required: true},
			"summary":        {Type: "string", Required: false},
			"agent_id":       {Type: "string", Required: false},
			"session_id":     {Type: "string", Required: false},
			"task_id":        {Type: "string", Required: false},
			"source_kind":    {Type: "string", Required: true, Description: "Candidate provenance source such as episode_pack or memory_packet_shell"},
			"source_id":      {Type: "string", Required: true},
			"tags":           {Type: "array[string]", Required: false},
			"importance":     {Type: "number", Required: false},
			"confidence":     {Type: "number", Required: false},
			"basis_digest":   {Type: "string", Required: true, Description: "Stable digest of the evidence window used to propose the candidate"},
			"basis_refs":     {Type: "array[string]", Required: false},
			"proposed_by":    {Type: "string", Required: true},
		},
	},
	"workspace.memory.promotion.resolve": {
		Method:      "workspace.memory.promotion.resolve",
		Description: "Resolve a pending memory promotion candidate and optionally materialize it through canonical workspace memory",
		Params: map[string]ParamSchema{
			"workspace_id":    {Type: "string", Required: true},
			"promotion_id":    {Type: "string", Required: false},
			"queue_key":       {Type: "string", Required: false},
			"resolution":      {Type: "string", Required: true, Enum: []string{"ACCEPTED", "REJECTED", "SUPERSEDED", "CANCELLED"}},
			"resolution_note": {Type: "string", Required: false},
			"resolved_by":     {Type: "string", Required: true},
		},
	},
	"workspace.memory.promotion.get": {
		Method:      "workspace.memory.promotion.get",
		Description: "Get a single memory promotion queue record",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"promotion_id": {Type: "string", Required: true},
		},
	},
	"workspace.memory.promotion.list": {
		Method:      "workspace.memory.promotion.list",
		Description: "List recent memory promotion queue records",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"state":          {Type: "string", Required: false, Enum: []string{"PENDING", "ACCEPTED", "REJECTED", "SUPERSEDED", "CANCELLED"}},
			"candidate_kind": {Type: "string", Required: false, Enum: []string{"WORKSPACE_MEMORY"}},
			"candidate_type": {Type: "string", Required: false},
			"limit":          {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.memory.pack.get": {
		Method:      "workspace.memory.pack.get",
		Description: "Get a canonical memory pack through the memory-service namespace using episode packs as the current backing source",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"pack_id":      {Type: "string", Required: true},
		},
	},
	"workspace.memory.remove": {
		Method:      "workspace.memory.remove",
		Description: "Archive a durable workspace memory record without deleting its provenance or audit trail",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"memory_id":    {Type: "string", Required: true},
			"removed_by":   {Type: "string", Required: true, Description: "Actor archiving the memory record"},
			"reason":       {Type: "string", Required: false, Description: "Optional tombstone reason"},
		},
	},
	"workspace.memory.restore": {
		Method:      "workspace.memory.restore",
		Description: "Restore an archived durable workspace memory record back to the active set",
		Params: map[string]ParamSchema{
			"workspace_id":    {Type: "string", Required: true},
			"memory_id":       {Type: "string", Required: true},
			"restored_by":     {Type: "string", Required: true, Description: "Actor restoring the memory record"},
			"recovery_reason": {Type: "string", Required: false, Description: "Optional reason for restoration"},
		},
	},
	"workspace.memory.graph.list": {
		Method:      "workspace.memory.graph.list",
		Description: "List derived memory-graph nodes from the compatibility graph projection over workspace memory, knowledge claims, and episode-pack state together with the shared workspace time-authority envelope, explicit compatibility-boundary metadata, and machine-readable projection-lag coverage for the filtered origin scope",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"memory_type":      {Type: "string", Required: false},
			"memory_layer":     {Type: "string", Required: false, Enum: []string{"EPISODIC", "SEMANTIC", "PROCEDURAL", "IDENTITY", "ARCHIVE"}},
			"visibility":       {Type: "string", Required: false, Enum: []string{"PRIVATE", "COALITION", "CLUSTER", "WORKSPACE"}},
			"epistemic_status": {Type: "string", Required: false, Enum: []string{"ALLEGED", "SUPPORTED", "VERIFIED", "DISPUTED", "RETRACTED"}},
			"lifecycle_state":  {Type: "string", Required: false, Enum: []string{"ACTIVE", "DORMANT", "SUPERSEDED", "ARCHIVED"}},
			"origin_kind":      {Type: "string", Required: false},
			"origin_id":        {Type: "string", Required: false},
			"source_kind":      {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"include_archived": {Type: "boolean", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.memory.graph.get": {
		Method:      "workspace.memory.graph.get",
		Description: "Get one derived memory-graph node from the compatibility graph projection, including refs, versions, edges, drift diagnostics, the shared workspace time-authority pair, explicit compatibility-boundary metadata, and structured projection-missing details when canonical knowledge-claim state exists but the derived node has not yet materialized",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"memory_id":    {Type: "string", Required: true},
		},
	},
	"workspace.memory.graph.atlas": {
		Method:      "workspace.memory.graph.atlas",
		Description: "Get a budgeted memory-first atlas subgraph for workspace exploration, returning filtered memory nodes, memory-to-memory edges, optional lineage hints, and optional faint runtime anchors",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"center_memory_id": {Type: "string", Required: false, Description: "Optional memory node id to center the atlas on"},
			"query":            {Type: "string", Required: false, Description: "Optional lexical seed for atlas overview/focus selection"},
			"memory_type":      {Type: "string", Required: false},
			"memory_layer":     {Type: "string", Required: false, Enum: []string{"EPISODIC", "SEMANTIC", "PROCEDURAL", "IDENTITY", "ARCHIVE"}},
			"visibility":       {Type: "string", Required: false, Enum: []string{"PRIVATE", "COALITION", "CLUSTER", "WORKSPACE"}},
			"epistemic_status": {Type: "string", Required: false, Enum: []string{"ALLEGED", "SUPPORTED", "VERIFIED", "DISPUTED", "RETRACTED"}},
			"lifecycle_state":  {Type: "string", Required: false, Enum: []string{"ACTIVE", "DORMANT", "SUPERSEDED", "ARCHIVED"}},
			"origin_kind":      {Type: "string", Required: false},
			"include_anchors":  {Type: "boolean", Required: false},
			"include_archived": {Type: "boolean", Required: false},
			"canonical_only":   {Type: "boolean", Required: false},
			"depth":            {Type: "integer", Required: false, Default: "1"},
			"limit_nodes":      {Type: "integer", Required: false, Default: "80"},
			"limit_edges":      {Type: "integer", Required: false, Default: "140"},
			"min_importance":   {Type: "number", Required: false},
			"min_activation":   {Type: "number", Required: false},
		},
	},
	"workspace.memory.graph.sync": {
		Method:      "workspace.memory.graph.sync",
		Description: "Backfill or resync the derived memory-graph compatibility projection from current workspace memory, knowledge claims, and episode packs without promoting the graph to canonical authority",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"workspace.memory.graph.repair": {
		Method:      "workspace.memory.graph.repair",
		Description: "Repair bounded workspace-memory compatibility-anchor drift without promoting the derived graph to canonical authority",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"memory_id":    {Type: "string", Required: false, Description: "Optional canonical workspace_memory id to repair explicitly"},
			"limit":        {Type: "integer", Required: false, Default: "25"},
		},
	},
	"workspace.memory.node.search": {
		Method:      "workspace.memory.node.search",
		Description: "Search derived memory-graph nodes through a compact agent-facing hit format over the existing compatibility projection, qualified by the shared workspace time-authority pair plus explicit compatibility-boundary and projection-lag metadata so stale or partial derived search windows cannot masquerade as canonical freshness",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"query":            {Type: "string", Required: true, Description: "Lexical needle matched against title, summary, body, and claim fields"},
			"memory_type":      {Type: "string", Required: false},
			"memory_layer":     {Type: "string", Required: false, Enum: []string{"EPISODIC", "SEMANTIC", "PROCEDURAL", "IDENTITY", "ARCHIVE"}},
			"visibility":       {Type: "string", Required: false, Enum: []string{"PRIVATE", "COALITION", "CLUSTER", "WORKSPACE"}},
			"epistemic_status": {Type: "string", Required: false, Enum: []string{"ALLEGED", "SUPPORTED", "VERIFIED", "DISPUTED", "RETRACTED"}},
			"lifecycle_state":  {Type: "string", Required: false, Enum: []string{"ACTIVE", "DORMANT", "SUPERSEDED", "ARCHIVED"}},
			"origin_kind":      {Type: "string", Required: false},
			"origin_id":        {Type: "string", Required: false},
			"source_kind":      {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"include_archived": {Type: "boolean", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.graph.snapshot": {
		Method:      "workspace.graph.snapshot",
		Description: "Get a complete Workspace Graph snapshot (nodes, edges, stats) matching the specified focus and mode",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"mode":         {Type: "string", Required: false, Enum: []string{"SYSTEM", "TASK_FOCUS", "CONTROL", "MEMORY_OVERLAY"}, Default: "SYSTEM"},
			"focus_id":     {Type: "string", Required: false, Description: "Center node ID for TASK_FOCUS or CONTROL modes"},
			"limit":        {Type: "integer", Required: false, Default: "1000"},
		},
	},
	"workspace.memory.residency.report": {
		Method:      "workspace.memory.residency.report",
		Description: "Record a non-authoritative local memory residency snapshot for an agent; read-side responses carry the shared workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"agent_id":            {Type: "string", Required: true},
			"report_id":           {Type: "string", Required: false},
			"session_id":          {Type: "string", Required: false},
			"report_scope":        {Type: "string", Required: false, Enum: []string{"AGENT", "SESSION", "CLUSTER"}},
			"p1_entry_count":      {Type: "integer", Required: false},
			"p2_entry_count":      {Type: "integer", Required: false},
			"p3_entry_count":      {Type: "integer", Required: false},
			"hot_hit_rate":        {Type: "number", Required: false},
			"persistent_hit_rate": {Type: "number", Required: false},
			"cluster_hit_rate":    {Type: "number", Required: false},
			"stale_read_rate":     {Type: "number", Required: false},
			"offload_ratio":       {Type: "number", Required: false},
			"notes":               {Type: "object", Required: false},
			"replicas":            {Type: "array", Required: false},
		},
	},
	"workspace.memory.residency.list": {
		Method:      "workspace.memory.residency.list",
		Description: "List non-authoritative local memory residency reports recorded for workspace agents together with the shared workspace time-authority envelope",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"report_scope": {Type: "string", Required: false, Enum: []string{"AGENT", "SESSION", "CLUSTER"}},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.memory.residency.get": {
		Method:      "workspace.memory.residency.get",
		Description: "Get a recorded local memory residency snapshot with replica states, version guards, and the shared workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"report_id":    {Type: "string", Required: true},
		},
	},
	"workspace.memory.metrics.report": {
		Method:      "workspace.memory.metrics.report",
		Description: "Record a non-authoritative local memory metrics snapshot with bounded access/usefulness counters; the stored/read-side result is still qualified by the current workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id":              {Type: "string", Required: true},
			"agent_id":                  {Type: "string", Required: true},
			"report_id":                 {Type: "string", Required: false},
			"session_id":                {Type: "string", Required: false},
			"report_scope":              {Type: "string", Required: false, Enum: []string{"AGENT", "SESSION", "CLUSTER"}},
			"window_started_at":         {Type: "string", Required: false, Description: "Inclusive RFC3339 timestamp for the metrics window start"},
			"window_ended_at":           {Type: "string", Required: false, Description: "Inclusive RFC3339 timestamp for the metrics window end"},
			"lookup_count":              {Type: "integer", Required: false},
			"l1_hit_count":              {Type: "integer", Required: false},
			"l2_hit_count":              {Type: "integer", Required: false},
			"p3_hit_count":              {Type: "integer", Required: false},
			"stale_hit_count":           {Type: "integer", Required: false},
			"promotion_count":           {Type: "integer", Required: false},
			"promotion_reuse_count":     {Type: "integer", Required: false},
			"flush_count":               {Type: "integer", Required: false},
			"flush_positive_count":      {Type: "integer", Required: false},
			"local_consolidation_count": {Type: "integer", Required: false},
			"potential_shared_op_count": {Type: "integer", Required: false},
			"dissent_hit_count":         {Type: "integer", Required: false},
			"dissent_available_count":   {Type: "integer", Required: false},
			"pollution_count":           {Type: "integer", Required: false},
			"notes":                     {Type: "object", Required: false},
		},
	},
	"workspace.memory.metrics.list": {
		Method:      "workspace.memory.metrics.list",
		Description: "List non-authoritative local memory metrics snapshots recorded for workspace agents, together with the current workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"report_scope": {Type: "string", Required: false, Enum: []string{"AGENT", "SESSION", "CLUSTER"}},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.memory.metrics.get": {
		Method:      "workspace.memory.metrics.get",
		Description: "Get a recorded non-authoritative local memory metrics snapshot with derived hit, precision, utility, and offload ratios plus the current workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"report_id":    {Type: "string", Required: true},
		},
	},
	"workspace.memory.coherence.report": {
		Method:      "workspace.memory.coherence.report",
		Description: "Build a read-only memory coherence report from latest local metrics, residency, and invalidation queue state",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"report_scope": {Type: "string", Required: false, Enum: []string{"AGENT", "SESSION", "CLUSTER"}},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.memory.coherence.scope": {
		Method:      "workspace.memory.coherence.scope",
		Description: "Get one read-only memory coherence scope report for an addressed agent and optional session",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"session_id":   {Type: "string", Required: false},
			"report_scope": {Type: "string", Required: false, Enum: []string{"AGENT", "SESSION", "CLUSTER"}},
		},
	},
	"workspace.memory.coherence.snapshot": {
		Method:      "workspace.memory.coherence.snapshot",
		Description: "Record a synthetic replayable snapshot of the current read-only memory coherence report",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"report_scope": {Type: "string", Required: false, Enum: []string{"AGENT", "SESSION", "CLUSTER"}},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.rsp.belief.report": {
		Method:      "workspace.rsp.belief.report",
		Description: "Build a shadow-only RSP stage-1 belief calibration report over fact-like, decision, and blocker claims",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"claim_type":   {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.rsp.belief.claim": {
		Method:      "workspace.rsp.belief.claim",
		Description: "Get one shadow-only RSP stage-1 belief calibration item for a knowledge claim",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"claim_id":     {Type: "string", Required: true},
		},
	},
	"workspace.rsp.belief.snapshot": {
		Method:      "workspace.rsp.belief.snapshot",
		Description: "Record a synthetic replayable snapshot of the current shadow-only RSP stage-1 belief report",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"claim_type":   {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.rsp.capability.get": {
		Method:      "workspace.rsp.capability.get",
		Description: "Read the unified RSP rollout capability flags resolved for a workspace",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"workspace.rsp.capability.put": {
		Method:      "workspace.rsp.capability.put",
		Description: "Update the unified RSP rollout capability flags used to gate belief, shadow anomaly/state, governed hints, and live autonomics",
		Params: map[string]ParamSchema{
			"workspace_id":               {Type: "string", Required: true},
			"belief_live":                {Type: "boolean", Required: false},
			"anomaly_shadow":             {Type: "boolean", Required: false},
			"state_shadow":               {Type: "boolean", Required: false},
			"forecast_shadow":            {Type: "boolean", Required: false},
			"safe_local_autonomics_live": {Type: "boolean", Required: false},
			"governed_hints_live":        {Type: "boolean", Required: false},
			"strong_consequences_live":   {Type: "boolean", Required: false},
			"updated_by":                 {Type: "string", Required: true},
			"reason":                     {Type: "string", Required: false},
		},
	},
	"workspace.rsp.state.report": {
		Method:      "workspace.rsp.state.report",
		Description: "Build a shadow-only RSP anomaly/state report over the current agent or cluster locus without introducing policy authority",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"doc_keys":         {Type: "array[string]", Required: false},
			"artifact_refs":    {Type: "array[string]", Required: false},
			"frontier_limit":   {Type: "integer", Required: false, Default: "3"},
		},
	},
	"workspace.rsp.state.snapshot": {
		Method:      "workspace.rsp.state.snapshot",
		Description: "Record a synthetic replayable snapshot of the current shadow-only RSP anomaly/state report",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"doc_keys":         {Type: "array[string]", Required: false},
			"artifact_refs":    {Type: "array[string]", Required: false},
			"frontier_limit":   {Type: "integer", Required: false, Default: "3"},
		},
	},
	"workspace.rsp.forecast.report": {
		Method:      "workspace.rsp.forecast.report",
		Description: "Build a public inspectable shadow-only RSP stage-2 forecast report over the current locus using the existing heuristic forecast read model",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"doc_keys":         {Type: "array[string]", Required: false},
			"artifact_refs":    {Type: "array[string]", Required: false},
			"frontier_limit":   {Type: "integer", Required: false, Default: "3"},
		},
	},
	"workspace.rsp.forecast.snapshot": {
		Method:      "workspace.rsp.forecast.snapshot",
		Description: "Record a rollout-gated synthetic replayable snapshot of the current shadow-only RSP stage-2 forecast report",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"doc_keys":         {Type: "array[string]", Required: false},
			"artifact_refs":    {Type: "array[string]", Required: false},
			"frontier_limit":   {Type: "integer", Required: false, Default: "3"},
		},
	},
	"workspace.memory.invalidation.poll": {
		Method:      "workspace.memory.invalidation.poll",
		Description: "Poll pending memory invalidations for an agent based on stored residency version guards, with the shared workspace time-authority envelope",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"agent_id":       {Type: "string", Required: true},
			"session_id":     {Type: "string", Required: false},
			"include_acked":  {Type: "boolean", Required: false},
			"limit":          {Type: "integer", Required: false, Default: "50"},
			"mark_delivered": {Type: "boolean", Required: false, Default: "false"},
		},
	},
	"workspace.memory.invalidation.ack": {
		Method:      "workspace.memory.invalidation.ack",
		Description: "Acknowledge one or more memory invalidations for an agent; responses stay qualified by the shared workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"agent_id":         {Type: "string", Required: true},
			"invalidation_ids": {Type: "array[string]", Required: true},
		},
	},
	"workspace.memory.invalidation.fail": {
		Method:      "workspace.memory.invalidation.fail",
		Description: "Record a failed invalidation application attempt and dead-letter after the bounded retry threshold under the shared workspace time-authority contract",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"agent_id":         {Type: "string", Required: true},
			"invalidation_ids": {Type: "array[string]", Required: true},
			"failure_reason":   {Type: "string", Required: false},
		},
	},
	"workspace.memory.invalidation.requeue": {
		Method:      "workspace.memory.invalidation.requeue",
		Description: "Re-open one or more dead-lettered invalidations for the addressed agent without changing canonical memory truth, under the shared workspace time-authority contract",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"agent_id":         {Type: "string", Required: true},
			"invalidation_ids": {Type: "array[string]", Required: true},
		},
	},
	"workspace.memory.invalidation.list": {
		Method:      "workspace.memory.invalidation.list",
		Description: "List pending or acknowledged memory invalidations for an agent without mutating delivery state, together with the shared workspace time-authority envelope",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"agent_id":            {Type: "string", Required: true},
			"session_id":          {Type: "string", Required: false},
			"include_acked":       {Type: "boolean", Required: false},
			"include_dead_letter": {Type: "boolean", Required: false},
			"limit":               {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.memory.invalidation.get": {
		Method:      "workspace.memory.invalidation.get",
		Description: "Get one durable memory invalidation record for the addressed agent with the shared workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id":    {Type: "string", Required: true},
			"agent_id":        {Type: "string", Required: true},
			"invalidation_id": {Type: "string", Required: true},
		},
	},
	"workspace.memory.invalidation.cursor.get": {
		Method:      "workspace.memory.invalidation.cursor.get",
		Description: "Get the durable invalidation delivery checkpoint for an agent and optional session scope with the shared workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"session_id":   {Type: "string", Required: false},
		},
	},
	"workspace.episode.pack.list": {
		Method:      "workspace.episode.pack.list",
		Description: "List canonical episode packs derived from session compaction and bounded runtime episodes",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"pack_type":    {Type: "string", Required: false},
			"pack_mode":    {Type: "string", Required: false, Enum: []string{"COMPLETE", "DETERMINISTIC_FALLBACK"}},
			"session_id":   {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.episode.pack.get": {
		Method:      "workspace.episode.pack.get",
		Description: "Get a canonical episode pack with structured ledgers, provenance, and compatibility links",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"pack_id":      {Type: "string", Required: true},
		},
	},
	"workspace.episode.pack.sync": {
		Method:      "workspace.episode.pack.sync",
		Description: "Backfill canonical episode packs from legacy session compaction snapshots",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"workspace.events.list": {
		Method:      "workspace.events.list",
		Description: "List append-only runtime journal events for the workspace control plane",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"event_type":   {Type: "string", Required: false},
			"entity_type":  {Type: "string", Required: false},
			"entity_id":    {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.events.replay": {
		Method:      "workspace.events.replay",
		Description: "Replay append-only runtime journal events into current coordination, queue, claim, and execution state, alongside replica coverage/freshness rows when replicated progress exists",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"agent_id":       {Type: "string", Required: false},
			"session_id":     {Type: "string", Required: false},
			"task_id":        {Type: "string", Required: false},
			"limit":          {Type: "integer", Required: false, Default: "500"},
			"include_events": {Type: "boolean", Required: false, Default: "false"},
		},
	},
	"workspace.events.evaluate": {
		Method:      "workspace.events.evaluate",
		Description: "Evaluate the replayed runtime journal and return verdict, metrics, findings, and replica coverage/freshness state; local follower lag or degraded apply state keeps replay scope explicitly partial without implying global canonical read readiness",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"agent_id":       {Type: "string", Required: false},
			"session_id":     {Type: "string", Required: false},
			"task_id":        {Type: "string", Required: false},
			"limit":          {Type: "integer", Required: false, Default: "500"},
			"include_events": {Type: "boolean", Required: false, Default: "false", Description: "Accepted for filter parity; ignored by evaluate output"},
		},
	},
	"workspace.authority.status": {
		Method:      "workspace.authority.status",
		Description: "Return the current single-host local authority status for a workspace, including local authority node identity, current authority row when present, runtime journal head, and a machine-readable lease_state such as missing, healthy, renew_due, stale, or foreign_live.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"scope":        {Type: "string", Required: false, Default: "workspace"},
		},
	},
	"workspace.authority.ensure_local": {
		Method:      "workspace.authority.ensure_local",
		Description: "Ensure the local single-host authority holder can safely proceed. This claims missing authority, renews the local lease when it is nearing expiry, reclaims stale authority, and fail-closes when another live holder still owns the workspace.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"scope":        {Type: "string", Required: false, Default: "workspace"},
			"actor_type":   {Type: "string", Required: false, Enum: []string{"operator", "system"}, Default: "operator"},
			"actor_id":     {Type: "string", Required: false, Description: "Operator or system actor recorded on authority lifecycle events"},
		},
	},
	"workspace.authority.force_break": {
		Method:      "workspace.authority.force_break",
		Description: "Force-expire the current workspace authority row for a wedged single-host holder. Intended as an operator recovery path when a live holder must be broken before local reclaim can proceed.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"scope":        {Type: "string", Required: false, Default: "workspace"},
			"actor_type":   {Type: "string", Required: false, Enum: []string{"operator", "system"}, Default: "operator"},
			"actor_id":     {Type: "string", Required: false, Description: "Operator or system actor recorded on the force-break event"},
		},
	},
	"workspace.instrumentation.report": {
		Method:      "workspace.instrumentation.report",
		Description: "Build a proto-cluster instrumentation report over runtime journal activity and derived runtime state",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"agent_id":      {Type: "string", Required: false},
			"session_id":    {Type: "string", Required: false},
			"task_id":       {Type: "string", Required: false},
			"limit":         {Type: "integer", Required: false, Default: "500"},
			"cluster_limit": {Type: "integer", Required: false, Default: "20"},
			"actor_id":      {Type: "string", Required: false, Description: "Accepted for param parity; ignored by report-only reads"},
		},
	},
	"workspace.instrumentation.clusters": {
		Method:      "workspace.instrumentation.clusters",
		Description: "List derived proto-clusters ranked by runtime hotspot activity",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"agent_id":      {Type: "string", Required: false},
			"session_id":    {Type: "string", Required: false},
			"task_id":       {Type: "string", Required: false},
			"limit":         {Type: "integer", Required: false, Default: "500"},
			"cluster_limit": {Type: "integer", Required: false, Default: "20"},
			"actor_id":      {Type: "string", Required: false, Description: "Accepted for param parity; ignored by cluster-only reads"},
		},
	},
	"workspace.instrumentation.snapshot": {
		Method:      "workspace.instrumentation.snapshot",
		Description: "Record a synthetic cluster.metric_snapshot runtime event from the current proto-cluster instrumentation report",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"agent_id":      {Type: "string", Required: false},
			"session_id":    {Type: "string", Required: false},
			"task_id":       {Type: "string", Required: false},
			"limit":         {Type: "integer", Required: false, Default: "500"},
			"cluster_limit": {Type: "integer", Required: false, Default: "20"},
			"actor_id":      {Type: "string", Required: false},
		},
	},
	"workspace.instrumentation.locus.bundle": {
		Method:      "workspace.instrumentation.locus.bundle",
		Description: "Resolve and bundle the current proto-cluster locus for a task/session/agent attachment, including tension, corridor, and control read-side context as a single read-only packet",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false, Description: "Optional explicit proto-cluster anchor; when present the bundle skips attachment resolution"},
			"agent_id":         {Type: "string", Required: false, Description: "Optional agent anchor used during locus resolution"},
			"task_id":          {Type: "string", Required: false, Description: "Optional task anchor used during locus resolution"},
			"session_id":       {Type: "string", Required: false, Description: "Optional session anchor used during locus resolution"},
			"doc_keys":         {Type: "array[string]", Required: false, Description: "Optional doc anchors used during locus resolution"},
			"artifact_refs":    {Type: "array[string]", Required: false, Description: "Optional artifact anchors used during locus resolution"},
			"frontier_limit":   {Type: "integer", Required: false, Default: "3", Description: "Limit for the scoped tension frontier included in the bundle"},
			"memory_budget":    {Type: "object", Required: false, Description: "Optional budget spanning structured retrieval lanes bounds"},
		},
	},
	"workspace.instrumentation.corridor.report": {
		Method:      "workspace.instrumentation.corridor.report",
		Description: "Build a read-only task-class and corridor-readiness report over current proto-clusters; this is a precursor to corridor-based policy, not policy authority",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by report-only reads"},
		},
	},
	"workspace.instrumentation.corridor.cluster": {
		Method:      "workspace.instrumentation.corridor.cluster",
		Description: "Inspect one proto-cluster's task-class hints, basis, and corridor-readiness approximation; read-only and separate from policy governance",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: true},
			"limit":            {Type: "integer", Required: false, Default: "20", Description: "Accepted for param parity; ignored by cluster detail reads"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by detail reads"},
		},
	},
	"workspace.instrumentation.corridor.snapshot": {
		Method:      "workspace.instrumentation.corridor.snapshot",
		Description: "Record a synthetic cluster.corridor_readiness_snapshot runtime event from the current read-only corridor readiness report; live SSE mirrors only the persisted event",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false},
		},
	},
	"workspace.instrumentation.corridor.fit.report": {
		Method:      "workspace.instrumentation.corridor.fit.report",
		Description: "Build a read-only corridor-fit report over task-class evidence, corridor catalog lookup, proto-cluster metrics, and confirmed tensions; this remains operator-facing and separate from applied policy",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by report-only reads"},
		},
	},
	"workspace.instrumentation.corridor.fit.cluster": {
		Method:      "workspace.instrumentation.corridor.fit.cluster",
		Description: "Inspect one proto-cluster's corridor-fit approximation, including metric gap breakdown and confirmed corroborating tensions, without turning it into policy authority",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: true},
			"limit":            {Type: "integer", Required: false, Default: "20", Description: "Accepted for param parity; ignored by cluster detail reads"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by detail reads"},
		},
	},
	"workspace.instrumentation.corridor.fit.snapshot": {
		Method:      "workspace.instrumentation.corridor.fit.snapshot",
		Description: "Record a synthetic cluster.corridor_fit_snapshot runtime event from the current read-only corridor-fit report; live SSE mirrors only the persisted event",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false},
		},
	},
	"workspace.instrumentation.corridor.ownership.report": {
		Method:      "workspace.instrumentation.corridor.ownership.report",
		Description: "Build a read-only task-first corridor ownership/basis report that resolves the current cluster-level owner basis before downstream fit or boundary diagnostics",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for shared param parity; ignored by read-only ownership reports"},
		},
	},
	"workspace.instrumentation.corridor.ownership.cluster": {
		Method:      "workspace.instrumentation.corridor.ownership.cluster",
		Description: "Inspect one proto-cluster's task-first corridor ownership digest, including ownership_state, owner_task_ids, basis_task_class, and supporting/conflicting task anchors, without creating policy authority",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: true},
			"limit":            {Type: "integer", Required: false, Default: "20", Description: "Accepted for shared param parity; ignored by cluster detail reads"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for shared param parity; ignored by detail reads"},
		},
	},
	"workspace.instrumentation.corridor.ownership.snapshot": {
		Method:      "workspace.instrumentation.corridor.ownership.snapshot",
		Description: "Record a synthetic cluster.corridor_ownership_snapshot runtime event from the current read-only corridor ownership/basis report; live SSE mirrors only the persisted event",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false},
		},
	},
	"workspace.instrumentation.corridor.boundary.report": {
		Method:      "workspace.instrumentation.corridor.boundary.report",
		Description: "Build a read-only fit-derived corridor-boundary report that separates basis_state from boundary_state and exposes nearest/violated metrics without turning the approximation into policy authority",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by report-only reads"},
		},
	},
	"workspace.instrumentation.corridor.boundary.cluster": {
		Method:      "workspace.instrumentation.corridor.boundary.cluster",
		Description: "Inspect one proto-cluster's fit-derived corridor-boundary digest, including basis_state, boundary_state, nearest metric/boundary, and signed violation digest, without creating applied policy",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: true},
			"limit":            {Type: "integer", Required: false, Default: "20", Description: "Accepted for param parity; ignored by cluster detail reads"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by detail reads"},
		},
	},
	"workspace.instrumentation.corridor.authority.report": {
		Method:      "workspace.instrumentation.corridor.authority.report",
		Description: "Build a task-first corridor-authority report over authored task_class evidence and derived task hints, independent of current proto-cluster visibility. Read-only and not policy authority.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: false, Description: "Optional scoped task anchor"},
			"limit":        {Type: "integer", Required: false, Default: "100"},
		},
	},
	"workspace.instrumentation.corridor.authority.task": {
		Method:      "workspace.instrumentation.corridor.authority.task",
		Description: "Inspect one task's corridor-authority basis, freshness, lookup, and current proto-cluster visibility. Read-only and separate from policy governance.",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true},
			"limit":        {Type: "integer", Required: false, Default: "1", Description: "Accepted for param parity; ignored by task detail reads"},
		},
	},
	"workspace.instrumentation.control.report": {
		Method:      "workspace.instrumentation.control.report",
		Description: "Build a read-only advisory control report from proto-cluster metrics and confirmed tensions, with pending tensions surfaced separately; separate from workspace.policy capability-governance APIs",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by report-only reads"},
		},
	},
	"workspace.instrumentation.control.cluster": {
		Method:      "workspace.instrumentation.control.cluster",
		Description: "Inspect one advisory control cluster with related active tensions, including clusters that currently exist only via persisted tensions",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: true},
			"limit":            {Type: "integer", Required: false, Default: "20", Description: "Accepted for param parity; ignored by cluster detail reads"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by detail reads"},
		},
	},
	"workspace.instrumentation.control.snapshot": {
		Method:      "workspace.instrumentation.control.snapshot",
		Description: "Record a synthetic cluster.control_advisory_snapshot runtime event from the current advisory control report; live SSE is mirrored only from the persisted runtime event",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false},
		},
	},
	"workspace.instrumentation.control.state.report": {
		Method:      "workspace.instrumentation.control.state.report",
		Description: "Build the current control-state interpretation scaffold over advisory control clusters; persisted rows are used when available and pre-tick clusters are returned as preview stabilized/candidate hints",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"mode":             {Type: "string", Required: false, Enum: []string{"STEADY", "ANTI_COLLAPSE", "COHERENCE", "DECENTRALIZE", "SYNERGY_SEEKING", "UNFREEZE", "STABILIZE"}, Description: "Filter by stabilized mode hint only; candidate mode hint remains visible in the returned state"},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by report-only reads"},
		},
	},
	"workspace.instrumentation.control.state.cluster": {
		Method:      "workspace.instrumentation.control.state.cluster",
		Description: "Inspect one control-state interpretation scaffold view together with its current advisory basis and durable control-state events; before the first tick this may still be preview state rather than a persisted row",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: true},
			"mode":             {Type: "string", Required: false, Enum: []string{"STEADY", "ANTI_COLLAPSE", "COHERENCE", "DECENTRALIZE", "SYNERGY_SEEKING", "UNFREEZE", "STABILIZE"}, Description: "Accepted for param parity with report/snapshot; ignored by cluster detail reads"},
			"limit":            {Type: "integer", Required: false, Description: "Accepted for param parity; ignored by cluster detail reads"},
			"actor_id":         {Type: "string", Required: false, Description: "Accepted for param parity; ignored by cluster detail reads"},
		},
	},
	"workspace.instrumentation.control.state.tick": {
		Method:      "workspace.instrumentation.control.state.tick",
		Description: "Advance the manual control-state interpretation scaffold for the current advisory cluster basis and persist cluster.control_state_ticked or cluster.control_state_stabilized runtime events; this does not auto-apply policy decisions",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"actor_id":         {Type: "string", Required: true},
			"mode":             {Type: "string", Required: false, Enum: []string{"STEADY", "ANTI_COLLAPSE", "COHERENCE", "DECENTRALIZE", "SYNERGY_SEEKING", "UNFREEZE", "STABILIZE"}, Description: "Accepted for param parity with report/snapshot; ignored by control ticks"},
			"limit":            {Type: "integer", Required: false, Description: "Accepted for param parity; ignored because tick uses a backend-fixed cluster window"},
		},
	},
	"workspace.instrumentation.control.state.snapshot": {
		Method:      "workspace.instrumentation.control.state.snapshot",
		Description: "Record a synthetic cluster.control_state_snapshot runtime event from the current control-state interpretation scaffold view, including preview state where no tick exists yet; live SSE is mirrored only from the persisted runtime event",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"mode":             {Type: "string", Required: false, Enum: []string{"STEADY", "ANTI_COLLAPSE", "COHERENCE", "DECENTRALIZE", "SYNERGY_SEEKING", "UNFREEZE", "STABILIZE"}, Description: "Filter by stabilized mode hint only; candidate mode hint remains visible in the returned state"},
			"limit":            {Type: "integer", Required: false, Default: "20"},
			"actor_id":         {Type: "string", Required: false},
		},
	},
	"workspace.instrumentation.unified.control.report": {
		Method:      "workspace.instrumentation.unified.control.report",
		Description: "Build a read-only unified arbitration report that combines RRP control-state, RMP memory coherence, and RSP governed hints in control-application order without applying live mutations",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"doc_keys":         {Type: "array", Required: false},
			"artifact_refs":    {Type: "array", Required: false},
			"frontier_limit":   {Type: "integer", Required: false, Default: "3"},
		},
	},
	"workspace.instrumentation.unified.control.snapshot": {
		Method:      "workspace.instrumentation.unified.control.snapshot",
		Description: "Record a synthetic unified-control snapshot runtime event from the current unified-control report; advisory reports emit cluster.unified_control_advisory_snapshot while active pilot effective-controls emit cluster.unified_control_effective_snapshot, and neither path applies live control mutations by itself",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"doc_keys":         {Type: "array", Required: false},
			"artifact_refs":    {Type: "array", Required: false},
			"frontier_limit":   {Type: "integer", Required: false, Default: "3"},
			"actor_id":         {Type: "string", Required: false},
		},
	},
	"workspace.tension.refresh": {
		Method:      "workspace.tension.refresh",
		Description: "Refresh the persisted tension overlay from the current proto-cluster instrumentation view and return an authority-qualified read-side report",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"actor_id":         {Type: "string", Required: true},
			"proto_cluster_id": {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "500"},
			"cluster_limit":    {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.tension.list": {
		Method:      "workspace.tension.list",
		Description: "List persisted workspace tensions together with the current workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"tension_type":     {Type: "string", Required: false, Enum: []string{"failure", "contradiction", "gap", "ambiguity", "bottleneck", "bridge", "fork_candidate", "dissent_followup", "repair", "review_scarcity", "cache_drift", "load_spike", "meta-tension"}},
			"lifecycle_state":  {Type: "string", Required: false, Enum: []string{"EMERGENT", "ACTIVE", "DORMANT", "RESOLVED", "DISCARDED", "ARCHIVED", "SUPERSEDED", "DISPUTED", "RECOVERED"}},
			"review_status":    {Type: "string", Required: false, Enum: []string{"PENDING", "CONFIRMED", "DISCARDED"}},
			"proto_cluster_id": {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.tension.get": {
		Method:      "workspace.tension.get",
		Description: "Get a single persisted tension with evidence, linked runtime events, related entities, and the current workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tension_id":   {Type: "string", Required: true},
		},
	},
	"workspace.tension.frontier": {
		Method:      "workspace.tension.frontier",
		Description: "List the current surfaced tension frontier together with the current workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"tension_type":     {Type: "string", Required: false, Enum: []string{"failure", "contradiction", "gap", "ambiguity", "bottleneck", "bridge", "fork_candidate", "dissent_followup", "repair", "review_scarcity", "cache_drift", "load_spike", "meta-tension"}},
			"lifecycle_state":  {Type: "string", Required: false, Enum: []string{"EMERGENT", "ACTIVE", "DORMANT", "RESOLVED", "DISCARDED", "ARCHIVED", "SUPERSEDED", "DISPUTED", "RECOVERED"}, Default: "ACTIVE"},
			"review_status":    {Type: "string", Required: false, Enum: []string{"PENDING", "CONFIRMED", "DISCARDED"}},
			"proto_cluster_id": {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "10"},
		},
	},
	"workspace.tension.confirm": {
		Method:      "workspace.tension.confirm",
		Description: "Persist tension.confirmed for a projected tension and mirror it to live SSE",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tension_id":   {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.tension.discard": {
		Method:      "workspace.tension.discard",
		Description: "Persist tension.discarded for a projected tension and mirror it to live SSE",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tension_id":   {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.tension.archive": {
		Method:      "workspace.tension.archive",
		Description: "Persist tension.archived for a projected tension and mirror it to live SSE",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tension_id":   {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.tension.lifecycle.update": {
		Method:      "workspace.tension.lifecycle.update",
		Description: "Compatibility facade for supported tension lifecycle transitions, with durable prompt-context evidence and live SSE parity",
		Params: map[string]ParamSchema{
			"workspace_id":    {Type: "string", Required: true},
			"tension_id":      {Type: "string", Required: true},
			"lifecycle_state": {Type: "string", Required: true, Enum: []string{"RESOLVED", "DISCARDED", "ARCHIVED"}},
			"updated_by":      {Type: "string", Required: true},
			"reason":          {Type: "string", Required: false},
		},
	},
	"workspace.tension.resolve": {
		Method:      "workspace.tension.resolve",
		Description: "Transition an active or dormant tension to RESOLVED and mirror it to live SSE",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tension_id":   {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.tension.dormant": {
		Method:      "workspace.tension.dormant",
		Description: "Transition an active tension to DORMANT and mirror it to live SSE",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tension_id":   {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.tension.add.dependency": {
		Method:      "workspace.tension.add.dependency",
		Description: "Add a structural dependency edge between two persisted workspace tensions and mirror the durable mutation to live SSE",
		Params: map[string]ParamSchema{
			"workspace_id":          {Type: "string", Required: true},
			"tension_id":            {Type: "string", Required: true},
			"depends_on_tension_id": {Type: "string", Required: true},
			"dependency_type":       {Type: "string", Required: false, Default: "BLOCKS"},
			"actor_id":              {Type: "string", Required: true},
			"reason":                {Type: "string", Required: false},
		},
	},
	"workspace.tension.remove.dependency": {
		Method:      "workspace.tension.remove.dependency",
		Description: "Remove a structural dependency edge between two persisted workspace tensions and mirror the durable mutation to live SSE",
		Params: map[string]ParamSchema{
			"workspace_id":          {Type: "string", Required: true},
			"tension_id":            {Type: "string", Required: true},
			"depends_on_tension_id": {Type: "string", Required: true},
			"dependency_type":       {Type: "string", Required: false, Default: "BLOCKS"},
			"actor_id":              {Type: "string", Required: true},
			"reason":                {Type: "string", Required: false},
		},
	},
	"workspace.tension.condense": {
		Method:      "workspace.tension.condense",
		Description: "Manual trigger for SCC condensation on the current tension dependency graph, with durable prompt-context evidence and live runtime-event parity",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true, Description: "Authenticated actor requesting condensation"},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.tension.attachable.list": {
		Method:      "workspace.tension.attachable.list",
		Description: "List currently attachable workspace tensions for one agent, including coalition-fit factors",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
		},
	},
	"workspace.tension.agent.attach": {
		Method:      "workspace.tension.agent.attach",
		Description: "Attach an agent to a tension coalition with durable prompt-context evidence and live runtime-event parity",
		Params: map[string]ParamSchema{
			"workspace_id":      {Type: "string", Required: true},
			"tension_id":        {Type: "string", Required: true},
			"agent_id":          {Type: "string", Required: true, Description: "Agent being attached to the coalition"},
			"actor_id":          {Type: "string", Required: true, Description: "Authenticated actor requesting the attach"},
			"success_criterion": {Type: "string", Required: false},
			"reason":            {Type: "string", Required: false},
		},
	},
	"workspace.tension.agent.detach": {
		Method:      "workspace.tension.agent.detach",
		Description: "Detach an agent from a tension coalition with durable prompt-context evidence and live runtime-event parity",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"coalition_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true, Description: "Agent being detached from the coalition"},
			"actor_id":     {Type: "string", Required: true, Description: "Authenticated actor requesting the detach"},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.ops.upsert": {
		Method:      "workspace.ops.upsert",
		Description: "Create or update a first-class operator queue item for blockers, decisions, or handoff obligations",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"queue_id":            {Type: "string", Required: false},
			"queue_key":           {Type: "string", Required: true, Description: "Stable idempotency key inside the workspace"},
			"queue_type":          {Type: "string", Required: false, Enum: []string{"BLOCKER", "DECISION", "HANDOFF", "FOLLOW_UP"}, Default: "BLOCKER"},
			"title":               {Type: "string", Required: true},
			"summary":             {Type: "string", Required: false},
			"details":             {Type: "string", Required: false},
			"assigned_to":         {Type: "string", Required: false},
			"urgency":             {Type: "string", Required: false, Enum: []string{"LOW", "NORMAL", "HIGH", "CRITICAL"}, Default: "NORMAL"},
			"source_kind":         {Type: "string", Required: false},
			"source_id":           {Type: "string", Required: false},
			"task_id":             {Type: "string", Required: false},
			"session_id":          {Type: "string", Required: false},
			"agent_id":            {Type: "string", Required: false},
			"keep_session_active": {Type: "boolean", Required: false},
			"due_at":              {Type: "string", Required: false},
			"current_revision":    {Type: "integer", Required: false, Description: "Preferred base-version token for optimistic concurrency on the current queue revision."},
			"current_updated_at":  {Type: "string", Required: false, Description: "Legacy optimistic-concurrency token; reject stale queue snapshots when explicit current_revision is not available."},
		},
	},
	"workspace.ops.request": {
		Method:      "workspace.ops.request",
		Description: "Create a typed external-gate request for credential/auth, payment/billing, or explicit approval flows",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"request_key":         {Type: "string", Required: true, Description: "Stable idempotency key for the gate request"},
			"gate_type":           {Type: "string", Required: true, Enum: []string{"CREDENTIAL_AUTH", "PAYMENT_BILLING", "EXPLICIT_APPROVAL"}},
			"title":               {Type: "string", Required: true},
			"summary":             {Type: "string", Required: false},
			"details":             {Type: "string", Required: false},
			"assigned_to":         {Type: "string", Required: false},
			"urgency":             {Type: "string", Required: false, Enum: []string{"LOW", "NORMAL", "HIGH", "CRITICAL"}, Description: "Optional override; defaults are derived from gate_type"},
			"source_kind":         {Type: "string", Required: false, Default: "external_gate"},
			"source_id":           {Type: "string", Required: false, Description: "Optional external source identifier; defaults to request_key"},
			"task_id":             {Type: "string", Required: false},
			"session_id":          {Type: "string", Required: false},
			"agent_id":            {Type: "string", Required: false},
			"keep_session_active": {Type: "boolean", Required: false, Default: "true"},
			"due_at":              {Type: "string", Required: false},
			"current_revision":    {Type: "integer", Required: false, Description: "Preferred base-version token for refreshing an existing gate queue once the current queue revision has advanced beyond its initial create."},
			"current_updated_at":  {Type: "string", Required: false, Description: "Legacy optimistic-concurrency token for refreshing an existing gate queue when explicit current_revision is not available."},
		},
	},
	"workspace.ops.list": {
		Method:      "workspace.ops.list",
		Description: "List operator queue items derived from or attached to current runtime work, qualified by the current workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"queue_type":   {Type: "string", Required: false},
			"status":       {Type: "string", Required: false, Enum: []string{"OPEN", "RESOLVED", "CANCELLED"}},
			"assigned_to":  {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.ops.get": {
		Method:      "workspace.ops.get",
		Description: "Fetch one operator queue item by queue_id or stable queue_key, including its current revision tokens",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"queue_id":     {Type: "string", Required: false},
			"queue_key":    {Type: "string", Required: false},
		},
	},
	"workspace.ops.resolve": {
		Method:      "workspace.ops.resolve",
		Description: "Resolve or cancel an operator queue obligation",
		Params: map[string]ParamSchema{
			"workspace_id":       {Type: "string", Required: true},
			"queue_id":           {Type: "string", Required: false},
			"queue_key":          {Type: "string", Required: false},
			"status":             {Type: "string", Required: false, Enum: []string{"RESOLVED", "CANCELLED"}, Default: "RESOLVED"},
			"resolved_by":        {Type: "string", Required: true},
			"resolution":         {Type: "string", Required: false},
			"current_revision":   {Type: "integer", Required: false, Description: "Preferred base-version token for optimistic concurrency on the current queue revision."},
			"current_updated_at": {Type: "string", Required: false, Description: "Legacy optimistic-concurrency token; reject stale queue snapshots when explicit current_revision is not available."},
		},
	},
	"workspace.ops.escalate": {
		Method:      "workspace.ops.escalate",
		Description: "Escalate an open operator queue item, optionally updating assignee, urgency, or due date",
		Params: map[string]ParamSchema{
			"workspace_id":       {Type: "string", Required: true},
			"queue_id":           {Type: "string", Required: false},
			"queue_key":          {Type: "string", Required: false},
			"escalated_by":       {Type: "string", Required: true},
			"reason":             {Type: "string", Required: false},
			"assigned_to":        {Type: "string", Required: false},
			"urgency":            {Type: "string", Required: false, Enum: []string{"LOW", "NORMAL", "HIGH", "CRITICAL"}},
			"due_at":             {Type: "string", Required: false},
			"current_revision":   {Type: "integer", Required: false, Description: "Preferred base-version token for optimistic concurrency on the current queue revision."},
			"current_updated_at": {Type: "string", Required: false, Description: "Legacy optimistic-concurrency token; reject stale queue snapshots when explicit current_revision is not available."},
		},
	},
	"workspace.claim.write": {
		Method:      "workspace.claim.write",
		Description: "Record a durable knowledge claim with canonical runtime provenance",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"claim_id":            {Type: "string", Required: false},
			"claim_type":          {Type: "string", Required: false, Enum: []string{"FACT", "DECISION", "LESSON", "PROCEDURE", "ANTI_PROCEDURE", "INCIDENT", "UPDATE_DIGEST", "BLOCKER", "CONSTRAINT", "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT", "HYPOTHESIS", "ALTERNATIVE_BRANCH", "ENTITY", "SUMMARY", "EXPERIENCE"}, Default: "FACT"},
			"status":              {Type: "string", Required: false, Enum: []string{"ACTIVE", "CONFIRMED", "REVIEW", "STALE", "SUPERSEDED", "DISPUTED"}, Default: "ACTIVE"},
			"subject":             {Type: "string", Required: true},
			"body":                {Type: "string", Required: true},
			"summary":             {Type: "string", Required: false},
			"confidence":          {Type: "number", Required: false},
			"source_kind":         {Type: "string", Required: false, Default: "manual"},
			"source_id":           {Type: "string", Required: false},
			"memory_id":           {Type: "string", Required: false},
			"task_id":             {Type: "string", Required: false},
			"session_id":          {Type: "string", Required: false},
			"agent_id":            {Type: "string", Required: false},
			"supersedes_claim_id": {Type: "string", Required: false},
			"conflicts_claim_id":  {Type: "string", Required: false},
			"evidence":            {Type: "array[string]", Required: false},
			"tags":                {Type: "array[string]", Required: false},
		},
	},
	"workspace.claim.list": {
		Method:      "workspace.claim.list",
		Description: "List durable workspace knowledge claims",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"claim_type":       {Type: "string", Required: false},
			"status":           {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"memory_id":        {Type: "string", Required: false},
			"source_kind":      {Type: "string", Required: false},
			"include_archived": {Type: "boolean", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.claim.links.list": {
		Method:      "workspace.claim.links.list",
		Description: "List typed semantic relations between durable knowledge claims",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"claim_id":      {Type: "string", Required: false},
			"from_claim_id": {Type: "string", Required: false},
			"to_claim_id":   {Type: "string", Required: false},
			"relation_type": {Type: "string", Required: false, Enum: []string{"SUPPORTS", "CONTRADICTS", "SUPERSEDES", "VALIDATED_BY", "BLOCKS", "RESOLVES"}},
			"limit":         {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.claim.review": {
		Method:      "workspace.claim.review",
		Description: "Mark a knowledge claim as needing review and open a follow-up queue item",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"claim_id":     {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
			"due_at":       {Type: "string", Required: false},
			"assigned_to":  {Type: "string", Required: false},
			"urgency":      {Type: "string", Required: false, Enum: []string{"LOW", "NORMAL", "HIGH", "CRITICAL"}},
		},
	},
	"workspace.claim.confirm": {
		Method:      "workspace.claim.confirm",
		Description: "Confirm a knowledge claim and resolve any open follow-up queue item",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"claim_id":     {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.claim.dispute": {
		Method:      "workspace.claim.dispute",
		Description: "Dispute a knowledge claim and open a follow-up queue item",
		Params: map[string]ParamSchema{
			"workspace_id":       {Type: "string", Required: true},
			"claim_id":           {Type: "string", Required: true},
			"actor_id":           {Type: "string", Required: true},
			"reason":             {Type: "string", Required: false},
			"due_at":             {Type: "string", Required: false},
			"assigned_to":        {Type: "string", Required: false},
			"urgency":            {Type: "string", Required: false, Enum: []string{"LOW", "NORMAL", "HIGH", "CRITICAL"}},
			"conflicts_claim_id": {Type: "string", Required: false},
		},
	},
	"workspace.claim.supersede": {
		Method:      "workspace.claim.supersede",
		Description: "Mark a knowledge claim as superseded by a newer claim",
		Params: map[string]ParamSchema{
			"workspace_id":         {Type: "string", Required: true},
			"claim_id":             {Type: "string", Required: true},
			"actor_id":             {Type: "string", Required: true},
			"superseding_claim_id": {Type: "string", Required: true},
			"reason":               {Type: "string", Required: false},
		},
	},
	"workspace.claim.stale": {
		Method:      "workspace.claim.stale",
		Description: "Mark a knowledge claim as stale and open a follow-up queue item",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"claim_id":     {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
			"due_at":       {Type: "string", Required: false},
			"assigned_to":  {Type: "string", Required: false},
			"urgency":      {Type: "string", Required: false, Enum: []string{"LOW", "NORMAL", "HIGH", "CRITICAL"}},
		},
	},
	"workspace.claim.escalate": {
		Method:      "workspace.claim.escalate",
		Description: "Escalate an active claim review workflow and update the underlying follow-up queue SLA",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"claim_id":     {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
			"assigned_to":  {Type: "string", Required: false},
			"urgency":      {Type: "string", Required: false, Enum: []string{"LOW", "NORMAL", "HIGH", "CRITICAL"}},
			"due_at":       {Type: "string", Required: false},
		},
	},
	"workspace.claim.search": {
		Method:      "workspace.claim.search",
		Description: "Search durable workspace knowledge claims using lexical retrieval",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"query":            {Type: "string", Required: true},
			"claim_type":       {Type: "string", Required: false},
			"status":           {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"session_id":       {Type: "string", Required: false},
			"task_id":          {Type: "string", Required: false},
			"memory_id":        {Type: "string", Required: false},
			"source_kind":      {Type: "string", Required: false},
			"include_archived": {Type: "boolean", Required: false},
			"limit":            {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.claim.archive": {
		Method:      "workspace.claim.archive",
		Description: "Archive a knowledge claim while retaining provenance and journal history",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"claim_id":     {Type: "string", Required: true},
			"archived_by":  {Type: "string", Required: true},
			"reason":       {Type: "string", Required: false},
		},
	},
	"workspace.execution.run.write": {
		Method:      "workspace.execution.run.write",
		Description: "Create or update a durable execution run that tracks plan/execution/verification state",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"run_id":       {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"title":        {Type: "string", Required: true},
			"summary":      {Type: "string", Required: false},
			"status":       {Type: "string", Required: false, Enum: []string{"PLANNED", "ACTIVE", "BLOCKED", "VERIFYING", "COMPLETED", "FAILED", "CANCELLED"}, Default: "PLANNED"},
			"outcome":      {Type: "string", Required: false},
			"verification": {Type: "object", Required: false},
		},
	},
	"workspace.execution.run.list": {
		Method:      "workspace.execution.run.list",
		Description: "List execution runs in the workspace together with the shared workspace time-authority envelope",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"status":       {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"session_id":   {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.execution.run.get": {
		Method:      "workspace.execution.run.get",
		Description: "Get one execution run together with its plan/execution/verification steps and the shared workspace time-authority pair",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"run_id":       {Type: "string", Required: true},
		},
	},
	"workspace.execution.agent_runs.cancel": {
		Method:      "workspace.execution.agent_runs.cancel",
		Description: "Cancel all nonterminal execution runs and steps for a stopped managed agent before reporting stop completion",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
			"summary":      {Type: "string", Required: false},
			"outcome":      {Type: "string", Required: false, Default: "STOPPED_BY_MANAGER"},
		},
	},
	"workspace.execution.step.write": {
		Method:      "workspace.execution.step.write",
		Description: "Create or update a step inside an execution run",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"step_id":        {Type: "string", Required: false},
			"run_id":         {Type: "string", Required: true},
			"parent_step_id": {Type: "string", Required: false},
			"phase":          {Type: "string", Required: false, Enum: []string{"PLAN", "EXECUTE", "VERIFY"}, Default: "PLAN"},
			"title":          {Type: "string", Required: true},
			"summary":        {Type: "string", Required: false},
			"status":         {Type: "string", Required: false, Enum: []string{"PENDING", "ACTIVE", "BLOCKED", "COMPLETED", "FAILED", "SKIPPED"}, Default: "PENDING"},
			"sort_order":     {Type: "integer", Required: false},
			"evidence":       {Type: "array[string]", Required: false},
			"verification":   {Type: "object", Required: false},
		},
	},
	"workspace.policy.put": {
		Method:      "workspace.policy.put",
		Description: "Create or update a workspace capability policy used to gate tool execution",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"policy_id":    {Type: "string", Required: false},
			"subject_type": {Type: "string", Required: true},
			"subject_id":   {Type: "string", Required: false, Description: "Defaults to *"},
			"capability":   {Type: "string", Required: false, Description: "Defaults to *"},
			"tool_id":      {Type: "string", Required: false, Description: "Defaults to *"},
			"effect":       {Type: "string", Required: false, Enum: []string{"ALLOW", "DENY", "REQUIRE_APPROVAL"}, Default: "ALLOW"},
			"reason":       {Type: "string", Required: false},
			"created_by":   {Type: "string", Required: true},
		},
	},
	"workspace.policy.list": {
		Method:      "workspace.policy.list",
		Description: "List capability policies configured for the workspace",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"subject_type": {Type: "string", Required: false},
			"subject_id":   {Type: "string", Required: false},
			"capability":   {Type: "string", Required: false},
			"tool_id":      {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.policy.check": {
		Method:      "workspace.policy.check",
		Description: "Evaluate capability policy for one actor/capability/tool tuple",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"subject_type": {Type: "string", Required: true},
			"subject_id":   {Type: "string", Required: true},
			"capability":   {Type: "string", Required: true},
			"tool_id":      {Type: "string", Required: false},
		},
	},
	"workspace.control.command.request": {
		Method:      "workspace.control.command.request",
		Description: "Record a canonical control-command request; exclusions may apply inline while other commands stay journaled for explicit review or execution",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"command_id":       {Type: "string", Required: false},
			"command_type":     {Type: "string", Required: true, Enum: []string{"tension.exclude_agent", "agent.control.refresh_kernel", "agent.control.flush_cache", "cluster.freeze", "workspace.throttle", "cluster.mode_switch"}},
			"scope":            {Type: "string", Required: false},
			"proto_cluster_id": {Type: "string", Required: false},
			"tension_id":       {Type: "string", Required: false},
			"agent_id":         {Type: "string", Required: false},
			"target_mode":      {Type: "string", Required: false},
			"ttl_seconds":      {Type: "integer", Required: false},
			"reason":           {Type: "string", Required: false},
			"requested_by":     {Type: "string", Required: true},
			"actor_type":       {Type: "string", Required: false, Description: "Optional control-command actor type. Only operator or system are accepted; protocol ownership is returned in the recorded command."},
			"parent_refs":      {Type: "array", Required: false},
		},
	},
	"workspace.agents.list": {
		Method:      "workspace.agents.list",
		Description: "List all agents in the workspace with their online status",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"workspace.sessions.list": {
		Method:      "workspace.sessions.list",
		Description: "List current active or recent session states in a workspace",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"active_only":  {Type: "boolean", Required: false, Default: "true"},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.nodes.list": {
		Method:      "workspace.nodes.list",
		Description: "List DAG nodes in the workspace",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: false, Description: "Filter by specific task"},
			"status":       {Type: "string", Required: false, Description: "Filter by status (e.g. PENDING, CLAIMED)"},
			"limit":        {Type: "integer", Required: false, Default: "50"},
		},
	},
	"workspace.agents.search": {
		Method:      "workspace.agents.search",
		Description: "Search agents by tags or capabilities",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tags":         {Type: "string", Required: false, Description: "Comma-separated tags to search for"},
		},
	},
	"workspace.tasks.list": {
		Method:      "workspace.tasks.list",
		Description: "List all tasks in the workspace, optionally filtered by project",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: false, Description: "Filter tasks by project ID"},
		},
	},
	"workspace.messages.list": {
		Method:      "workspace.messages.list",
		Description: "List recent messages in the workspace",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"workspace.updates.list": {
		Method:      "workspace.updates.list",
		Description: "List recent workspace updates/feed",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"workspace.search": {
		Method:      "workspace.search",
		Description: "Search across workspace docs, tasks, updates, tools, artifacts, memory, and knowledge claims",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"query":        {Type: "string", Required: true},
			"entity_type":  {Type: "string", Required: false, Description: "Optional entity filter: doc|task|update|tool|artifact|memory|claim"},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.compaction.candidates": {
		Method:      "workspace.compaction.candidates",
		Description: "List active session ledger entries that exceed canonical compaction thresholds",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: false},
			"active_only":  {Type: "boolean", Required: false, Default: "true"},
			"min_messages": {Type: "integer", Required: false, Default: "12"},
			"min_tokens":   {Type: "integer", Required: false, Default: "12000"},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},
	"workspace.compaction.snapshots": {
		Method:      "workspace.compaction.snapshots",
		Description: "List recorded session compaction snapshots from the canonical runtime ledger",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"session_id":   {Type: "string", Required: false},
			"agent_id":     {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "20"},
		},
	},

	// ── Tasks ──
	"task.submit": {
		Method:      "task.submit",
		Description: "Create a workspace-scoped task with a durable task.created runtime event",
		Params: map[string]ParamSchema{
			"workspace_id":          {Type: "string", Required: true, Description: "Workspace attachment target"},
			"task_id":               {Type: "string", Required: true},
			"title":                 {Type: "string", Required: true},
			"description":           {Type: "string", Required: false},
			"owner_user_id":         {Type: "string", Required: true},
			"priority":              {Type: "string", Required: false, Enum: []string{"low", "normal", "high", "critical"}, Default: "normal"},
			"task_kind":             {Type: "string", Required: false, Enum: []string{"EXECUTION", "COORDINATION"}, Default: "EXECUTION", Description: "Legacy execution/coordination switch; use project_lane for project phase/lane"},
			"task_template":         {Type: "string", Required: false, Enum: []string{"generic", "research", "bugfix", "deploy", "integration", "ops", "tooling"}, Default: "generic"},
			"task_class":            {Type: "string", Required: false, Description: "Optional explicit authored task class evidence", Enum: []string{"PROOF", "EXPLORATION", "INTEGRATION", "INCIDENT"}},
			"task_class_source":     {Type: "string", Required: false, Description: "Source for explicit authored task class evidence", Enum: []string{"EXPLICIT", "TEMPLATE_DEFAULT", "UNSET"}},
			"graph":                 {Type: "object", Required: false, Description: "Optional DAG graph payload"},
			"linked_by":             {Type: "string", Required: false, Description: "Actor recorded when attaching to a workspace"},
			"tags":                  {Type: "array[string]", Required: false},
			"project_id":            {Type: "string", Required: false, Description: "Link task to a project"},
			"project_lane":          {Type: "string", Required: false, Description: "Project coordination lane hint such as strategy, frontend, backend, integration, review, docs, or ops"},
			"requires_project_gate": {Type: "boolean", Required: false, Default: "false", Description: "Whether completion should wait for a project-level coordination gate"},
			"dependency_task_ids":   {Type: "array[string]", Required: false, Description: "Hard upstream task ids; each creates a BLOCKS link and prevents this task from being selected until the upstream task resolves"},
			"related_task_ids":      {Type: "array[string]", Required: false, Description: "Non-blocking related task ids; each creates a RELATES_TO link and does not affect claim/frontier scheduling"},
			"write_scope_hints":     {Type: "array[string]", Required: false, Description: "Optional intended repository/file scope hints for branch-based coordination"},
			"task_requirements":     {Type: "object", Required: false, Description: "Optional structured requirements envelope for fit/routing evidence such as work modes, skills, tools, claim policy, and acceptance ids"},
		},
	},
	"task.status": {
		Method:      "task.status",
		Description: "Get task status by task_id",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true},
		},
	},
	"task.class.put": {
		Method:      "task.class.put",
		Description: "Set or clear explicit authored task class evidence used by read-side corridor lookup; does not assign policy authority",
		Params: map[string]ParamSchema{
			"workspace_id":      {Type: "string", Required: true},
			"task_id":           {Type: "string", Required: true},
			"task_class":        {Type: "string", Required: false, Description: "Concrete authored task class. Omit or empty to clear explicit evidence", Enum: []string{"PROOF", "EXPLORATION", "INTEGRATION", "INCIDENT"}},
			"task_class_source": {Type: "string", Required: false, Description: "Authored class source. Use UNSET or omit when clearing", Enum: []string{"EXPLICIT", "TEMPLATE_DEFAULT", "UNSET"}},
			"actor_id":          {Type: "string", Required: false, Description: "Actor recorded in audit trail"},
		},
	},
	"task.project_fields.put": {
		Method:      "task.project_fields.put",
		Description: "Set or clear project linkage and project coordination taxonomy for an existing workspace task",
		Params: map[string]ParamSchema{
			"workspace_id":          {Type: "string", Required: true},
			"task_id":               {Type: "string", Required: true},
			"project_id":            {Type: "string", Required: false, Description: "Project id in the same workspace; empty string clears the link"},
			"task_kind":             {Type: "string", Required: false, Enum: []string{"EXECUTION", "COORDINATION"}, Description: "Legacy execution/coordination switch; use project_lane for project phase/lane"},
			"project_lane":          {Type: "string", Required: false, Description: "Project coordination lane hint"},
			"requires_project_gate": {Type: "boolean", Required: false},
			"actor_id":              {Type: "string", Required: false, Description: "Actor recorded in audit trail"},
		},
	},
	"task.close": {
		Method:      "task.close",
		Description: "Close or cancel a coordination task",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: false},
			"resolution":   {Type: "string", Required: false, Enum: []string{"RESOLVED", "FAILED", "CANCELLED"}, Default: "RESOLVED"},
			"reason":       {Type: "string", Required: false},
		},
	},

	// ── Projects ──
	"project.create": {
		Method:      "project.create",
		Description: "Create a new project in the workspace",
		Params: map[string]ParamSchema{
			"project_id":   {Type: "string", Required: true, Description: "Unique project identifier"},
			"workspace_id": {Type: "string", Required: true},
			"title":        {Type: "string", Required: true, Description: "Project title"},
			"description":  {Type: "string", Required: false, Description: "Project description"},
			"created_by":   {Type: "string", Required: true, Description: "Authenticated actor creating the project"},
		},
	},
	"project.list": {
		Method:      "project.list",
		Description: "List all projects in the workspace with task counts",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"project.get": {
		Method:      "project.get",
		Description: "Get detailed info about a specific project",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true},
		},
	},
	"project.update": {
		Method:      "project.update",
		Description: "Update project title, description, or status",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true},
			"title":        {Type: "string", Required: true, Description: "New project title"},
			"description":  {Type: "string", Required: false, Description: "New project description"},
			"status":       {Type: "string", Required: false, Description: "New status", Enum: []string{"ACTIVE", "ARCHIVED"}},
			"actor_id":     {Type: "string", Required: true, Description: "Human/system actor authorizing this project mutation"},
		},
	},
	"project.delete": {
		Method:      "project.delete",
		Description: "Delete a project and unlink its tasks",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project ID to delete"},
			"actor_id":     {Type: "string", Required: true, Description: "Human/system actor authorizing this project mutation"},
		},
	},
	"project.profile.get": {
		Method:      "project.profile.get",
		Description: "Read the project-centric autonomous workspace profile for coordination and phase-aware work selection",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project profile to read"},
		},
	},
	"project.profile.update": {
		Method:      "project.profile.update",
		Description: "Update the project-centric autonomous workspace profile and record durable runtime evidence once the storage authority API is linked",
		Params: map[string]ParamSchema{
			"workspace_id":               {Type: "string", Required: true},
			"project_id":                 {Type: "string", Required: true, Description: "Project profile to update"},
			"actor_id":                   {Type: "string", Required: true, Description: "Human/system actor authorizing this project profile mutation"},
			"goal":                       {Type: "string", Required: false, Description: "Project-level autonomous workspace goal"},
			"design_doc_id":              {Type: "string", Required: false, Description: "Design/spec document identifier for the project"},
			"implementation_plan_doc_id": {Type: "string", Required: false, Description: "Implementation plan document identifier for the project"},
			"repo_required":              {Type: "boolean", Required: false, Description: "Whether implementation requires a backing repository"},
			"repo_status":                {Type: "string", Required: false, Description: "Repository readiness status", Enum: []string{"NOT_REQUIRED", "MISSING", "READY", "BLOCKED", "UNKNOWN"}},
			"repo_url":                   {Type: "string", Required: false, Description: "Repository URL for implementation coordination"},
			"repo_default_branch":        {Type: "string", Required: false, Description: "Default branch for project implementation coordination"},
		},
	},
	"project.phase.transition": {
		Method:      "project.phase.transition",
		Description: "Transition a project between autonomous-work phases and record phase history plus a runtime event once the storage authority API is linked",
		Params: map[string]ParamSchema{
			"workspace_id":      {Type: "string", Required: true},
			"project_id":        {Type: "string", Required: true, Description: "Project whose phase should transition"},
			"actor_id":          {Type: "string", Required: true, Description: "Human/system actor authorizing this project phase transition"},
			"to_phase":          {Type: "string", Required: true, Description: "Target project phase"},
			"reason":            {Type: "string", Required: false, Description: "Reason or coordination note for the transition"},
			"coordination_mode": {Type: "string", Required: false, Description: "Optional coordination mode. trust_first records phase movement while treating unmet project gates as advisory.", Enum: []string{"strict", "trust_first"}, Default: "strict"},
		},
	},
	"project.gates.status": {
		Method:      "project.gates.status",
		Description: "Read the current project gate status for stable autonomous coordination",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project gate status to read"},
		},
	},
	"project.coordination.get": {
		Method:      "project.coordination.get",
		Description: "Read the project coordination snapshot used by project-centric autonomous workspaces",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project coordination snapshot to read"},
		},
	},
	"project.lead.claim": {
		Method:      "project.lead.claim",
		Description: "Claim the strategic lead lease for a project and record durable runtime evidence",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"project_id":    {Type: "string", Required: true, Description: "Project whose strategic lead should be claimed"},
			"actor_id":      {Type: "string", Required: true, Description: "Authenticated actor authorizing this project role mutation"},
			"agent_id":      {Type: "string", Required: true, Description: "Agent claiming the strategic lead role"},
			"lease_seconds": {Type: "integer", Required: false, Description: "Requested lease duration in seconds"},
			"lease_token":   {Type: "string", Required: false, Description: "Optional caller lease token for idempotent renewal/coordination"},
			"summary":       {Type: "string", Required: false, Description: "Coordination note for the lead claim"},
		},
	},
	"project.lead.renew": {
		Method:      "project.lead.renew",
		Description: "Renew an active project strategic lead lease and record durable runtime evidence",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"project_id":    {Type: "string", Required: true, Description: "Project whose strategic lead lease should renew"},
			"actor_id":      {Type: "string", Required: true, Description: "Authenticated actor authorizing this project role mutation"},
			"role_id":       {Type: "string", Required: true, Description: "Strategic lead role record to renew"},
			"lease_seconds": {Type: "integer", Required: false, Description: "Requested renewed lease duration in seconds"},
			"lease_token":   {Type: "string", Required: false, Description: "Optional caller lease token for renewal coordination"},
			"summary":       {Type: "string", Required: false, Description: "Coordination note for the renewal"},
		},
	},
	"project.lead.release": {
		Method:      "project.lead.release",
		Description: "Release an active project strategic lead role and record durable runtime evidence",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project whose strategic lead should be released"},
			"actor_id":     {Type: "string", Required: true, Description: "Authenticated actor authorizing this project role mutation"},
			"role_id":      {Type: "string", Required: true, Description: "Strategic lead role record to release"},
			"lease_token":  {Type: "string", Required: false, Description: "Optional caller lease token guard for release coordination"},
			"summary":      {Type: "string", Required: false, Description: "Coordination note for the release"},
		},
	},
	"project.lead.transfer": {
		Method:      "project.lead.transfer",
		Description: "Transfer the project strategic lead role to another agent and record durable runtime evidence",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"project_id":    {Type: "string", Required: true, Description: "Project whose strategic lead should transfer"},
			"actor_id":      {Type: "string", Required: true, Description: "Authenticated actor authorizing this project role mutation"},
			"role_id":       {Type: "string", Required: true, Description: "Strategic lead role record to transfer"},
			"to_agent_id":   {Type: "string", Required: true, Description: "Agent receiving the strategic lead role"},
			"lease_seconds": {Type: "integer", Required: false, Description: "Requested lease duration for the transferee in seconds"},
			"lease_token":   {Type: "string", Required: false, Description: "Optional caller lease token for transfer coordination"},
			"summary":       {Type: "string", Required: false, Description: "Coordination note for the transfer"},
		},
	},
	"project.role.assign": {
		Method:      "project.role.assign",
		Description: "Assign a non-lead project role to an agent and record durable runtime evidence",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"project_id":       {Type: "string", Required: true, Description: "Project receiving the role assignment"},
			"actor_id":         {Type: "string", Required: true, Description: "Authenticated actor authorizing this project role mutation"},
			"agent_id":         {Type: "string", Required: true, Description: "Agent receiving the role assignment"},
			"role_type":        {Type: "string", Required: true, Description: "Project role type to assign"},
			"write_scope_json": {Type: "string", Required: false, Description: "Optional JSON string describing the role's write scope"},
			"summary":          {Type: "string", Required: false, Description: "Coordination note for the role assignment"},
		},
	},
	"project.roles.list": {
		Method:      "project.roles.list",
		Description: "List project role assignments for coordination, optionally including inactive records",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"project_id":       {Type: "string", Required: true, Description: "Project whose roles should be listed"},
			"include_inactive": {Type: "boolean", Required: false, Description: "Include released or inactive role records"},
		},
	},
	"project.governance.predicates.check": {
		Method:      "project.governance.predicates.check",
		Description: "Read-only server-side verification of strict project governance stall predicates",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"project_id":          {Type: "string", Required: true, Description: "Project to inspect for stall evidence"},
			"challenged_agent_id": {Type: "string", Required: true, Description: "Current strategic lead being evaluated"},
			"stall_predicates":    {Type: "array", Required: false, Description: "Optional strict predicate list", Enum: []string{"fanout_absent", "idle_roster"}},
		},
	},
	"project.governance.challenge.raise": {
		Method:      "project.governance.challenge.raise",
		Description: "Open a durable project leadership challenge after strict server-side stall predicates hold",
		Params: map[string]ParamSchema{
			"workspace_id":                 {Type: "string", Required: true},
			"project_id":                   {Type: "string", Required: true, Description: "Project whose strategic lead is challenged"},
			"actor_id":                     {Type: "string", Required: true, Description: "Authenticated actor raising the challenge"},
			"challenged_agent_id":          {Type: "string", Required: true, Description: "Current strategic lead agent"},
			"challenger_agent_id":          {Type: "string", Required: true, Description: "Agent presenting the challenge evidence"},
			"nominated_successor_agent_id": {Type: "string", Required: false, Description: "Optional successor if the electorate votes to reassign"},
			"stall_predicates":             {Type: "array", Required: false, Description: "Strict predicates to verify", Enum: []string{"fanout_absent", "idle_roster"}},
			"evidence_refs":                {Type: "array", Required: false, Description: "Durable evidence references supporting the challenge"},
			"argument_doc_key":             {Type: "string", Required: false, Description: "Workspace doc key containing challenger arguments"},
			"tension_id":                   {Type: "string", Required: false, Description: "Optional governed tension backing this challenge"},
			"defense_window_seconds":       {Type: "integer", Required: false, Description: "Defense window override"},
			"voting_window_seconds":        {Type: "integer", Required: false, Description: "Voting window override"},
			"max_rounds":                   {Type: "integer", Required: false, Description: "Maximum challenge rounds, capped at 3"},
		},
	},
	"project.governance.challenge.defend": {
		Method:      "project.governance.challenge.defend",
		Description: "Record the challenged lead's durable defense or concession and open voting",
		Params: map[string]ParamSchema{
			"workspace_id":          {Type: "string", Required: true},
			"actor_id":              {Type: "string", Required: true, Description: "Authenticated defender or operator"},
			"challenge_id":          {Type: "string", Required: true},
			"round":                 {Type: "integer", Required: false, Description: "Expected challenge round"},
			"stance":                {Type: "string", Required: true, Enum: []string{"DEFEND", "CONCEDE"}},
			"defense_doc_key":       {Type: "string", Required: false, Description: "Workspace doc key containing defense arguments"},
			"voting_window_seconds": {Type: "integer", Required: false, Description: "Voting window override after defense"},
		},
	},
	"project.governance.vote.cast": {
		Method:      "project.governance.vote.cast",
		Description: "Cast one durable governance vote for a challenge round",
		Params: map[string]ParamSchema{
			"workspace_id":      {Type: "string", Required: true},
			"actor_id":          {Type: "string", Required: true, Description: "Authenticated actor casting or recording the vote"},
			"challenge_id":      {Type: "string", Required: true},
			"round":             {Type: "integer", Required: false, Description: "Expected challenge round"},
			"voter_agent_id":    {Type: "string", Required: true, Description: "Agent electorate member whose vote is recorded"},
			"ballot":            {Type: "string", Required: true, Enum: []string{"UPHOLD_LEAD", "REASSIGN", "ABSTAIN"}},
			"rationale_doc_key": {Type: "string", Required: false, Description: "Optional workspace doc key for vote rationale"},
		},
	},
	"project.governance.challenge.tally": {
		Method:      "project.governance.challenge.tally",
		Description: "Tally a voting challenge with strict two-thirds electorate quorum; reassignment is explicit opt-in",
		Params: map[string]ParamSchema{
			"workspace_id":       {Type: "string", Required: true},
			"actor_id":           {Type: "string", Required: true, Description: "Authenticated actor tallying the vote"},
			"challenge_id":       {Type: "string", Required: true},
			"reassign_enabled":   {Type: "boolean", Required: false, Description: "Must be true to transfer the strategic lead after a reassign quorum"},
			"lead_lease_seconds": {Type: "integer", Required: false, Description: "Optional lease duration for the successor lead"},
		},
	},
	"project.governance.challenge.get": {
		Method:      "project.governance.challenge.get",
		Description: "Read one project governance challenge and optionally its current-round votes",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"challenge_id":  {Type: "string", Required: true},
			"include_votes": {Type: "boolean", Required: false, Description: "Include current-round votes"},
		},
	},
	"project.governance.challenge.list": {
		Method:      "project.governance.challenge.list",
		Description: "List durable project governance challenges for a workspace or project",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: false},
			"state":        {Type: "string", Required: false, Enum: []string{"RAISED", "DEFENSE_OPEN", "VOTING", "NEGOTIATION", "RESOLVED_UPHELD", "RESOLVED_REASSIGNED", "RESOLVED_DEFAULT", "AUTO_WITHDRAWN"}},
			"limit":        {Type: "integer", Required: false, Description: "Maximum records to return"},
		},
	},
	"project.governance.votes.list": {
		Method:      "project.governance.votes.list",
		Description: "List durable votes for a project governance challenge round",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"challenge_id": {Type: "string", Required: true},
			"round":        {Type: "integer", Required: false},
		},
	},
	"project.repository.upsert": {
		Method:      "project.repository.upsert",
		Description: "Register or update a project repository record and record durable runtime evidence",
		Params: map[string]ParamSchema{
			"workspace_id":              {Type: "string", Required: true},
			"project_id":                {Type: "string", Required: true, Description: "Project receiving the repository record"},
			"actor_id":                  {Type: "string", Required: true, Description: "Authenticated actor authorizing this repository mutation"},
			"repo_id":                   {Type: "string", Required: false, Description: "Optional stable repository record id"},
			"remote_url":                {Type: "string", Required: false, Description: "Repository remote URL"},
			"remote_kind":               {Type: "string", Required: false, Description: "Repository remote kind", Enum: []string{"github", "gitlab", "local", "unknown"}},
			"owner":                     {Type: "string", Required: false, Description: "Repository owner or namespace"},
			"name":                      {Type: "string", Required: false, Description: "Repository name"},
			"default_branch":            {Type: "string", Required: false, Description: "Default branch for agent work"},
			"integration_branch":        {Type: "string", Required: false, Description: "Branch used for integration"},
			"credential_vault_entry_id": {Type: "string", Required: false, Description: "Vault entry id for repository credentials; never pass secret material"},
			"repo_status":               {Type: "string", Required: false, Description: "Repository lifecycle status", Enum: []string{"MISSING", "REQUESTED", "CREATED", "READY", "BROKEN", "ARCHIVED"}},
			"is_canonical":              {Type: "boolean", Required: false, Description: "Whether this is the canonical repository for the project"},
			"created_by_agent_id":       {Type: "string", Required: false, Description: "Agent that originally materialized or requested the repository"},
		},
	},
	"project.repositories.list": {
		Method:      "project.repositories.list",
		Description: "List project repository records for coordination",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"project_id":       {Type: "string", Required: true, Description: "Project whose repositories should be listed"},
			"include_archived": {Type: "boolean", Required: false, Description: "Include archived repository records"},
		},
	},
	"project.checkout.register": {
		Method:      "project.checkout.register",
		Description: "Register or heartbeat an agent checkout/worktree and record durable runtime evidence",
		Params: map[string]ParamSchema{
			"workspace_id":    {Type: "string", Required: true},
			"project_id":      {Type: "string", Required: true, Description: "Project receiving the checkout record"},
			"actor_id":        {Type: "string", Required: true, Description: "Authenticated actor authorizing this checkout mutation"},
			"checkout_id":     {Type: "string", Required: false, Description: "Optional stable checkout record id"},
			"repo_id":         {Type: "string", Required: true, Description: "Repository record this checkout belongs to"},
			"machine_id":      {Type: "string", Required: true, Description: "Stable local machine identifier"},
			"machine_label":   {Type: "string", Required: false, Description: "Human-readable machine label"},
			"owner_user_id":   {Type: "string", Required: false, Description: "Local user that owns the checkout"},
			"agent_id":        {Type: "string", Required: false, Description: "Agent using the checkout"},
			"local_path":      {Type: "string", Required: true, Description: "Local checkout path on that machine"},
			"checkout_kind":   {Type: "string", Required: false, Description: "Checkout kind", Enum: []string{"clone", "worktree", "integration", "review", "scratch"}},
			"branch_name":     {Type: "string", Required: false, Description: "Current working branch"},
			"base_branch":     {Type: "string", Required: false, Description: "Base branch for the checkout"},
			"head_sha":        {Type: "string", Required: false, Description: "Observed HEAD revision"},
			"base_sha":        {Type: "string", Required: false, Description: "Observed base revision"},
			"dirty_state":     {Type: "string", Required: false, Description: "Observed dirty state", Enum: []string{"clean", "dirty", "unknown"}},
			"active_task_id":  {Type: "string", Required: false, Description: "Task currently using the checkout; must be paired with active_claim_id"},
			"active_claim_id": {Type: "string", Required: false, Description: "Active task claim currently using the checkout; task claims are keyed by task_id and must match active_task_id, checkout agent_id, and CLAIMED status"},
			"status":          {Type: "string", Required: false, Description: "Checkout lifecycle status", Enum: []string{"ACTIVE", "STALE", "BLOCKED", "ABANDONED", "ARCHIVED"}},
			"last_seen_at":    {Type: "string", Required: false, Description: "Last observed heartbeat timestamp"},
		},
	},
	"project.checkouts.list": {
		Method:      "project.checkouts.list",
		Description: "List project checkout records for coordination and stale-checkout diagnosis",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"project_id":          {Type: "string", Required: true, Description: "Project whose checkouts should be listed"},
			"repo_id":             {Type: "string", Required: false, Description: "Optional repository filter"},
			"agent_id":            {Type: "string", Required: false, Description: "Optional agent filter"},
			"include_inactive":    {Type: "boolean", Required: false, Description: "Include inactive checkout records"},
			"stale_after_seconds": {Type: "integer", Required: false, Description: "Derived stale threshold in seconds"},
			"reference_timestamp": {Type: "string", Required: false, Description: "Timestamp used for deterministic stale calculation"},
		},
	},
	"project.branch.register": {
		Method:      "project.branch.register",
		Description: "Register or heartbeat an agent-owned project branch and record durable runtime evidence; READY_FOR_REVIEW records only the branch receipt and returns project.patch_queue.submit as the mandatory next transition",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"project_id":       {Type: "string", Required: true, Description: "Project receiving the branch record"},
			"actor_id":         {Type: "string", Required: true, Description: "Authenticated actor authorizing this branch mutation"},
			"branch_id":        {Type: "string", Required: false, Description: "Optional stable branch registry id"},
			"repo_id":          {Type: "string", Required: true, Description: "Repository record this branch belongs to"},
			"checkout_id":      {Type: "string", Required: false, Description: "Optional checkout/worktree currently using the branch"},
			"agent_id":         {Type: "string", Required: false, Description: "Agent owning the branch; agent principals default to actor_id"},
			"active_task_id":   {Type: "string", Required: false, Description: "Task currently using the branch; must match active_claim_id when provided"},
			"active_claim_id":  {Type: "string", Required: false, Description: "Active task claim currently using the branch; task claims are keyed by task_id and must match active_task_id and agent_id"},
			"branch_name":      {Type: "string", Required: true, Description: "Git branch name reserved for this agent or integration slice"},
			"branch_kind":      {Type: "string", Required: false, Description: "Branch purpose", Enum: []string{"feature", "integration", "review", "release", "scratch"}},
			"base_branch":      {Type: "string", Required: false, Description: "Base branch this branch was created from"},
			"head_sha":         {Type: "string", Required: false, Description: "Observed HEAD revision"},
			"base_sha":         {Type: "string", Required: false, Description: "Observed base revision"},
			"write_scope_json": {Type: "string", Required: false, Description: "Optional JSON string describing intended write scope"},
			"review_doc_key":   {Type: "string", Required: false, Description: "Workspace doc key containing branch review evidence; required when status is READY_FOR_REVIEW"},
			"status":           {Type: "string", Required: false, Description: "Branch lifecycle status", Enum: []string{"RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW", "MERGED", "ABANDONED", "ARCHIVED"}},
		},
	},
	"project.branches.list": {
		Method:      "project.branches.list",
		Description: "List project branch records for coordination, collision avoidance, and review handoff",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"project_id":       {Type: "string", Required: true, Description: "Project whose branches should be listed"},
			"repo_id":          {Type: "string", Required: false, Description: "Optional repository filter"},
			"agent_id":         {Type: "string", Required: false, Description: "Optional agent-owner filter"},
			"active_task_id":   {Type: "string", Required: false, Description: "Optional active task filter"},
			"include_inactive": {Type: "boolean", Required: false, Description: "Include merged, abandoned, or archived branch records"},
		},
	},
	"project.patch_queue.submit": {
		Method:      "project.patch_queue.submit",
		Description: "Submit a READY_FOR_REVIEW project branch as a controlled durable patch queue candidate without mutating git or enabling auto-merge",
		Params: map[string]ParamSchema{
			"workspace_id":               {Type: "string", Required: true},
			"project_id":                 {Type: "string", Required: true, Description: "Project receiving the patch queue candidate"},
			"actor_id":                   {Type: "string", Required: true, Description: "Authenticated actor submitting the candidate"},
			"queue_id":                   {Type: "string", Required: false, Description: "Optional stable queue id; defaults to project/repo scope"},
			"item_id":                    {Type: "string", Required: false, Description: "Optional stable item id; defaults to branch scope"},
			"repo_id":                    {Type: "string", Required: true, Description: "Repository containing the READY_FOR_REVIEW branch"},
			"branch_id":                  {Type: "string", Required: true, Description: "READY_FOR_REVIEW branch registry id"},
			"review_doc_key":             {Type: "string", Required: false, Description: "Workspace doc key for branch review evidence; must match the branch when provided"},
			"supersedes_queue_id":        {Type: "string", Required: false, Description: "Queue id being superseded for same-head BLOCKED requeue, or legacy patch-only cancel+replace; legacy replacements must keep this same queue_id"},
			"supersedes_item_id":         {Type: "string", Required: false, Description: "Item id being superseded for same-head BLOCKED requeue, or legacy patch-only cancel+replace historical provenance"},
			"evidence_doc_key":           {Type: "string", Required: false, Description: "Validation or review evidence doc proving a superseded BLOCKED reason is no longer current; omit for legacy patch-only cancel+replace"},
			"repo_authority_mode":        {Type: "string", Required: false, Description: "Repository authority mode; runtime submit requires repoauthority_controlled_queue with complete durable binding evidence. patch_only_temp_repo is legacy/invalid and is only canceled/replaced as historical evidence.", Enum: []string{"repoauthority_controlled_queue"}},
			"pathset_json":               {Type: "string", Required: false, Description: "Optional JSON pathset; defaults to branch write_scope_json"},
			"base_ref":                   {Type: "string", Required: false, Description: "Base branch/ref used by the candidate"},
			"base_sha":                   {Type: "string", Required: false, Description: "Observed base revision"},
			"head_sha":                   {Type: "string", Required: false, Description: "Observed candidate head revision; defaults to branch head_sha"},
			"auto_merge":                 {Type: "boolean", Required: false, Description: "Must remain false; automatic merge is not enabled"},
			"task_id":                    {Type: "string", Required: true, Description: "Durable mutation-binding task ref"},
			"session_id":                 {Type: "string", Required: true, Description: "Durable mutation-binding session ref"},
			"run_id":                     {Type: "string", Required: true, Description: "Durable mutation-binding execution run ref"},
			"agent_id":                   {Type: "string", Required: true, Description: "Durable mutation-binding agent ref"},
			"principal_type":             {Type: "string", Required: false, Description: "Durable mutation-binding principal type"},
			"principal_id":               {Type: "string", Required: false, Description: "Durable mutation-binding principal id"},
			"capability_snapshot_id":     {Type: "string", Required: true, Description: "Durable capability snapshot id for the proposed mutation context"},
			"capability_snapshot_schema": {Type: "string", Required: true, Description: "Durable capability snapshot schema for the proposed mutation context"},
			"repo_root":                  {Type: "string", Required: true, Description: "Registered local repository root for mutation binding diagnostics"},
			"base_tree_hash":             {Type: "string", Required: true, Description: "Base tree hash used by mutation binding diagnostics"},
			"base_file_hashes":           {Type: "object", Required: false, Description: "Map of normalized path to base file hash for mutation binding diagnostics"},
			"base_file_hashes_json":      {Type: "string", Required: false, Description: "JSON object equivalent to base_file_hashes"},
			"context_digest":             {Type: "string", Required: false, Description: "Optional precomputed proposal context digest; verified when supplied"},
			"repo_lease_id":              {Type: "string", Required: true, Description: "Durable repo lease id for mutation binding diagnostics"},
			"lease_term":                 {Type: "integer", Required: true, Description: "Durable repo lease term for mutation binding diagnostics"},
			"max_attempts":               {Type: "integer", Required: false, Description: "Durable bounded retry limit; defaults to 1 and does not enable retry execution"},
			"operation_id":               {Type: "string", Required: false, Description: "Reserved for future operation binding; project.patch_queue.submit rejects self-asserted operation refs"},
			"operation_kind":             {Type: "string", Required: false, Description: "Reserved for future operation binding; project.patch_queue.submit rejects self-asserted operation refs", Enum: []string{"repo_patch_apply"}},
		},
	},
	"project.patch_queue.supersede": {
		Method:      "project.patch_queue.supersede",
		Description: "Create a fresh PROPOSED patch queue item that supersedes a BLOCKED same-branch/head item using fresh validation evidence; no git mutation is performed",
		Params: map[string]ParamSchema{
			"workspace_id":               {Type: "string", Required: true},
			"project_id":                 {Type: "string", Required: true, Description: "Project containing the blocked patch queue item"},
			"actor_id":                   {Type: "string", Required: true, Description: "Authenticated reviewer, integrator, or strategic lead actor"},
			"queue_id":                   {Type: "string", Required: true, Description: "Superseded BLOCKED patch queue id"},
			"item_id":                    {Type: "string", Required: true, Description: "Superseded BLOCKED patch queue item id"},
			"new_item_id":                {Type: "string", Required: true, Description: "Fresh item id for the proposed superseding queue item"},
			"evidence_doc_key":           {Type: "string", Required: true, Description: "Fresh validation/review evidence doc proving the old BLOCKED reason is no longer current for the same branch/head"},
			"task_id":                    {Type: "string", Required: false, Description: "Optional controlled binding task ref; omit all binding refs to inherit from the superseded controlled item, or provide a complete override set"},
			"session_id":                 {Type: "string", Required: false, Description: "Optional controlled binding session ref; partial override sets fail closed"},
			"run_id":                     {Type: "string", Required: false, Description: "Optional controlled binding execution run ref; partial override sets fail closed"},
			"agent_id":                   {Type: "string", Required: false, Description: "Optional controlled binding agent ref; partial override sets fail closed"},
			"principal_type":             {Type: "string", Required: false, Description: "Optional principal type provenance"},
			"principal_id":               {Type: "string", Required: false, Description: "Optional principal id provenance"},
			"capability_snapshot_id":     {Type: "string", Required: false, Description: "Optional controlled binding capability snapshot id; omit all binding refs to inherit from the superseded controlled item, or provide a complete override set"},
			"capability_snapshot_schema": {Type: "string", Required: false, Description: "Optional controlled binding capability snapshot schema; partial override sets fail closed"},
			"repo_root":                  {Type: "string", Required: false, Description: "Optional controlled binding repo root; partial override sets fail closed"},
			"base_tree_hash":             {Type: "string", Required: false, Description: "Optional controlled binding base tree hash; partial override sets fail closed"},
			"base_file_hashes":           {Type: "object", Required: false, Description: "Optional controlled binding base file hashes; partial override sets fail closed"},
			"base_file_hashes_json":      {Type: "string", Required: false, Description: "Optional JSON object equivalent to base_file_hashes"},
			"context_digest":             {Type: "string", Required: false, Description: "Optional precomputed successor context digest; verified when supplied"},
			"repo_lease_id":              {Type: "string", Required: false, Description: "Optional controlled binding repo lease id; partial override sets fail closed"},
			"lease_term":                 {Type: "integer", Required: false, Description: "Optional controlled binding lease term; partial override sets fail closed"},
			"max_attempts":               {Type: "integer", Required: false, Description: "Durable bounded retry limit; defaults to the superseded item or 1"},
		},
	},
	"project.patch_queue.list": {
		Method:      "project.patch_queue.list",
		Description: "List durable project patch queue candidates visible to agents and integration owners",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project whose patch queue candidates should be listed"},
			"repo_id":      {Type: "string", Required: false, Description: "Optional repository filter"},
			"branch_id":    {Type: "string", Required: false, Description: "Optional branch filter"},
			"state":        {Type: "string", Required: false, Description: "Optional queue state filter", Enum: []string{"PROPOSED", "CLAIMED", "ACCEPTED", "REJECTED", "BLOCKED", "CANCELED"}},
		},
	},
	"service.direction.upsert": {
		Method:      "service.direction.upsert",
		Description: "Create or update a durable service-factory direction brief that anchors autonomous candidate generation, constraints, and budget posture.",
		Params:      serviceDirectionUpsertParamSchema(),
	},
	"service.direction.list": {
		Method:      "service.direction.list",
		Description: "List durable service-factory direction briefs for autonomous portfolio reflection.",
		Params:      serviceListParamSchema(map[string]ParamSchema{"status": {Type: "string", Required: false, Enum: []string{"DRAFT", "ACTIVE", "PAUSED", "ARCHIVED"}}}),
	},
	"service.direction.get": {
		Method:      "service.direction.get",
		Description: "Fetch one durable service-factory direction brief.",
		Params:      serviceGetParamSchema("direction_id", "Direction brief id"),
	},
	"service.candidate.upsert": {
		Method:      "service.candidate.upsert",
		Description: "Create or update a durable service candidate, including target user, pain, solution, distribution, monetization, score, and evidence plan.",
		Params:      serviceCandidateUpsertParamSchema(),
	},
	"service.candidate.list": {
		Method:      "service.candidate.list",
		Description: "List service candidates, optionally filtered by direction and selection status.",
		Params: serviceListParamSchema(map[string]ParamSchema{
			"direction_id": {Type: "string", Required: false},
			"status":       {Type: "string", Required: false, Enum: []string{"PROPOSED", "SELECTED", "REJECTED", "PARKED"}},
		}),
	},
	"service.candidate.get": {
		Method:      "service.candidate.get",
		Description: "Fetch one durable service candidate.",
		Params:      serviceGetParamSchema("candidate_id", "Service candidate id"),
	},
	"service.run.start": {
		Method:      "service.run.start",
		Description: "Start or upsert a service run bound to a selected candidate and a normal Rhizome project.",
		Params:      serviceRunUpsertParamSchema(),
	},
	"service.run.update": {
		Method:      "service.run.update",
		Description: "Update durable service run status, deployment target, public URL, budget account, credential policy, or budget cap.",
		Params:      serviceRunUpdateParamSchema(),
	},
	"service.run.list": {
		Method:      "service.run.list",
		Description: "List service runs for a workspace, candidate, project, or lifecycle status.",
		Params: serviceListParamSchema(map[string]ParamSchema{
			"candidate_id": {Type: "string", Required: false},
			"project_id":   {Type: "string", Required: false},
			"status":       {Type: "string", Required: false, Enum: []string{"PLANNED", "ACTIVE", "BLOCKED", "DEPLOYED", "MEASURING", "COMPLETED", "KILLED", "CANCELLED"}},
		}),
	},
	"service.run.get": {
		Method:      "service.run.get",
		Description: "Fetch one durable service run.",
		Params:      serviceGetParamSchema("run_id", "Service run id"),
	},
	"service.approval.grant": {
		Method:      "service.approval.grant",
		Description: "Record a service-scoped approval grant used to gate paid or credentialed provider resources.",
		Params:      serviceApprovalGrantParamSchema(),
	},
	"service.resource.record": {
		Method:      "service.resource.record",
		Description: "Record a provider resource for a service run; paid or credentialed active resources require an approved service approval grant.",
		Params:      serviceResourceRecordParamSchema(),
	},
	"service.spend.record": {
		Method:      "service.spend.record",
		Description: "Record service-run spend evidence and enforce the run budget cap.",
		Params:      serviceSpendRecordParamSchema(),
	},
	"service.revenue.record": {
		Method:      "service.revenue.record",
		Description: "Record service-run revenue or monetization observation evidence.",
		Params:      serviceRevenueRecordParamSchema(),
	},
	"service.outcome.record": {
		Method:      "service.outcome.record",
		Description: "Record a service outcome decision; continue/iterate decisions require public deploy, health, analytics, spend, and evidence refs.",
		Params:      serviceOutcomeRecordParamSchema(),
	},
	"service.coordination.get": {
		Method:      "service.coordination.get",
		Description: "Fetch a service-run coordination packet including candidate, direction, governance, spend, revenue, outcomes, and bound project coordination.",
		Params:      serviceGetParamSchema("run_id", "Service run id"),
	},
	"project.patch_queue.claim": {
		Method:      "project.patch_queue.claim",
		Description: "Claim a durable project patch queue candidate for integration review without mutating git",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"project_id":    {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":      {Type: "string", Required: true, Description: "Authenticated integration actor"},
			"queue_id":      {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":       {Type: "string", Required: true, Description: "Patch queue item id"},
			"claim_token":   {Type: "string", Required: false, Description: "Optional claim fence token for renewal; generated when omitted"},
			"lease_seconds": {Type: "integer", Required: false, Description: "Claim lease duration, capped by the server"},
		},
	},
	"project.patch_queue.operation_bind": {
		Method:      "project.patch_queue.operation_bind",
		Description: "Bind a claimed controlled patch queue candidate to a durable runtime operation ledger; this records evidence only and does not apply, merge, push, rebase, or switch branches",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"project_id":          {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":            {Type: "string", Required: true, Description: "Authenticated integration actor holding the claim"},
			"queue_id":            {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":             {Type: "string", Required: true, Description: "Patch queue item id"},
			"operation_id":        {Type: "string", Required: false, Description: "Optional existing runtime operation ledger id; when omitted the server creates a non-terminal repo_patch_apply ledger record"},
			"operation_kind":      {Type: "string", Required: false, Description: "Optional repo mutation operation kind; defaults to repo_patch_apply", Enum: []string{"repo_patch_apply"}},
			"mutation_paths_json": {Type: "string", Required: false, Description: "Optional JSON pathset; defaults to and must match the queued pathset"},
			"claim_token":         {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
		},
	},
	"project.patch_queue.cas_record": {
		Method:      "project.patch_queue.cas_record",
		Description: "Record conflict-safe CAS and verification evidence for a claimed operation-bound patch queue candidate; this is durable evidence only and does not apply, merge, push, rebase, or switch branches",
		Params: map[string]ParamSchema{
			"workspace_id":  {Type: "string", Required: true},
			"project_id":    {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":      {Type: "string", Required: true, Description: "Authenticated integration actor holding the claim"},
			"queue_id":      {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":       {Type: "string", Required: true, Description: "Patch queue item id"},
			"cas_result":    {Type: "object", Required: true, Description: "repo_cas_patch_apply.v1 result with applied status, matching operation context digest, patch digest, and path results"},
			"test_evidence": {Type: "object", Required: true, Description: "repo_patch_queue_test_evidence.v1 verification result proving candidate checks passed"},
			"claim_token":   {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
		},
	},
	"project.patch_queue.materialization_record": {
		Method:      "project.patch_queue.materialization_record",
		Description: "Record durable content-bound patch materialization for a claimed CAS-verified patch queue candidate; this stores candidate bytes as evidence only and does not apply, merge, push, rebase, switch branches, or mutate files",
		Params: map[string]ParamSchema{
			"workspace_id":    {Type: "string", Required: true},
			"project_id":      {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":        {Type: "string", Required: true, Description: "Authenticated integration actor holding the claim"},
			"queue_id":        {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":         {Type: "string", Required: true, Description: "Patch queue item id"},
			"materialization": {Type: "object", Required: true, Description: "repo_patch_materialization.v1 object with exact pathset candidate UTF-8 content bound to CAS candidate hashes"},
			"claim_token":     {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
		},
	},
	"project.patch_queue.rollback_record": {
		Method:      "project.patch_queue.rollback_record",
		Description: "Record durable rollback proof evidence for a claimed operation-bound and CAS-verified patch queue candidate; this does not invoke git or mutate files",
		Params: map[string]ParamSchema{
			"workspace_id":      {Type: "string", Required: true},
			"project_id":        {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":          {Type: "string", Required: true, Description: "Authenticated integration actor holding the claim"},
			"queue_id":          {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":           {Type: "string", Required: true, Description: "Patch queue item id"},
			"rollback_evidence": {Type: "object", Required: true, Description: "repo_patch_queue_rollback_evidence.v1 object with rollback operation refs, per-path restore hashes, verification status, and digestable recorded_at"},
			"claim_token":       {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
		},
	},
	"project.patch_queue.integration_record": {
		Method:      "project.patch_queue.integration_record",
		Description: "Record durable patch queue integration admission or integrated receipt; callers must write this receipt before reporting canonical git mutation success",
		Params: map[string]ParamSchema{
			"workspace_id":             {Type: "string", Required: true},
			"project_id":               {Type: "string", Required: true, Description: "Project containing the accepted patch queue candidate"},
			"actor_id":                 {Type: "string", Required: true, Description: "Authenticated integration actor"},
			"queue_id":                 {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":                  {Type: "string", Required: true, Description: "Patch queue item id"},
			"outcome":                  {Type: "string", Required: true, Description: "Integration receipt outcome", Enum: []string{"admitted", "integrated", "repair"}},
			"integration_mode":         {Type: "string", Required: false, Description: "Integration authority mode", Enum: []string{"materialization", "direct_merge"}},
			"authority_mode":           {Type: "string", Required: false, Description: "Runtime authority source for the integration attempt", Enum: []string{"project_role", "trust_first_advisory"}},
			"repo_id":                  {Type: "string", Required: false},
			"source_branch_id":         {Type: "string", Required: false},
			"source_head_sha":          {Type: "string", Required: false},
			"target_branch":            {Type: "string", Required: true, Description: "Canonical target branch for admitted/integrated receipts; refs/heads/ prefix is normalized for identity"},
			"target_head_before":       {Type: "string", Required: false},
			"target_head_after":        {Type: "string", Required: false},
			"remote_target_head_after": {Type: "string", Required: false, Description: "Canonical remote target branch head verified after push or already-integrated proof"},
			"merge_performed":          {Type: "boolean", Required: false},
			"push_attempted":           {Type: "boolean", Required: false},
			"push_succeeded":           {Type: "boolean", Required: false},
			"already_integrated":       {Type: "boolean", Required: false},
			"repair_reason":            {Type: "string", Required: false, Description: "Required when outcome=repair"},
		},
	},
	"project.patch_queue.integration_repair": {
		Method:      "project.patch_queue.integration_repair",
		Description: "Record a durable patch queue integration repair receipt after a failed canonical mutation attempt; this preserves evidence and blocks the accepted item for repair",
		Params: map[string]ParamSchema{
			"workspace_id":             {Type: "string", Required: true},
			"project_id":               {Type: "string", Required: true, Description: "Project containing the accepted patch queue candidate"},
			"actor_id":                 {Type: "string", Required: true, Description: "Authenticated integration actor"},
			"queue_id":                 {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":                  {Type: "string", Required: true, Description: "Patch queue item id"},
			"outcome":                  {Type: "string", Required: false, Description: "Defaults to repair", Enum: []string{"repair"}},
			"integration_mode":         {Type: "string", Required: false, Description: "Integration authority mode", Enum: []string{"materialization", "direct_merge"}},
			"authority_mode":           {Type: "string", Required: false, Description: "Runtime authority source for the integration attempt", Enum: []string{"project_role", "trust_first_advisory"}},
			"repo_id":                  {Type: "string", Required: false},
			"source_branch_id":         {Type: "string", Required: false},
			"source_head_sha":          {Type: "string", Required: false},
			"target_branch":            {Type: "string", Required: false},
			"target_head_before":       {Type: "string", Required: false},
			"target_head_after":        {Type: "string", Required: false},
			"remote_target_head_after": {Type: "string", Required: false, Description: "Canonical remote target branch head verified after push or failed integration attempt"},
			"merge_performed":          {Type: "boolean", Required: false},
			"push_attempted":           {Type: "boolean", Required: false},
			"push_succeeded":           {Type: "boolean", Required: false},
			"already_integrated":       {Type: "boolean", Required: false},
			"repair_reason":            {Type: "string", Required: true},
		},
	},
	"project.patch_queue.reviewer_advisory_record": {
		Method:      "project.patch_queue.reviewer_advisory_record",
		Description: "Record durable reviewer advisory evidence for a claimed rollback-proven patch queue candidate; this remains advisory-only and does not merge, push, rebase, switch branches, or mutate files",
		Params: map[string]ParamSchema{
			"workspace_id":      {Type: "string", Required: true},
			"project_id":        {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":          {Type: "string", Required: true, Description: "Reviewer or integration actor recording the advisory"},
			"queue_id":          {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":           {Type: "string", Required: true, Description: "Patch queue item id"},
			"reviewer_advisory": {Type: "object", Required: true, Description: "repo_patch_queue_reviewer_advisory.v1 advisory; omitted canonical fields are derived from durable queue/CAS/rollback evidence"},
			"claim_token":       {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
		},
	},
	"project.patch_queue.operator_enablement_record": {
		Method:      "project.patch_queue.operator_enablement_record",
		Description: "Record explicit human operator enablement evidence after reviewer advisory; requires RHIZOME_OPERATOR_IDS and still does not execute git mutation",
		Params: map[string]ParamSchema{
			"workspace_id":        {Type: "string", Required: true},
			"project_id":          {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":            {Type: "string", Required: true, Description: "Authenticated human operator id"},
			"queue_id":            {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":             {Type: "string", Required: true, Description: "Patch queue item id"},
			"operator_enablement": {Type: "object", Required: true, Description: "repo_patch_queue_operator_enablement.v1 object; omitted canonical fields are derived from durable queue/reviewer evidence"},
			"claim_token":         {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
		},
	},
	"operator.patch_queue.enable": {
		Method:      "operator.patch_queue.enable",
		Description: "Record the final human operator enablement gate for a reviewed patch queue candidate; requires an authenticated human profile listed in RHIZOME_OPERATOR_IDS and still does not execute git mutation",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":     {Type: "string", Required: true, Description: "Authenticated human operator id"},
			"queue_id":     {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":      {Type: "string", Required: true, Description: "Patch queue item id"},
			"claim_token":  {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
			"reason":       {Type: "string", Required: false, Description: "Operator note explaining the enablement decision"},
		},
	},
	"project.patch_queue.release": {
		Method:      "project.patch_queue.release",
		Description: "Release a claimed project patch queue candidate back to PROPOSED without mutating git",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":     {Type: "string", Required: true, Description: "Authenticated claim owner"},
			"queue_id":     {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":      {Type: "string", Required: true, Description: "Patch queue item id"},
			"claim_token":  {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
		},
	},
	"project.patch_queue.decision": {
		Method:      "project.patch_queue.decision",
		Description: "Record an integration decision for a claimed project patch queue candidate without merging or mutating git",
		Params: map[string]ParamSchema{
			"workspace_id":            {Type: "string", Required: true},
			"project_id":              {Type: "string", Required: true, Description: "Project containing the patch queue candidate"},
			"actor_id":                {Type: "string", Required: true, Description: "Authenticated claim owner"},
			"queue_id":                {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":                 {Type: "string", Required: true, Description: "Patch queue item id"},
			"decision":                {Type: "string", Required: true, Description: "Integration decision", Enum: []string{"ACCEPTED", "REJECTED", "BLOCKED", "CANCELED"}},
			"decision_doc_key":        {Type: "string", Required: false, Description: "Optional workspace doc key containing decision evidence"},
			"decision_summary":        {Type: "string", Required: true, Description: "Concise factual integration decision summary"},
			"checked_source_doc_keys": {Type: "array", Required: false, Description: "Source doc keys explicitly checked by an ACCEPTED source-fidelity review"},
			"claim_token":             {Type: "string", Required: true, Description: "Claim fence token returned by project.patch_queue.claim"},
		},
	},
	"project.patch_queue.review_task.reconcile": {
		Method:      "project.patch_queue.review_task.reconcile",
		Description: "Repair or create the durable review task receipt for a live patch queue candidate without mutating git or changing the queue decision state",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project containing the live patch queue candidate"},
			"actor_id":     {Type: "string", Required: true, Description: "Authenticated integration actor performing receipt repair"},
			"queue_id":     {Type: "string", Required: true, Description: "Patch queue id"},
			"item_id":      {Type: "string", Required: true, Description: "Patch queue item id"},
		},
	},
	"project.patch_queue.decision_continuation.consume": {
		Method:      "project.patch_queue.decision_continuation.consume",
		Description: "Consume a pending durable patch queue decision continuation outbox row into one visible follow-up task without mutating git",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"project_id":   {Type: "string", Required: true, Description: "Project containing the terminal patch queue decision"},
			"actor_id":     {Type: "string", Required: true, Description: "Authenticated integration actor consuming the continuation"},
			"outbox_id":    {Type: "string", Required: false, Description: "Decision continuation outbox id; may be omitted when queue_id/item_id are provided"},
			"queue_id":     {Type: "string", Required: false, Description: "Patch queue id"},
			"item_id":      {Type: "string", Required: false, Description: "Patch queue item id"},
		},
	},

	// Budget ledger
	"budget.account.ensure": {
		Method:      "budget.account.ensure",
		Description: "Create or update a hard budget account bound to a principal and workspace",
		Params: map[string]ParamSchema{
			"account_id":     {Type: "string", Required: true},
			"principal_type": {Type: "string", Required: true},
			"principal_id":   {Type: "string", Required: true},
			"workspace_id":   {Type: "string", Required: true},
			"currency":       {Type: "string", Required: false, Default: "USD"},
			"limit_micros":   {Type: "integer", Required: true, Description: "Hard limit in integer micro-units"},
			"status":         {Type: "string", Required: false, Enum: []string{"ACTIVE"}, Default: "ACTIVE"},
		},
	},
	"budget.account.get": {
		Method:      "budget.account.get",
		Description: "Read a hard budget account snapshot",
		Params: map[string]ParamSchema{
			"account_id":   {Type: "string", Required: true},
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"budget.reserve": {
		Method:      "budget.reserve",
		Description: "Reserve hard budget for a specific workspace/agent/task/run/provider/model binding",
		Params: map[string]ParamSchema{
			"reservation_id":  {Type: "string", Required: true},
			"idempotency_key": {Type: "string", Required: true},
			"account_id":      {Type: "string", Required: true},
			"workspace_id":    {Type: "string", Required: true},
			"agent_id":        {Type: "string", Required: true},
			"task_id":         {Type: "string", Required: true},
			"run_id":          {Type: "string", Required: true},
			"provider_id":     {Type: "string", Required: true},
			"model":           {Type: "string", Required: true},
			"amount_micros":   {Type: "integer", Required: true},
			"reason":          {Type: "string", Required: false},
		},
	},
	"budget.spend": {
		Method:      "budget.spend",
		Description: "Capture spend against an existing budget reservation",
		Params: map[string]ParamSchema{
			"entry_id":        {Type: "string", Required: true},
			"idempotency_key": {Type: "string", Required: true},
			"account_id":      {Type: "string", Required: true},
			"reservation_id":  {Type: "string", Required: true},
			"workspace_id":    {Type: "string", Required: true},
			"agent_id":        {Type: "string", Required: true},
			"task_id":         {Type: "string", Required: true},
			"run_id":          {Type: "string", Required: true},
			"provider_id":     {Type: "string", Required: true},
			"model":           {Type: "string", Required: true},
			"amount_micros":   {Type: "integer", Required: true},
			"reason":          {Type: "string", Required: false},
		},
	},
	"budget.release": {
		Method:      "budget.release",
		Description: "Release unused reserved budget from an existing reservation",
		Params: map[string]ParamSchema{
			"entry_id":        {Type: "string", Required: true},
			"idempotency_key": {Type: "string", Required: true},
			"account_id":      {Type: "string", Required: true},
			"reservation_id":  {Type: "string", Required: true},
			"workspace_id":    {Type: "string", Required: true},
			"agent_id":        {Type: "string", Required: true},
			"task_id":         {Type: "string", Required: true},
			"run_id":          {Type: "string", Required: true},
			"provider_id":     {Type: "string", Required: true},
			"model":           {Type: "string", Required: true},
			"amount_micros":   {Type: "integer", Required: true},
			"reason":          {Type: "string", Required: false},
		},
	},
	"budget.refund": {
		Method:      "budget.refund",
		Description: "Refund captured spend against a concrete source spend entry",
		Params: map[string]ParamSchema{
			"entry_id":        {Type: "string", Required: true},
			"idempotency_key": {Type: "string", Required: true},
			"account_id":      {Type: "string", Required: true},
			"workspace_id":    {Type: "string", Required: true},
			"agent_id":        {Type: "string", Required: true},
			"task_id":         {Type: "string", Required: true},
			"run_id":          {Type: "string", Required: true},
			"provider_id":     {Type: "string", Required: true},
			"model":           {Type: "string", Required: true},
			"source_entry_id": {Type: "string", Required: true},
			"amount_micros":   {Type: "integer", Required: true},
			"reason":          {Type: "string", Required: false},
		},
	},
	"budget.ledger.list": {
		Method:      "budget.ledger.list",
		Description: "List budget ledger entries by account, reservation, task, or run",
		Params: map[string]ParamSchema{
			"account_id":     {Type: "string", Required: false},
			"reservation_id": {Type: "string", Required: false},
			"workspace_id":   {Type: "string", Required: true},
			"task_id":        {Type: "string", Required: false},
			"run_id":         {Type: "string", Required: false},
			"limit":          {Type: "integer", Required: false, Default: "500"},
		},
	},
	"budget.reservations.list": {
		Method:      "budget.reservations.list",
		Description: "List budget reservations by account, workspace, agent, task, run, or status",
		Params: map[string]ParamSchema{
			"account_id":   {Type: "string", Required: false},
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: false},
			"task_id":      {Type: "string", Required: false},
			"run_id":       {Type: "string", Required: false},
			"status":       {Type: "string", Required: false},
			"limit":        {Type: "integer", Required: false, Default: "500"},
		},
	},
	"budget.health": {
		Method:      "budget.health",
		Description: "Read budget ledger health counters for diagnostics and readiness",
		Params: map[string]ParamSchema{
			"stale_after_sec": {Type: "integer", Required: false, Default: "3600"},
		},
	},

	// ── Limits ──
	"limits.group.create": {
		Method:      "limits.group.create",
		Description: "Create a limit group (subscription-based grouping for agent rate limits)",
		Params: map[string]ParamSchema{
			"group_id":          {Type: "string", Required: true, Description: "Unique group identifier"},
			"workspace_id":      {Type: "string", Required: true},
			"title":             {Type: "string", Required: true, Description: "Group display name"},
			"owner_name":        {Type: "string", Required: false, Description: "Subscription owner name"},
			"subscription_tier": {Type: "string", Required: false, Description: "Subscription level (e.g. openai-plus, claude-pro)"},
			"daily_limit":       {Type: "integer", Required: false, Description: "Daily usage limit"},
			"weekly_limit":      {Type: "integer", Required: false, Description: "Weekly usage limit"},
			"actor_id":          {Type: "string", Required: true, Description: "Human/system actor authorizing this group mutation"},
		},
	},
	"limits.group.update": {
		Method:      "limits.group.update",
		Description: "Update limit group metadata and/or agent membership",
		Params: map[string]ParamSchema{
			"workspace_id":      {Type: "string", Required: true},
			"group_id":          {Type: "string", Required: true},
			"title":             {Type: "string", Required: false, Description: "New group title"},
			"owner_name":        {Type: "string", Required: false, Description: "New owner name"},
			"subscription_tier": {Type: "string", Required: false, Description: "New subscription tier"},
			"daily_limit":       {Type: "integer", Required: false, Description: "New daily limit"},
			"weekly_limit":      {Type: "integer", Required: false, Description: "New weekly limit"},
			"agent_ids":         {Type: "array[string]", Required: false, Description: "Full replacement of agent membership list"},
			"actor_id":          {Type: "string", Required: true, Description: "Human/system actor authorizing this group mutation"},
		},
	},
	"limits.group.get": {
		Method:      "limits.group.get",
		Description: "Get a specific limit group with its agent list",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"group_id":     {Type: "string", Required: true},
		},
	},
	"agent.limits.get": {
		Method:      "agent.limits.get",
		Description: "Get the exact limit group and capacities assigned to a specific agent",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"agent_id":     {Type: "string", Required: true},
		},
	},
	"limits.group.list": {
		Method:      "limits.group.list",
		Description: "List all limit groups in the workspace",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"limits.group.delete": {
		Method:      "limits.group.delete",
		Description: "Delete a limit group (agents are unlinked, snapshots are deleted)",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"group_id":     {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true, Description: "Human/system actor authorizing this group mutation"},
		},
	},
	"limits.report": {
		Method:      "limits.report",
		Description: "Agent reports remaining limits for its group (updates group + creates audit snapshot)",
		Params: map[string]ParamSchema{
			"group_id":         {Type: "string", Required: true, Description: "Limit group to report for"},
			"agent_id":         {Type: "string", Required: true, Description: "Reporting agent ID"},
			"daily_remaining":  {Type: "integer", Required: true, Description: "Remaining daily usage"},
			"weekly_remaining": {Type: "integer", Required: true, Description: "Remaining weekly usage"},
		},
	},
	"limits.bootstrap": {
		Method:      "limits.bootstrap",
		Description: "Create singleton limit groups for all agents that don't have one yet",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"limits.snapshots": {
		Method:      "limits.snapshots",
		Description: "Get limit usage snapshots for a group within a time range (for charts)",
		Params: map[string]ParamSchema{
			"group_id": {Type: "string", Required: true, Description: "Limit group to get snapshots for"},
			"days":     {Type: "integer", Description: "Number of days back (default 7)"},
		},
	},
	"news.publish": {
		Method:      "news.publish",
		Description: "Publish a news item — broadcasts notification to all agents",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"title":        {Type: "string", Required: true, Description: "News headline"},
			"content":      {Type: "string", Required: true, Description: "News body (markdown)"},
			"author_id":    {Type: "string", Required: true, Description: "Who published (agent_id or username)"},
			"author_type":  {Type: "string", Description: "agent or human (default: agent)"},
		},
	},
	"news.list": {
		Method:      "news.list",
		Description: "List news items in chronological order",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"limit":        {Type: "integer", Description: "Max items (default 50)"},
		},
	},
	"news.poll": {
		Method:      "news.poll",
		Description: "Poll news items after a stored cursor so agent listeners can ingest missed system updates",
		Params: map[string]ParamSchema{
			"workspace_id":     {Type: "string", Required: true},
			"after_created_at": {Type: "string", Description: "RFC3339Nano cursor timestamp from a prior news.poll response"},
			"after_news_id":    {Type: "string", Description: "Tie-breaker news id from a prior news.poll response; requires after_created_at"},
			"limit":            {Type: "integer", Description: "Max items (default 20)"},
			"lookback_hours":   {Type: "integer", Description: "Initial lookback window when no cursor is stored (default 24)"},
		},
	},
	"news.delete": {
		Method:      "news.delete",
		Description: "Delete a news item",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"news_id":      {Type: "string", Required: true},
			"actor_id":     {Type: "string", Required: true},
		},
	},

	// ── Tools ──
	"tool.register": {
		Method:      "tool.register",
		Description: "Register a tool in the workspace registry",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"tool_id":        {Type: "string", Required: true},
			"display_name":   {Type: "string", Required: true},
			"description":    {Type: "string", Required: false},
			"owner_user_id":  {Type: "string", Required: false},
			"owner_agent_id": {Type: "string", Required: false, Description: "Agent that owns this tool"},
			"kind":           {Type: "string", Required: false, Enum: []string{"BRIDGE", "SERVICE", "SANDBOX", "INTEGRATION", "BOT"}},
			"status":         {Type: "string", Required: false, Enum: []string{"ACTIVE", "PAUSED", "DEPRECATED"}, Default: "ACTIVE"},
			"version":        {Type: "string", Required: false},
			"access_level":   {Type: "string", Required: false, Description: "Access level", Default: "WORKSPACE"},
			"endpoint":       {Type: "string", Required: false, Description: "Service endpoint URL"},
			"capabilities":   {Type: "array[string]", Required: false, Description: "Array of capability strings"},
			"input_schema":   {Type: "object", Required: false, Description: "JSON Schema for tool.call arguments"},
			"manifest_json":  {Type: "string", Required: false},
		},
	},
	"tool.list": {
		Method:      "tool.list",
		Description: "List all registered tools in the workspace",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"tool.status": {
		Method:      "tool.status",
		Description: "Get a specific tool's details",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tool_id":      {Type: "string", Required: true},
		},
	},
	"tool.remove": {
		Method:      "tool.remove",
		Description: "Remove a tool from the workspace registry after its runtime deployment has been cleaned up",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"tool_id":      {Type: "string", Required: true},
			"removed_by":   {Type: "string", Required: false, Description: "Optional actor recorded in the audit trail"},
		},
	},
	"tool.deploy": {
		Method:      "tool.deploy",
		Description: "Deploy a tool script (saves source code to disk for execution)",
		Params: map[string]ParamSchema{
			"tool_id":      {Type: "string", Required: true},
			"workspace_id": {Type: "string", Required: true},
			"runtime":      {Type: "string", Required: false, Enum: []string{"python", "bash", "node"}, Default: "python"},
			"source_code":  {Type: "string", Required: true, Description: "Tool script source code"},
			"entry_point":  {Type: "string", Required: false, Description: "Entry filename (default: main.py/sh/js)"},
			"deployed_by":  {Type: "string", Required: false},
		},
	},
	"tool.call": {
		Method:      "tool.call",
		Description: "Execute a deployed tool. Arguments are passed as JSON on stdin",
		Params: map[string]ParamSchema{
			"tool_id":              {Type: "string", Required: true},
			"workspace_id":         {Type: "string", Required: true},
			"arguments":            {Type: "object", Required: true, Description: "Arguments passed to the tool (tool-specific)"},
			"timeout_sec":          {Type: "integer", Required: false, Description: "Execution timeout in seconds (capped by the current request deadline)", Default: "300"},
			"actor_type":           {Type: "string", Required: false, Description: "Optional actor type; if provided it must match the authenticated principal, otherwise the current principal is used"},
			"actor_id":             {Type: "string", Required: false, Description: "Optional actor id; if provided it must match the authenticated principal, otherwise the current principal is used"},
			"requested_capability": {Type: "string", Required: false, Description: "Registered tool execution uses the canonical capability `tool.call`; any other value is rejected"},
			"task_id":              {Type: "string", Required: false, Description: "Optional claimed task binding; if provided it must match a live same-owner claim/session binding"},
			"session_id":           {Type: "string", Required: false, Description: "Optional active session binding; if provided it must match task_id and the authenticated agent"},
			"run_id":               {Type: "string", Required: false, Description: "Optional parent execution-run binding; if provided it must match task_id/session_id and the authenticated agent"},
		},
	},
	"tool.undeploy": {
		Method:      "tool.undeploy",
		Description: "Remove a deployed tool from disk",
		Params: map[string]ParamSchema{
			"tool_id":      {Type: "string", Required: true},
			"workspace_id": {Type: "string", Required: true},
		},
	},

	// ── Vault ──
	"vault.create": {
		Method:      "vault.create",
		Description: "Store a secret in the encrypted vault",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"entry_id":     {Type: "string", Required: false},
			"title":        {Type: "string", Required: true},
			"description":  {Type: "string", Required: false},
			"fields_json":  {Type: "string", Required: false},
			"created_by":   {Type: "string", Required: true},
		},
	},
	"vault.update": {
		Method:      "vault.update",
		Description: "Update a vault entry",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"entry_id":     {Type: "string", Required: true},
			"title":        {Type: "string", Required: true},
			"description":  {Type: "string", Required: false},
			"fields_json":  {Type: "string", Required: false},
			"actor":        {Type: "string", Required: true},
		},
	},
	"vault.get": {
		Method:      "vault.get",
		Description: "Retrieve a secret from the vault",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"entry_id":     {Type: "string", Required: true},
			"actor":        {Type: "string", Required: false},
		},
	},
	"vault.list": {
		Method:      "vault.list",
		Description: "List vault keys (values are not returned)",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"actor":        {Type: "string", Required: false},
		},
	},
	"vault.delete": {
		Method:      "vault.delete",
		Description: "Delete a secret from the vault",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"entry_id":     {Type: "string", Required: true},
			"actor":        {Type: "string", Required: true},
		},
	},
	"vault.audit": {
		Method:      "vault.audit",
		Description: "Get vault access audit log",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"limit":        {Type: "integer", Required: false, Default: "50"},
		},
	},

	// ── Actions ──
	"action.create": {
		Method:      "action.create",
		Description: "Create a human action; can also promote a queued rebase follow-up or task-scoped rollback-failure recovery queue into an explicit action when queue_id or queue_key is provided and deterministic task context is available",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"task_id":      {Type: "string", Required: false, Description: "Required unless hydrated from the source queue or linked tension context"},
			"agent_id":     {Type: "string", Required: false, Description: "Optional explicit owner; can be hydrated from the source queue or linked tension context"},
			"assigned_to":  {Type: "string", Required: false},
			"title":        {Type: "string", Required: false, Description: "Required unless hydrated from the source queue"},
			"description":  {Type: "string", Required: false},
			"blocking":     {Type: "boolean", Required: false, Description: "Defaults from keep_session_active when promoting a rebase or rollback-recovery queue"},
			"queue_id":     {Type: "string", Required: false, Description: "Optional source queue to promote into an action when deterministic task context is available"},
			"queue_key":    {Type: "string", Required: false, Description: "Optional source queue key when queue_id is not provided"},
		},
	},
	"action.list": {
		Method:      "action.list",
		Description: "List actions (optionally filter by status)",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"status":       {Type: "string", Required: false, Enum: []string{"PENDING", "COMPLETED", "FAILED"}},
		},
	},
	"action.start": {
		Method:      "action.start",
		Description: "Start a pending rebase-linked action and mark its source workflow in progress",
		Params: map[string]ParamSchema{
			"action_id":  {Type: "string", Required: true},
			"started_by": {Type: "string", Required: true},
			"comment":    {Type: "string", Required: false},
		},
	},
	"action.pause": {
		Method:      "action.pause",
		Description: "Pause an in-progress rebase-linked action and return its source workflow to a claimed waiting state",
		Params: map[string]ParamSchema{
			"action_id": {Type: "string", Required: true},
			"paused_by": {Type: "string", Required: true},
			"comment":   {Type: "string", Required: false},
		},
	},
	"action.resolve": {
		Method:      "action.resolve",
		Description: "Resolve a pending action and apply the terminal completion or retry semantics for any linked rebase follow-up or rollback-failure source queue",
		Params: map[string]ParamSchema{
			"action_id":   {Type: "string", Required: true},
			"resolution":  {Type: "string", Required: true, Enum: []string{"COMPLETED", "FAILED"}},
			"resolved_by": {Type: "string", Required: true},
			"comment":     {Type: "string", Required: false},
		},
	},

	// ── Events ──
	"event.emit": {
		Method:      "event.emit",
		Description: "Emit an ephemeral SSE event to workspace subscribers without writing a runtime-journal row; type must use the ephemeral.* namespace",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
			"type":         {Type: "string", Required: true, Description: "Ephemeral-only event type; must start with ephemeral."},
			"agent_id":     {Type: "string", Required: false},
			"summary":      {Type: "string", Required: true},
			"payload_json": {Type: "string", Required: false},
		},
	},

	// ── RPC Logs ──
	"rpc.logs.list": {
		Method:      "rpc.logs.list",
		Description: "List RPC access logs within a recent time window, with filters and 24h stats",
		Params: map[string]ParamSchema{
			"method":      {Type: "string", Required: false, Description: "Filter by method name"},
			"status":      {Type: "string", Required: false, Enum: []string{"ok", "error"}},
			"limit":       {Type: "integer", Required: false, Default: "50"},
			"before_id":   {Type: "integer", Required: false, Description: "Cursor for pagination (load entries before this ID)"},
			"since_hours": {Type: "integer", Required: false, Default: "24", Description: "Only include entries newer than this rolling window in hours"},
		},
	},

	// ── Schema (self-referential) ──
	"rpc.describe": {
		Method:      "rpc.describe",
		Description: "Get parameter schema for any RPC method",
		Params: map[string]ParamSchema{
			"method": {Type: "string", Required: true, Description: "RPC method name to describe"},
		},
	},
	"rpc.methods.list": {
		Method:      "rpc.methods.list",
		Description: "List all available RPC methods with descriptions",
		Params:      map[string]ParamSchema{},
	},
	"runtime.build.info": {
		Method:      "runtime.build.info",
		Description: "Return non-secret build/runtime identity (binary hash, VCS revision, vcs_modified) for the running server binary, used to prove remote/local build parity",
		Params: map[string]ParamSchema{
			"workspace_id": {Type: "string", Required: true},
		},
	},
	"workspace.auth.agent.register": {
		Method:      "workspace.auth.agent.register",
		Description: "Register an agent with the workspace password and issue a per-agent token",
		Params: map[string]ParamSchema{
			"workspace_id":       {Type: "string", Required: true},
			"workspace_name":     {Type: "string", Required: false, Description: "Workspace title alias. If provided, the server resolves it to workspace_id."},
			"workspace_password": {Type: "string", Required: true},
			"agent_id":           {Type: "string", Required: false},
			"agent_name":         {Type: "string", Required: false},
			"display_name":       {Type: "string", Required: false},
			"group_id":           {Type: "string", Required: false, Description: "Optional shared limit group identifier for provider-scoped budgeting"},
			"owner_user_id":      {Type: "string", Required: false},
			"role":               {Type: "string", Required: false, Default: "generalist"},
			"protocol_version":   {Type: "string", Required: false, Default: "workspace-bootstrap/v1"},
			"capabilities":       {Type: "string", Required: false, Description: "Comma-separated list or JSON array of capabilities"},
			"summary":            {Type: "string", Required: false},
			"notes":              {Type: "string", Required: false},
			"host_url":           {Type: "string", Required: false},
		},
	},
	"workspace.auth.agent.update": {
		Method:      "workspace.auth.agent.update",
		Description: "Update declared metadata for an owned agent without changing its owner, live presence, or current live summary",
		Params: map[string]ParamSchema{
			"agent_id":         {Type: "string", Required: true},
			"display_name":     {Type: "string", Required: false, Description: "If omitted, preserve the current display_name."},
			"role":             {Type: "string", Required: false, Description: "If omitted, preserve the current role."},
			"protocol_version": {Type: "string", Required: false, Description: "If omitted, preserve the current protocol_version."},
			"capabilities":     {Type: "string", Required: false, Description: "Comma-separated list or JSON array of capabilities. If omitted, preserve the current capabilities."},
		},
	},
	"workspace.auth.human.register": {
		Method:      "workspace.auth.human.register",
		Description: "Register a human in a workspace using a username, display name, workspace password, and personal password",
		Params: map[string]ParamSchema{
			"workspace_id":       {Type: "string", Required: true},
			"workspace_name":     {Type: "string", Required: false, Description: "Workspace title alias. If provided, the server resolves it to workspace_id."},
			"workspace_password": {Type: "string", Required: true},
			"username":           {Type: "string", Required: false},
			"login_name":         {Type: "string", Required: false, Description: "Deprecated alias for username."},
			"display_name":       {Type: "string", Required: false},
			"name":               {Type: "string", Required: false, Description: "Legacy alias. If only name is provided, it is used as both username and display_name."},
			"password":           {Type: "string", Required: true},
		},
	},
	"workspace.auth.human.login": {
		Method:      "workspace.auth.human.login",
		Description: "Authenticate a human by username and personal password, issuing a new token",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"workspace_name": {Type: "string", Required: false, Description: "Workspace title alias. If provided, the server resolves it to workspace_id."},
			"username":       {Type: "string", Required: false},
			"login_name":     {Type: "string", Required: false, Description: "Deprecated alias for username."},
			"name":           {Type: "string", Required: false, Description: "Legacy alias for username."},
			"password":       {Type: "string", Required: true},
		},
	},
	"workspace.auth.human.profile.get": {
		Method:      "workspace.auth.human.profile.get",
		Description: "Get the authenticated human profile and owned agents",
		Params:      map[string]ParamSchema{},
	},
	"workspace.auth.human.profile.update": {
		Method:      "workspace.auth.human.profile.update",
		Description: "Update the authenticated human profile",
		Params: map[string]ParamSchema{
			"display_name":     {Type: "string", Required: false},
			"password":         {Type: "string", Required: false},
			"telegram_user_id": {Type: "integer", Required: false},
		},
	},
	"workspace.auth.human.sessions.list": {
		Method:      "workspace.auth.human.sessions.list",
		Description: "List auth sessions for the authenticated human",
		Params:      map[string]ParamSchema{},
	},
	"workspace.auth.human.sessions.revoke": {
		Method:      "workspace.auth.human.sessions.revoke",
		Description: "Revoke one auth session or all other sessions for the authenticated human",
		Params: map[string]ParamSchema{
			"token_id":           {Type: "string", Required: false},
			"all_other_sessions": {Type: "boolean", Required: false},
			"reason":             {Type: "string", Required: false},
		},
	},
	"workspace.auth.agent.token.rotate": {
		Method:      "workspace.auth.agent.token.rotate",
		Description: "Rotate the token for an owned agent and deliver a private system notice to that agent",
		Params: map[string]ParamSchema{
			"agent_id": {Type: "string", Required: true},
		},
	},
	"workspace.security.password.update": {
		Method:      "workspace.security.password.update",
		Description: "Update the workspace password",
		Params: map[string]ParamSchema{
			"workspace_id":       {Type: "string", Required: true},
			"workspace_name":     {Type: "string", Required: false},
			"workspace_password": {Type: "string", Required: false, Description: "Preferred field for the new workspace password."},
			"password":           {Type: "string", Required: false, Description: "Legacy alias for workspace_password."},
			"description":        {Type: "string", Required: false},
			"updated_by":         {Type: "string", Required: false},
		},
	},
	"workspace.security.audit.list": {
		Method:      "workspace.security.audit.list",
		Description: "List security events for a workspace",
		Params: map[string]ParamSchema{
			"workspace_id":   {Type: "string", Required: true},
			"workspace_name": {Type: "string", Required: false},
			"limit":          {Type: "integer", Required: false, Default: "50"},
		},
	},
}

func serviceBaseWriteParamSchema() map[string]ParamSchema {
	return map[string]ParamSchema{
		"workspace_id":    {Type: "string", Required: true},
		"actor_id":        {Type: "string", Required: true, Description: "Authenticated actor recording the service-factory mutation"},
		"idempotency_key": {Type: "string", Required: false, Description: "Stable idempotency key for retry-safe writes"},
	}
}

func serviceListParamSchema(extra map[string]ParamSchema) map[string]ParamSchema {
	params := map[string]ParamSchema{
		"workspace_id": {Type: "string", Required: true},
		"limit":        {Type: "integer", Required: false, Default: "50"},
	}
	for key, value := range extra {
		params[key] = value
	}
	return params
}

func serviceGetParamSchema(idField, description string) map[string]ParamSchema {
	return map[string]ParamSchema{
		"workspace_id": {Type: "string", Required: true},
		idField:        {Type: "string", Required: true, Description: description},
	}
}

func serviceDirectionUpsertParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["direction_id"] = ParamSchema{Type: "string", Required: false, Description: "Optional stable direction id; generated when omitted"}
	params["title"] = ParamSchema{Type: "string", Required: true}
	params["description"] = ParamSchema{Type: "string", Required: false}
	params["constraints_json"] = ParamSchema{Type: "string", Required: false, Description: "JSON object containing strategy, vertical, cost, compliance, or deployment constraints"}
	params["budget_cap_micros"] = ParamSchema{Type: "integer", Required: false, Description: "Optional total budget cap for this direction in micros"}
	params["status"] = ParamSchema{Type: "string", Required: false, Enum: []string{"DRAFT", "ACTIVE", "PAUSED", "ARCHIVED"}, Default: "ACTIVE"}
	return params
}

func serviceCandidateUpsertParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["candidate_id"] = ParamSchema{Type: "string", Required: false, Description: "Optional stable candidate id; generated when omitted"}
	params["direction_id"] = ParamSchema{Type: "string", Required: true}
	params["title"] = ParamSchema{Type: "string", Required: true}
	params["target_user"] = ParamSchema{Type: "string", Required: false}
	params["user_pain"] = ParamSchema{Type: "string", Required: false}
	params["solution_summary"] = ParamSchema{Type: "string", Required: false}
	params["distribution"] = ParamSchema{Type: "string", Required: false}
	params["monetization"] = ParamSchema{Type: "string", Required: false}
	params["implementation_size"] = ParamSchema{Type: "string", Required: false}
	params["risk_level"] = ParamSchema{Type: "string", Required: false}
	params["score"] = ParamSchema{Type: "integer", Required: false}
	params["evidence_plan_json"] = ParamSchema{Type: "string", Required: false, Description: "JSON object describing validation, deployment, analytics, ad, and cost evidence to collect"}
	params["status"] = ParamSchema{Type: "string", Required: false, Enum: []string{"PROPOSED", "SELECTED", "REJECTED", "PARKED"}, Default: "PROPOSED"}
	return params
}

func serviceRunUpsertParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["run_id"] = ParamSchema{Type: "string", Required: false, Description: "Optional stable service run id; generated when omitted"}
	params["candidate_id"] = ParamSchema{Type: "string", Required: true, Description: "Selected service candidate id"}
	params["project_id"] = ParamSchema{Type: "string", Required: true, Description: "Normal Rhizome project that owns implementation, review, integration, and QA tasks"}
	params["title"] = ParamSchema{Type: "string", Required: true}
	params["deploy_target"] = ParamSchema{Type: "string", Required: false}
	params["public_url"] = ParamSchema{Type: "string", Required: false}
	params["health_check_url"] = ParamSchema{Type: "string", Required: false}
	params["budget_account_id"] = ParamSchema{Type: "string", Required: false}
	params["budget_cap_micros"] = ParamSchema{Type: "integer", Required: false}
	params["credential_policy"] = ParamSchema{Type: "string", Required: false, Enum: []string{"PENDING_APPROVAL", "FREE_TIER_ONLY", "APPROVED"}, Default: "PENDING_APPROVAL"}
	params["status"] = ParamSchema{Type: "string", Required: false, Enum: []string{"PLANNED", "ACTIVE", "BLOCKED", "DEPLOYED", "MEASURING", "COMPLETED", "KILLED", "CANCELLED"}, Default: "ACTIVE"}
	return params
}

func serviceRunUpdateParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["run_id"] = ParamSchema{Type: "string", Required: true, Description: "Existing service run id"}
	params["candidate_id"] = ParamSchema{Type: "string", Required: false, Description: "Optional immutable candidate id restatement; rejected if it differs from the existing run"}
	params["project_id"] = ParamSchema{Type: "string", Required: false, Description: "Optional immutable project id restatement; rejected if it differs from the existing run"}
	params["title"] = ParamSchema{Type: "string", Required: false}
	params["deploy_target"] = ParamSchema{Type: "string", Required: false}
	params["public_url"] = ParamSchema{Type: "string", Required: false}
	params["health_check_url"] = ParamSchema{Type: "string", Required: false}
	params["budget_account_id"] = ParamSchema{Type: "string", Required: false}
	params["budget_cap_micros"] = ParamSchema{Type: "integer", Required: false}
	params["credential_policy"] = ParamSchema{Type: "string", Required: false, Enum: []string{"PENDING_APPROVAL", "FREE_TIER_ONLY", "APPROVED"}}
	params["status"] = ParamSchema{Type: "string", Required: false, Enum: []string{"PLANNED", "ACTIVE", "BLOCKED", "DEPLOYED", "MEASURING", "COMPLETED", "KILLED", "CANCELLED"}}
	return params
}

func serviceApprovalGrantParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["grant_id"] = ParamSchema{Type: "string", Required: false}
	params["run_id"] = ParamSchema{Type: "string", Required: true}
	params["grant_type"] = ParamSchema{Type: "string", Required: true, Description: "Approval category, such as paid_resource, credentialed_provider, ads, domain, or deploy"}
	params["scope_json"] = ParamSchema{Type: "string", Required: false, Description: "JSON object describing the approved scope"}
	params["approval_ref"] = ParamSchema{Type: "string", Required: false, Description: "Durable human/operator approval reference"}
	params["status"] = ParamSchema{Type: "string", Required: false, Enum: []string{"PENDING", "APPROVED", "REJECTED", "EXPIRED", "REVOKED"}, Default: "PENDING"}
	params["approved_by"] = ParamSchema{Type: "string", Required: false}
	params["expires_at"] = ParamSchema{Type: "string", Required: false}
	return params
}

func serviceResourceRecordParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["resource_id"] = ParamSchema{Type: "string", Required: false}
	params["run_id"] = ParamSchema{Type: "string", Required: true}
	params["provider"] = ParamSchema{Type: "string", Required: true}
	params["resource_type"] = ParamSchema{Type: "string", Required: true}
	params["resource_ref"] = ParamSchema{Type: "string", Required: false}
	params["credential_vault_entry_id"] = ParamSchema{Type: "string", Required: false, Description: "Vault entry reference only; credential material is rejected"}
	params["approval_grant_id"] = ParamSchema{Type: "string", Required: false, Description: "Required before active/provisioned paid or credentialed resources"}
	params["paid"] = ParamSchema{Type: "boolean", Required: false}
	params["cost_cap_micros"] = ParamSchema{Type: "integer", Required: false}
	params["status"] = ParamSchema{Type: "string", Required: false, Enum: []string{"PENDING_APPROVAL", "PROVISIONED", "ACTIVE", "REVOKED", "FAILED"}, Default: "PENDING_APPROVAL"}
	params["ttl_expires_at"] = ParamSchema{Type: "string", Required: false}
	return params
}

func serviceSpendRecordParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["receipt_id"] = ParamSchema{Type: "string", Required: false}
	params["run_id"] = ParamSchema{Type: "string", Required: true}
	params["provider_resource_id"] = ParamSchema{Type: "string", Required: false}
	params["ledger_entry_id"] = ParamSchema{Type: "string", Required: false, Description: "Optional budget ledger entry id to bind this service spend"}
	params["amount_micros"] = ParamSchema{Type: "integer", Required: true}
	params["currency"] = ParamSchema{Type: "string", Required: false, Default: "USD"}
	params["external_receipt_ref"] = ParamSchema{Type: "string", Required: false}
	params["evidence_ref"] = ParamSchema{Type: "string", Required: true}
	return params
}

func serviceRevenueRecordParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["observation_id"] = ParamSchema{Type: "string", Required: false}
	params["run_id"] = ParamSchema{Type: "string", Required: true}
	params["amount_micros"] = ParamSchema{Type: "integer", Required: true}
	params["currency"] = ParamSchema{Type: "string", Required: false, Default: "USD"}
	params["source"] = ParamSchema{Type: "string", Required: true}
	params["external_receipt_ref"] = ParamSchema{Type: "string", Required: false}
	params["evidence_ref"] = ParamSchema{Type: "string", Required: true}
	params["observed_at"] = ParamSchema{Type: "string", Required: false}
	return params
}

func serviceOutcomeRecordParamSchema() map[string]ParamSchema {
	params := serviceBaseWriteParamSchema()
	params["outcome_id"] = ParamSchema{Type: "string", Required: false}
	params["run_id"] = ParamSchema{Type: "string", Required: true}
	params["public_url"] = ParamSchema{Type: "string", Required: false, Description: "Required public http(s) URL for CONTINUE/ITERATE"}
	params["deploy_health_status"] = ParamSchema{Type: "string", Required: false, Enum: []string{"UNKNOWN", "PASS", "FAIL", "WAIVED"}, Default: "UNKNOWN"}
	params["deploy_evidence_ref"] = ParamSchema{Type: "string", Required: false}
	params["analytics_json"] = ParamSchema{Type: "string", Required: false, Description: "JSON object; non-empty for CONTINUE/ITERATE"}
	params["analytics_evidence_ref"] = ParamSchema{Type: "string", Required: false}
	params["spend_micros"] = ParamSchema{Type: "integer", Required: false}
	params["spend_evidence_ref"] = ParamSchema{Type: "string", Required: false}
	params["revenue_micros"] = ParamSchema{Type: "integer", Required: false}
	params["revenue_evidence_ref"] = ParamSchema{Type: "string", Required: false}
	params["quality_score"] = ParamSchema{Type: "integer", Required: false}
	params["decision"] = ParamSchema{Type: "string", Required: true, Enum: []string{"CONTINUE", "ITERATE", "KILL", "BLOCKED", "HOLD"}}
	params["decision_reason"] = ParamSchema{Type: "string", Required: true}
	params["evidence_refs_json"] = ParamSchema{Type: "string", Required: true, Description: "JSON string array of durable evidence refs"}
	return params
}

func sessionEventParamSchema() map[string]ParamSchema {
	return map[string]ParamSchema{
		"workspace_id":          {Type: "string", Required: true},
		"session_id":            {Type: "string", Required: true},
		"agent_id":              {Type: "string", Required: true},
		"task_id":               {Type: "string", Required: false},
		"summary":               {Type: "string", Required: true},
		"status":                {Type: "string", Required: false, Enum: []string{"ACTIVE", "BLOCKED", "WAITING_DECISION", "HANDOFF_PENDING", "ENDED"}},
		"owner_scope":           {Type: "string", Required: false},
		"blocked_on":            {Type: "array[object]", Required: false, Description: "Structured blocker refs with kind/detail"},
		"decision_needed_from":  {Type: "string", Required: false},
		"decision_type":         {Type: "string", Required: false},
		"keep_session_active":   {Type: "boolean", Required: false},
		"handoff_to":            {Type: "string", Required: false},
		"related_doc_keys":      {Type: "array[string]", Required: false},
		"related_artifact_refs": {Type: "array[object]", Required: false},
		"iterations":            {Type: "integer", Required: false},
		"total_input_tokens":    {Type: "integer", Required: false},
		"total_output_tokens":   {Type: "integer", Required: false},
		"tool_calls":            {Type: "integer", Required: false},
		"updated_at":            {Type: "string", Required: false},
	}
}

type rpcDescribeParams struct {
	Method string `json:"method"`
}

func (h *Handler) rpcDescribe(_ context.Context, raw json.RawMessage) (any, *RPCError) {
	var p rpcDescribeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	method := strings.TrimSpace(p.Method)
	if method == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "method is required. Use rpc.methods.list to see all available methods"}
	}
	schema, ok := rpcMethodSchemas[method]
	if !ok {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "unknown method: " + method + ". Use rpc.methods.list to see all available methods"}
	}
	return schema, nil
}

func (h *Handler) rpcMethodsList(_ context.Context, _ json.RawMessage) (any, *RPCError) {
	type methodSummary struct {
		Method      string `json:"method"`
		Description string `json:"description"`
	}
	methods := make([]methodSummary, 0, len(rpcMethodSchemas))
	for _, s := range rpcMethodSchemas {
		methods = append(methods, methodSummary{Method: s.Method, Description: s.Description})
	}
	sort.Slice(methods, func(i, j int) bool {
		return methods[i].Method < methods[j].Method
	})
	return map[string]any{
		"count":   len(methods),
		"methods": methods,
	}, nil
}
