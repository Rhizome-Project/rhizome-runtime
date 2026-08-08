package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ServiceVentureTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
	name        string
	method      string
	description string
	properties  map[string]any
	required    []string
	write       bool
}

func NewServiceVentureTool(client *RhizomeClient, workspaceID, agentID, name, method, description string, properties map[string]any, required []string, write bool) *ServiceVentureTool {
	return &ServiceVentureTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		name:        strings.TrimSpace(name),
		method:      strings.TrimSpace(method),
		description: strings.TrimSpace(description),
		properties:  properties,
		required:    required,
		write:       write,
	}
}

func (t *ServiceVentureTool) Name() string { return t.name }

func (t *ServiceVentureTool) Description() string { return t.description }

func (t *ServiceVentureTool) Parameters() map[string]any {
	props := map[string]any{}
	for key, value := range t.properties {
		props[key] = value
	}
	return map[string]any{
		"type":       "object",
		"properties": props,
		"required":   t.required,
	}
}

func (t *ServiceVentureTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || (t.write && t.agentID == "") {
		return &ToolResult{Output: t.name + " is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	params := map[string]any{}
	for key, value := range args {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		params[key] = value
	}
	params["workspace_id"] = t.workspaceID
	if t.write {
		params["actor_id"] = t.agentID
	}
	result, err := t.client.CallServiceVenture(ctx, t.method, params)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("%s failed: %v", t.name, err), IsError: true}
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil || len(raw) == 0 {
		return &ToolResult{Output: t.name + " completed"}
	}
	return &ToolResult{Output: string(raw)}
}

func RegisterServiceVentureTools(registry *ToolRegistry, client *RhizomeClient, workspaceID, agentID string) {
	if registry == nil {
		return
	}
	for _, spec := range serviceVentureToolSpecs() {
		registry.Register(NewServiceVentureTool(client, workspaceID, agentID, spec.name, spec.method, spec.description, spec.properties, spec.required, spec.write))
	}
}

type serviceVentureToolSpec struct {
	name        string
	method      string
	description string
	properties  map[string]any
	required    []string
	write       bool
}

func serviceVentureToolSpecs() []serviceVentureToolSpec {
	return []serviceVentureToolSpec{
		{
			name:        "service_direction_upsert",
			method:      "service.direction.upsert",
			description: "Create or update a durable service-factory direction brief: target market, constraints, budget cap, and portfolio status. Use this when autonomous scouting discovers a repeatable family of small services to build.",
			properties: serviceProps(
				serviceString("direction_id", "Stable direction id. Use a deterministic id for the same portfolio direction."),
				serviceString("idempotency_key", "Optional idempotency key for retries."),
				serviceString("title", "Short direction title."),
				serviceString("description", "Concrete service-factory direction and operator intent."),
				serviceString("constraints_json", "JSON object with constraints such as deployment, ads, credentials, and product non-goals."),
				serviceInteger("budget_cap_micros", "Optional direction-level budget cap in micros."),
				serviceString("status", "DRAFT, ACTIVE, PAUSED, or ARCHIVED."),
			),
			required: []string{"title"},
			write:    true,
		},
		{
			name:        "service_direction_list",
			method:      "service.direction.list",
			description: "List service-factory direction briefs visible in this workspace before proposing duplicate directions.",
			properties:  serviceProps(serviceString("status", "Optional status filter."), serviceInteger("limit", "Maximum rows.")),
		},
		{
			name:        "service_direction_get",
			method:      "service.direction.get",
			description: "Read one service-factory direction brief by id.",
			properties:  serviceProps(serviceString("direction_id", "Direction id.")),
			required:    []string{"direction_id"},
		},
		{
			name:        "service_candidate_upsert",
			method:      "service.candidate.upsert",
			description: "Create or update a candidate service idea under a direction, including user pain, solution, distribution, monetization, score, and evidence plan. Prefer updating an existing candidate over duplicating the same idea.",
			properties: serviceProps(
				serviceString("candidate_id", "Stable candidate id."),
				serviceString("idempotency_key", "Optional idempotency key for retries."),
				serviceString("direction_id", "Parent direction id."),
				serviceString("title", "Candidate service title."),
				serviceString("target_user", "Specific target user."),
				serviceString("user_pain", "Concrete user pain."),
				serviceString("solution_summary", "Small-service solution summary."),
				serviceString("distribution", "Distribution hypothesis."),
				serviceString("monetization", "Monetization hypothesis."),
				serviceString("implementation_size", "Small/medium/large estimate."),
				serviceString("risk_level", "Risk level."),
				serviceInteger("score", "0-100 priority score."),
				serviceString("evidence_plan_json", "JSON object describing deploy, analytics, spend, revenue, and QA evidence to collect."),
				serviceString("status", "PROPOSED, SELECTED, REJECTED, or PARKED."),
			),
			required: []string{"direction_id", "title"},
			write:    true,
		},
		{
			name:        "service_candidate_list",
			method:      "service.candidate.list",
			description: "List candidate service ideas before selecting or creating another candidate.",
			properties:  serviceProps(serviceString("direction_id", "Optional direction filter."), serviceString("status", "Optional status filter."), serviceInteger("limit", "Maximum rows.")),
		},
		{
			name:        "service_candidate_get",
			method:      "service.candidate.get",
			description: "Read one candidate service idea by id.",
			properties:  serviceProps(serviceString("candidate_id", "Candidate id.")),
			required:    []string{"candidate_id"},
		},
		{
			name:        "service_run_start",
			method:      "service.run.start",
			description: "Start a durable service venture run for a SELECTED candidate and a Rhizome project. Use after a project exists; this is the bridge from portfolio idea to coordinated implementation/launch work.",
			properties:  serviceRunProperties(),
			required:    []string{"run_id", "candidate_id", "project_id", "title"},
			write:       true,
		},
		{
			name:        "service_run_update",
			method:      "service.run.update",
			description: "Update a non-terminal service run status or launch metadata. Only the run starter, a project-role holder, or an operator principal may mutate an existing run.",
			properties:  serviceRunProperties(),
			required:    []string{"run_id"},
			write:       true,
		},
		{
			name:        "service_run_list",
			method:      "service.run.list",
			description: "List service venture runs by candidate, project, status, or workspace before starting duplicate runs.",
			properties:  serviceProps(serviceString("candidate_id", "Optional candidate filter."), serviceString("project_id", "Optional project filter."), serviceString("status", "Optional status filter."), serviceInteger("limit", "Maximum rows.")),
		},
		{
			name:        "service_run_get",
			method:      "service.run.get",
			description: "Read one service venture run by id.",
			properties:  serviceProps(serviceString("run_id", "Service run id.")),
			required:    []string{"run_id"},
		},
		{
			name:        "service_coordination_get",
			method:      "service.coordination.get",
			description: "Read the full coordination packet for a service run: direction, candidate, project, resources, spend, revenue, outcomes, and current run status.",
			properties:  serviceProps(serviceString("run_id", "Service run id.")),
			required:    []string{"run_id"},
		},
		{
			name:        "service_approval_grant",
			method:      "service.approval.grant",
			description: "Request or record a service approval grant. Agent principals should create PENDING/REJECTED/REVOKED records or cite operator approval; they cannot self-approve APPROVED grants.",
			properties: serviceProps(
				serviceString("grant_id", "Stable grant id."),
				serviceString("idempotency_key", "Optional idempotency key for retries."),
				serviceString("run_id", "Service run id."),
				serviceString("grant_type", "Grant type, such as paid_resource or credential_use."),
				serviceString("scope_json", "JSON object describing provider/resource/cap scope."),
				serviceString("approval_ref", "Operator/system approval reference. Required for APPROVED status."),
				serviceString("status", "PENDING, APPROVED, REJECTED, EXPIRED, or REVOKED."),
				serviceString("approved_by", "Human/operator/system approver id when approved."),
				serviceString("expires_at", "Optional RFC3339 expiry."),
			),
			required: []string{"run_id", "grant_type", "scope_json"},
			write:    true,
		},
		{
			name:        "service_resource_record",
			method:      "service.resource.record",
			description: "Record a provider resource for a service run. Paid or credentialed resources require run credential_policy=APPROVED plus an approved grant; store only vault references, never secret material.",
			properties: serviceProps(
				serviceString("resource_id", "Stable resource id."),
				serviceString("idempotency_key", "Optional idempotency key for retries."),
				serviceString("run_id", "Service run id."),
				serviceString("provider", "Provider, such as vercel, cloudflare, google-ads, analytics."),
				serviceString("resource_type", "Resource type, such as project, deployment, campaign, analytics-property."),
				serviceString("resource_ref", "External resource reference, not a secret."),
				serviceString("credential_vault_entry_id", "Vault reference only; never raw secret material."),
				serviceString("approval_grant_id", "Approved grant id for paid/credentialed resources."),
				serviceBoolean("paid", "Whether this is a paid resource."),
				serviceInteger("cost_cap_micros", "Resource cost cap in micros."),
				serviceString("status", "PENDING_APPROVAL, PROVISIONED, ACTIVE, REVOKED, or FAILED."),
				serviceString("ttl_expires_at", "Optional RFC3339 TTL."),
			),
			required: []string{"run_id", "provider", "resource_type"},
			write:    true,
		},
		{
			name:        "service_spend_record",
			method:      "service.spend.record",
			description: "Record service spend evidence. Positive spend must point to a budget ledger SPEND entry for the same run/account and a structured evidence doc/artifact/event.",
			properties: serviceProps(
				serviceString("receipt_id", "Stable spend receipt id."),
				serviceString("idempotency_key", "Optional idempotency key for retries."),
				serviceString("run_id", "Service run id."),
				serviceString("provider_resource_id", "Optional provider resource id."),
				serviceString("ledger_entry_id", "Required budget ledger SPEND entry id for positive spend."),
				serviceInteger("amount_micros", "Spend amount in micros."),
				serviceString("currency", "Currency, default USD."),
				serviceString("external_receipt_ref", "External receipt reference."),
				serviceString("evidence_ref", "Structured workspace evidence doc/artifact/event ref."),
			),
			required: []string{"run_id", "amount_micros", "evidence_ref"},
			write:    true,
		},
		{
			name:        "service_revenue_record",
			method:      "service.revenue.record",
			description: "Record revenue or monetization observation evidence for a service run.",
			properties: serviceProps(
				serviceString("observation_id", "Stable revenue observation id."),
				serviceString("idempotency_key", "Optional idempotency key for retries."),
				serviceString("run_id", "Service run id."),
				serviceInteger("amount_micros", "Revenue amount in micros."),
				serviceString("currency", "Currency, default USD."),
				serviceString("source", "Revenue/analytics source."),
				serviceString("external_receipt_ref", "External receipt/reference."),
				serviceString("evidence_ref", "Structured workspace evidence doc/artifact/event ref."),
				serviceString("observed_at", "Optional RFC3339 observation time."),
			),
			required: []string{"run_id", "source", "evidence_ref"},
			write:    true,
		},
		{
			name:        "service_outcome_record",
			method:      "service.outcome.record",
			description: "Record a service run outcome. CONTINUE/ITERATE requires public URL, PASS deploy health, analytics JSON, spend evidence, and structured evidence refs; KILL/BLOCKED/HOLD also require durable evidence refs.",
			properties: serviceProps(
				serviceString("outcome_id", "Stable outcome id."),
				serviceString("idempotency_key", "Optional idempotency key for retries."),
				serviceString("run_id", "Service run id."),
				serviceString("public_url", "Non-local public URL for continue/iterate outcomes."),
				serviceString("deploy_health_status", "PASS, FAIL, or UNKNOWN."),
				serviceString("deploy_evidence_ref", "Structured deploy evidence ref."),
				serviceString("analytics_json", "JSON object with analytics summary."),
				serviceString("analytics_evidence_ref", "Structured analytics evidence ref."),
				serviceInteger("spend_micros", "Spend amount in micros."),
				serviceString("spend_evidence_ref", "Structured spend evidence ref."),
				serviceInteger("revenue_micros", "Revenue amount in micros."),
				serviceString("revenue_evidence_ref", "Structured revenue evidence ref."),
				serviceInteger("quality_score", "0-100 quality score."),
				serviceString("decision", "CONTINUE, ITERATE, KILL, BLOCKED, or HOLD."),
				serviceString("decision_reason", "Specific evidence-backed reason."),
				serviceString("evidence_refs_json", "JSON string array of all structured evidence refs."),
			),
			required: []string{"run_id", "decision", "decision_reason", "evidence_refs_json"},
			write:    true,
		},
	}
}

func serviceRunProperties() map[string]any {
	return serviceProps(
		serviceString("run_id", "Stable service run id."),
		serviceString("idempotency_key", "Optional idempotency key for retries."),
		serviceString("candidate_id", "Selected candidate id."),
		serviceString("project_id", "Rhizome project id that will own implementation/launch coordination."),
		serviceString("title", "Run title."),
		serviceString("deploy_target", "Deployment target, such as vercel, cloudflare, static-hosting."),
		serviceString("public_url", "Public URL when known."),
		serviceString("health_check_url", "Health check URL when known."),
		serviceString("budget_account_id", "Budget account id for spend ledger binding."),
		serviceInteger("budget_cap_micros", "Run budget cap in micros."),
		serviceString("credential_policy", "PENDING_APPROVAL, FREE_TIER_ONLY, or APPROVED."),
		serviceString("status", "PLANNED, ACTIVE, BLOCKED, DEPLOYED, MEASURING, COMPLETED, KILLED, or CANCELLED."),
	)
}

func serviceProps(items ...map[string]any) map[string]any {
	props := map[string]any{}
	for _, item := range items {
		for key, value := range item {
			props[key] = value
		}
	}
	return props
}

func serviceString(name, description string) map[string]any {
	return map[string]any{name: map[string]any{"type": "string", "description": description}}
}

func serviceInteger(name, description string) map[string]any {
	return map[string]any{name: map[string]any{"type": "integer", "description": description}}
}

func serviceBoolean(name, description string) map[string]any {
	return map[string]any{name: map[string]any{"type": "boolean", "description": description}}
}
