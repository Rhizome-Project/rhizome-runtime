package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type BudgetTool struct {
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

func NewBudgetTool(client *RhizomeClient, workspaceID, agentID, name, method, description string, properties map[string]any, required []string, write bool) *BudgetTool {
	return &BudgetTool{
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

func (t *BudgetTool) Name() string { return t.name }

func (t *BudgetTool) Description() string { return t.description }

func (t *BudgetTool) Parameters() map[string]any {
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

func (t *BudgetTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
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
	if _, ok := params["workspace_id"]; !ok {
		params["workspace_id"] = t.workspaceID
	}
	if t.write {
		if _, ok := params["agent_id"]; !ok {
			params["agent_id"] = t.agentID
		}
	}
	var result map[string]any
	if err := t.client.call(ctx, t.method, params, &result); err != nil {
		return &ToolResult{Output: fmt.Sprintf("%s failed: %v", t.name, err), IsError: true}
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil || len(raw) == 0 {
		return &ToolResult{Output: t.name + " completed"}
	}
	return &ToolResult{Output: string(raw)}
}

func RegisterBudgetTools(registry *ToolRegistry, client *RhizomeClient, workspaceID, agentID string) {
	if registry == nil {
		return
	}
	for _, spec := range budgetToolSpecs() {
		registry.Register(NewBudgetTool(client, workspaceID, agentID, spec.name, spec.method, spec.description, spec.properties, spec.required, spec.write))
	}
}

type budgetToolSpec struct {
	name        string
	method      string
	description string
	properties  map[string]any
	required    []string
	write       bool
}

func budgetToolSpecs() []budgetToolSpec {
	scopeProps := budgetProps(
		budgetString("account_id", "Budget account id."),
		budgetString("task_id", "Task id for spend/reservation scope."),
		budgetString("run_id", "Service run id or runtime run id for spend/reservation scope."),
		budgetString("provider_id", "Provider id for spend/reservation scope."),
		budgetString("model", "Model or external spend category."),
		budgetInteger("amount_micros", "Amount in micros."),
		budgetString("reason", "Short reason/evidence summary."),
	)
	return []budgetToolSpec{
		{
			name:        "budget_account_ensure",
			method:      "budget.account.ensure",
			description: "Create or update a hard budget account for a service run, project, agent, or operator-approved portfolio scope. Use before reserving or recording positive service spend.",
			properties: budgetProps(
				budgetString("account_id", "Stable budget account id."),
				budgetString("principal_type", "Principal type, such as service_run, project, agent, or human."),
				budgetString("principal_id", "Principal id bound to the account."),
				budgetString("currency", "Currency, default USD."),
				budgetInteger("limit_micros", "Hard limit in micros."),
				budgetString("status", "Budget account status; ACTIVE is the only supported active state."),
			),
			required: []string{"account_id", "principal_type", "principal_id", "limit_micros"},
			write:    true,
		},
		{
			name:        "budget_account_get",
			method:      "budget.account.get",
			description: "Read a hard budget account snapshot before reserving or recording service spend.",
			properties:  budgetProps(budgetString("account_id", "Budget account id.")),
			required:    []string{"account_id"},
		},
		{
			name:        "budget_reserve",
			method:      "budget.reserve",
			description: "Reserve budget for a concrete run/task/provider/model binding before paid service work. Use stable reservation_id and idempotency_key for retries.",
			properties: budgetProps(
				budgetString("reservation_id", "Stable reservation id."),
				budgetString("idempotency_key", "Required idempotency key."),
				budgetString("account_id", "Budget account id."),
				budgetString("task_id", "Task id for scope."),
				budgetString("run_id", "Service run id for scope."),
				budgetString("provider_id", "Provider id."),
				budgetString("model", "Model or external spend category."),
				budgetInteger("amount_micros", "Reserved amount in micros."),
				budgetString("reason", "Short reason/evidence summary."),
			),
			required: []string{"reservation_id", "idempotency_key", "account_id", "task_id", "run_id", "provider_id", "model", "amount_micros"},
			write:    true,
		},
		{
			name:        "budget_spend",
			method:      "budget.spend",
			description: "Capture spend against an existing budget reservation. The resulting entry_id is required by service_spend_record for positive spend.",
			properties:  budgetProps(budgetString("entry_id", "Stable spend ledger entry id."), budgetString("idempotency_key", "Required idempotency key."), budgetString("reservation_id", "Existing reservation id."), scopeProps),
			required:    []string{"entry_id", "idempotency_key", "account_id", "reservation_id", "task_id", "run_id", "provider_id", "model", "amount_micros"},
			write:       true,
		},
		{
			name:        "budget_release",
			method:      "budget.release",
			description: "Release unused reserved budget from an existing reservation.",
			properties:  budgetProps(budgetString("entry_id", "Stable release ledger entry id."), budgetString("idempotency_key", "Required idempotency key."), budgetString("reservation_id", "Existing reservation id."), scopeProps),
			required:    []string{"entry_id", "idempotency_key", "account_id", "reservation_id", "task_id", "run_id", "provider_id", "model", "amount_micros"},
			write:       true,
		},
		{
			name:        "budget_refund",
			method:      "budget.refund",
			description: "Record a refund against an existing spend ledger entry. Use only with concrete billing evidence.",
			properties:  budgetProps(budgetString("entry_id", "Stable refund ledger entry id."), budgetString("idempotency_key", "Required idempotency key."), budgetString("source_entry_id", "Existing spend entry being refunded."), scopeProps),
			required:    []string{"entry_id", "idempotency_key", "account_id", "source_entry_id", "task_id", "run_id", "provider_id", "model", "amount_micros"},
			write:       true,
		},
		{
			name:        "budget_ledger_list",
			method:      "budget.ledger.list",
			description: "List budget ledger entries by account, reservation, task, or run before binding service spend evidence.",
			properties:  budgetProps(budgetString("account_id", "Optional budget account id."), budgetString("reservation_id", "Optional reservation id."), budgetString("task_id", "Optional task id."), budgetString("run_id", "Optional run id."), budgetInteger("limit", "Maximum rows.")),
		},
		{
			name:        "budget_reservations_list",
			method:      "budget.reservations.list",
			description: "List budget reservations by account, agent, task, run, or status.",
			properties:  budgetProps(budgetString("account_id", "Optional budget account id."), budgetString("agent_id", "Optional agent id."), budgetString("task_id", "Optional task id."), budgetString("run_id", "Optional run id."), budgetString("status", "Optional OPEN/CLOSED status."), budgetInteger("limit", "Maximum rows.")),
		},
	}
}

func budgetProps(items ...map[string]any) map[string]any {
	props := map[string]any{}
	for _, item := range items {
		for key, value := range item {
			props[key] = value
		}
	}
	return props
}

func budgetString(name, description string) map[string]any {
	return map[string]any{name: map[string]any{"type": "string", "description": description}}
}

func budgetInteger(name, description string) map[string]any {
	return map[string]any{name: map[string]any{"type": "integer", "description": description}}
}
