package main

type ServiceDirectionBriefRecord struct {
	DirectionID     string `json:"direction_id"`
	IdempotencyKey  string `json:"idempotency_key,omitempty"`
	WorkspaceID     string `json:"workspace_id"`
	Title           string `json:"title"`
	Description     string `json:"description,omitempty"`
	ConstraintsJSON string `json:"constraints_json,omitempty"`
	BudgetCapMicros int64  `json:"budget_cap_micros,omitempty"`
	Status          string `json:"status"`
	CreatedBy       string `json:"created_by,omitempty"`
	UpdatedBy       string `json:"updated_by,omitempty"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type ServiceCandidateRecord struct {
	CandidateID        string `json:"candidate_id"`
	IdempotencyKey     string `json:"idempotency_key,omitempty"`
	WorkspaceID        string `json:"workspace_id"`
	DirectionID        string `json:"direction_id"`
	Title              string `json:"title"`
	TargetUser         string `json:"target_user,omitempty"`
	UserPain           string `json:"user_pain,omitempty"`
	SolutionSummary    string `json:"solution_summary,omitempty"`
	Distribution       string `json:"distribution,omitempty"`
	Monetization       string `json:"monetization,omitempty"`
	ImplementationSize string `json:"implementation_size,omitempty"`
	RiskLevel          string `json:"risk_level,omitempty"`
	Score              int    `json:"score,omitempty"`
	EvidencePlanJSON   string `json:"evidence_plan_json,omitempty"`
	Status             string `json:"status"`
	SelectedBy         string `json:"selected_by,omitempty"`
	SelectedAt         string `json:"selected_at,omitempty"`
	CreatedBy          string `json:"created_by,omitempty"`
	UpdatedBy          string `json:"updated_by,omitempty"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
}

type ServiceRunRecord struct {
	RunID            string `json:"run_id"`
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
	WorkspaceID      string `json:"workspace_id"`
	CandidateID      string `json:"candidate_id"`
	ProjectID        string `json:"project_id"`
	Title            string `json:"title"`
	DeployTarget     string `json:"deploy_target,omitempty"`
	PublicURL        string `json:"public_url,omitempty"`
	HealthCheckURL   string `json:"health_check_url,omitempty"`
	BudgetAccountID  string `json:"budget_account_id,omitempty"`
	BudgetCapMicros  int64  `json:"budget_cap_micros,omitempty"`
	CredentialPolicy string `json:"credential_policy"`
	Status           string `json:"status"`
	StartedBy        string `json:"started_by,omitempty"`
	UpdatedBy        string `json:"updated_by,omitempty"`
	StartedAt        string `json:"started_at"`
	UpdatedAt        string `json:"updated_at"`
	CompletedAt      string `json:"completed_at,omitempty"`
}

type ServiceApprovalGrantRecord struct {
	GrantID        string `json:"grant_id"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	WorkspaceID    string `json:"workspace_id"`
	RunID          string `json:"run_id"`
	GrantType      string `json:"grant_type"`
	ScopeJSON      string `json:"scope_json,omitempty"`
	ApprovalRef    string `json:"approval_ref,omitempty"`
	Status         string `json:"status"`
	ApprovedBy     string `json:"approved_by,omitempty"`
	ExpiresAt      string `json:"expires_at,omitempty"`
	CreatedBy      string `json:"created_by,omitempty"`
	UpdatedBy      string `json:"updated_by,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

type ServiceProviderResourceRecord struct {
	ResourceID             string `json:"resource_id"`
	IdempotencyKey         string `json:"idempotency_key,omitempty"`
	WorkspaceID            string `json:"workspace_id"`
	RunID                  string `json:"run_id"`
	Provider               string `json:"provider"`
	ResourceType           string `json:"resource_type"`
	ResourceRef            string `json:"resource_ref,omitempty"`
	CredentialVaultEntryID string `json:"credential_vault_entry_id,omitempty"`
	ApprovalGrantID        string `json:"approval_grant_id,omitempty"`
	Paid                   bool   `json:"paid"`
	CostCapMicros          int64  `json:"cost_cap_micros,omitempty"`
	Status                 string `json:"status"`
	TTLExpiresAt           string `json:"ttl_expires_at,omitempty"`
	CreatedBy              string `json:"created_by,omitempty"`
	UpdatedBy              string `json:"updated_by,omitempty"`
	CreatedAt              string `json:"created_at"`
	UpdatedAt              string `json:"updated_at"`
}

type ServiceSpendReceiptRecord struct {
	ReceiptID          string `json:"receipt_id"`
	IdempotencyKey     string `json:"idempotency_key,omitempty"`
	WorkspaceID        string `json:"workspace_id"`
	RunID              string `json:"run_id"`
	ProviderResourceID string `json:"provider_resource_id,omitempty"`
	LedgerEntryID      string `json:"ledger_entry_id,omitempty"`
	AmountMicros       int64  `json:"amount_micros"`
	Currency           string `json:"currency"`
	ExternalReceiptRef string `json:"external_receipt_ref,omitempty"`
	EvidenceRef        string `json:"evidence_ref"`
	RecordedBy         string `json:"recorded_by,omitempty"`
	RecordedAt         string `json:"recorded_at"`
}

type ServiceRevenueObservationRecord struct {
	ObservationID      string `json:"observation_id"`
	IdempotencyKey     string `json:"idempotency_key,omitempty"`
	WorkspaceID        string `json:"workspace_id"`
	RunID              string `json:"run_id"`
	AmountMicros       int64  `json:"amount_micros"`
	Currency           string `json:"currency"`
	Source             string `json:"source"`
	ExternalReceiptRef string `json:"external_receipt_ref,omitempty"`
	EvidenceRef        string `json:"evidence_ref"`
	ObservedAt         string `json:"observed_at"`
	RecordedBy         string `json:"recorded_by,omitempty"`
	CreatedAt          string `json:"created_at"`
}

type ServiceOutcomeRecord struct {
	OutcomeID            string `json:"outcome_id"`
	IdempotencyKey       string `json:"idempotency_key,omitempty"`
	WorkspaceID          string `json:"workspace_id"`
	RunID                string `json:"run_id"`
	PublicURL            string `json:"public_url,omitempty"`
	DeployHealthStatus   string `json:"deploy_health_status,omitempty"`
	DeployEvidenceRef    string `json:"deploy_evidence_ref,omitempty"`
	AnalyticsJSON        string `json:"analytics_json,omitempty"`
	AnalyticsEvidenceRef string `json:"analytics_evidence_ref,omitempty"`
	SpendMicros          int64  `json:"spend_micros,omitempty"`
	SpendEvidenceRef     string `json:"spend_evidence_ref,omitempty"`
	RevenueMicros        int64  `json:"revenue_micros,omitempty"`
	RevenueEvidenceRef   string `json:"revenue_evidence_ref,omitempty"`
	QualityScore         int    `json:"quality_score,omitempty"`
	Decision             string `json:"decision"`
	DecisionReason       string `json:"decision_reason"`
	EvidenceRefsJSON     string `json:"evidence_refs_json,omitempty"`
	RecordedBy           string `json:"recorded_by,omitempty"`
	RecordedAt           string `json:"recorded_at"`
}

type ServiceCoordinationRecord struct {
	SnapshotAt          string                            `json:"snapshot_at"`
	CoordinationVersion string                            `json:"coordination_version"`
	Direction           ServiceDirectionBriefRecord       `json:"direction"`
	Candidate           ServiceCandidateRecord            `json:"candidate"`
	Run                 ServiceRunRecord                  `json:"run"`
	Project             *ProjectCoordinationRecord        `json:"project,omitempty"`
	ApprovalGrants      []ServiceApprovalGrantRecord      `json:"approval_grants,omitempty"`
	ProviderResources   []ServiceProviderResourceRecord   `json:"provider_resources,omitempty"`
	SpendReceipts       []ServiceSpendReceiptRecord       `json:"spend_receipts,omitempty"`
	RevenueObservations []ServiceRevenueObservationRecord `json:"revenue_observations,omitempty"`
	Outcomes            []ServiceOutcomeRecord            `json:"outcomes,omitempty"`
}
