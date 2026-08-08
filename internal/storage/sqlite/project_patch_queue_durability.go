package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const ProjectPatchQueueDurabilityProofContract = "project_patch_queue_durability_proof.v2"

type ProjectPatchQueueDurabilityProof struct {
	Contract                     string                                        `json:"contract"`
	State                        string                                        `json:"state"`
	Durable                      bool                                          `json:"durable"`
	Message                      string                                        `json:"message,omitempty"`
	TablePresent                 bool                                          `json:"table_present"`
	PrimaryKeyPresent            bool                                          `json:"primary_key_present"`
	LiveBranchIndexPresent       bool                                          `json:"live_branch_index_present"`
	ClaimIndexPresent            bool                                          `json:"claim_index_present"`
	MaterializationIndexPresent  bool                                          `json:"materialization_index_present"`
	SupersessionIndexPresent     bool                                          `json:"supersession_index_present"`
	ReviewTaskReceiptPresent     bool                                          `json:"review_task_receipt_present"`
	DecisionContinuationPresent  bool                                          `json:"decision_continuation_present"`
	DecisionContinuationMissing  int                                           `json:"decision_continuation_missing"`
	MaterializationStoragePolicy ProjectPatchQueueMaterializationStoragePolicy `json:"materialization_storage_policy"`
	LifecycleColumnsPresent      bool                                          `json:"lifecycle_columns_present"`
	BindingColumnsPresent        bool                                          `json:"binding_columns_present"`
	MissingLifecycleColumns      []string                                      `json:"missing_lifecycle_columns,omitempty"`
	MissingBindingColumns        []string                                      `json:"missing_binding_columns,omitempty"`
	ItemCount                    int                                           `json:"item_count"`
	LiveItemCount                int                                           `json:"live_item_count"`
	ClaimedItemCount             int                                           `json:"claimed_item_count"`
	TerminalItemCount            int                                           `json:"terminal_item_count"`
	MaterializedItemCount        int                                           `json:"materialized_item_count"`
	MaterializationJSONBytes     int64                                         `json:"materialization_json_bytes"`
	LiveMaterializationJSONBytes int64                                         `json:"live_materialization_json_bytes"`
	CheckedAt                    string                                        `json:"checked_at"`
	Digest                       string                                        `json:"digest"`
	Error                        string                                        `json:"error,omitempty"`
}

type ProjectPatchQueueMaterializationStoragePolicy struct {
	MaxFiles                    int   `json:"max_files"`
	MaxFileBytes                int64 `json:"max_file_bytes"`
	MaxTotalBytes               int64 `json:"max_total_bytes"`
	MaxMaterializationJSONBytes int64 `json:"max_materialization_json_bytes"`
	MaxAuthorityProofJSONBytes  int64 `json:"max_authority_proof_json_bytes"`
}

type projectPatchQueueColumnMetadata struct {
	Name string
	PK   int
}

type sqliteIndexMetadata struct {
	Name    string
	Unique  bool
	Partial bool
	Columns []string
	SQL     string
}

// ProjectPatchQueueDurabilityProof is a read-only schema and state proof for the
// storage-backed patch queue. It intentionally proves persistence primitives,
// not semantic readiness to allow repository mutation.
func (s *Store) ProjectPatchQueueDurabilityProof(ctx context.Context) ProjectPatchQueueDurabilityProof {
	if ctx == nil {
		ctx = context.Background()
	}
	proof := ProjectPatchQueueDurabilityProof{
		Contract:                     ProjectPatchQueueDurabilityProofContract,
		State:                        "degraded",
		MaterializationStoragePolicy: projectPatchQueueMaterializationStoragePolicy(),
		CheckedAt:                    time.Now().UTC().Format(time.RFC3339Nano),
	}
	if s == nil || s.db == nil {
		proof.State = "unsupported"
		proof.Message = "sqlite store is not available"
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}

	tablePresent, err := sqliteTablePresent(ctx, s.db, "project_patch_queue_items")
	if err != nil {
		proof.State = "error"
		proof.Message = "patch queue durability proof failed"
		proof.Error = err.Error()
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}
	proof.TablePresent = tablePresent
	if !proof.TablePresent {
		proof.Message = "project_patch_queue_items table is missing"
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}

	columns, err := projectPatchQueueColumnMetadataMap(ctx, s.db)
	if err != nil {
		proof.State = "error"
		proof.Message = "patch queue column metadata query failed"
		proof.Error = err.Error()
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}
	proof.PrimaryKeyPresent = columns["queue_id"].PK == 1 && columns["item_id"].PK == 2

	requiredLifecycleColumns := []string{
		"claimed_by",
		"claim_token",
		"claimed_at",
		"claim_expires_at",
		"decision_doc_key",
		"decision_summary",
		"decided_by",
		"decided_at",
	}
	for _, name := range requiredLifecycleColumns {
		if _, ok := columns[name]; !ok {
			proof.MissingLifecycleColumns = append(proof.MissingLifecycleColumns, name)
		}
	}
	proof.LifecycleColumnsPresent = len(proof.MissingLifecycleColumns) == 0
	requiredBindingColumns := []string{
		"task_id",
		"session_id",
		"run_id",
		"agent_id",
		"principal_type",
		"principal_id",
		"capability_snapshot_id",
		"capability_snapshot_schema",
		"repo_root",
		"base_tree_hash",
		"base_file_hashes_json",
		"context_digest",
		"repo_lease_id",
		"lease_term",
		"attempt",
		"max_attempts",
		"next_retry_at",
		"dead_lettered_at",
		"operation_id",
		"operation_kind",
		"operation_binding_schema",
		"operation_binding_accepted",
		"operation_context_digest",
		"operation_lease_context_digest",
		"operation_mutation_paths_json",
		"operation_bound_by",
		"operation_bound_at",
		"cas_evidence_schema",
		"cas_evidence_accepted",
		"cas_status",
		"cas_patch_digest",
		"cas_evaluation_digest",
		"cas_result_json",
		"cas_test_evidence_json",
		"cas_test_evidence_digest",
		"cas_recorded_by",
		"cas_recorded_at",
		"materialization_schema",
		"materialization_accepted",
		"materialization_json",
		"materialization_digest",
		"materialization_recorded_by",
		"materialization_recorded_at",
		"materialization_authority_proof_json",
		"materialization_authority_proof_digest",
		"rollback_evidence_schema",
		"rollback_evidence_accepted",
		"rollback_evidence_json",
		"rollback_evidence_digest",
		"rollback_recorded_by",
		"rollback_recorded_at",
		"reviewer_advisory_schema",
		"reviewer_advisory_accepted",
		"reviewer_advisory_json",
		"reviewer_advisory_digest",
		"reviewer_recorded_by",
		"reviewer_recorded_at",
		"operator_enablement_schema",
		"operator_enablement_accepted",
		"operator_enablement_json",
		"operator_enablement_digest",
		"operator_enabled_by",
		"operator_enabled_at",
		"supersedes_queue_id",
		"supersedes_item_id",
		"evidence_doc_key",
	}
	for _, name := range requiredBindingColumns {
		if _, ok := columns[name]; !ok {
			proof.MissingBindingColumns = append(proof.MissingBindingColumns, name)
		}
	}
	proof.BindingColumnsPresent = len(proof.MissingBindingColumns) == 0

	indexes, err := sqliteIndexMetadataByName(ctx, s.db, "project_patch_queue_items")
	if err != nil {
		proof.State = "error"
		proof.Message = "patch queue index metadata query failed"
		proof.Error = err.Error()
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}
	proof.LiveBranchIndexPresent = projectPatchQueueLiveBranchIndexReady(indexes["idx_project_patch_queue_items_live_branch"])
	proof.ClaimIndexPresent = projectPatchQueueClaimIndexReady(indexes["idx_project_patch_queue_items_claim"])
	proof.MaterializationIndexPresent = projectPatchQueueMaterializationIndexReady(indexes["idx_project_patch_queue_items_materialization"])
	proof.SupersessionIndexPresent = projectPatchQueueSupersessionIndexReady(indexes["idx_project_patch_queue_items_supersession"])
	reviewTaskReceiptPresent, err := projectPatchQueueReviewTaskReceiptProjectionReady(ctx, s.db)
	if err != nil {
		proof.State = "error"
		proof.Message = "patch queue review task receipt projection query failed"
		proof.Error = err.Error()
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}
	proof.ReviewTaskReceiptPresent = reviewTaskReceiptPresent
	decisionContinuationPresent, err := projectPatchQueueDecisionContinuationOutboxReady(ctx, s.db)
	if err != nil {
		proof.State = "error"
		proof.Message = "patch queue decision continuation outbox query failed"
		proof.Error = err.Error()
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}
	proof.DecisionContinuationPresent = decisionContinuationPresent
	if proof.DecisionContinuationPresent {
		missing, err := projectPatchQueueDecisionContinuationMissingCount(ctx, s.db)
		if err != nil {
			proof.State = "error"
			proof.Message = "patch queue decision continuation state query failed"
			proof.Error = err.Error()
			return finalizeProjectPatchQueueDurabilityProof(proof)
		}
		proof.DecisionContinuationMissing = missing
	}

	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN state IN ('PROPOSED', 'CLAIMED') THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN state = 'CLAIMED' THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN state IN ('ACCEPTED', 'INTEGRATED', 'REJECTED', 'BLOCKED', 'CANCELED') THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN materialization_accepted = 1 AND materialization_json <> '' AND materialization_json <> '{}' THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN materialization_accepted = 1 AND materialization_json <> '' AND materialization_json <> '{}' THEN LENGTH(CAST(materialization_json AS BLOB)) ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN state IN ('PROPOSED', 'CLAIMED') AND materialization_accepted = 1 AND materialization_json <> '' AND materialization_json <> '{}' THEN LENGTH(CAST(materialization_json AS BLOB)) ELSE 0 END), 0)
  FROM project_patch_queue_items`).Scan(
		&proof.ItemCount,
		&proof.LiveItemCount,
		&proof.ClaimedItemCount,
		&proof.TerminalItemCount,
		&proof.MaterializedItemCount,
		&proof.MaterializationJSONBytes,
		&proof.LiveMaterializationJSONBytes,
	); err != nil {
		proof.State = "error"
		proof.Message = "patch queue item count query failed"
		proof.Error = err.Error()
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}

	proof.Durable = proof.TablePresent &&
		proof.PrimaryKeyPresent &&
		proof.LiveBranchIndexPresent &&
		proof.ClaimIndexPresent &&
		proof.MaterializationIndexPresent &&
		proof.SupersessionIndexPresent &&
		proof.ReviewTaskReceiptPresent &&
		proof.DecisionContinuationPresent &&
		proof.DecisionContinuationMissing == 0 &&
		proof.LifecycleColumnsPresent &&
		proof.BindingColumnsPresent
	if proof.Durable {
		proof.State = "ok"
		proof.Message = "project patch queue durability primitives are present"
		return finalizeProjectPatchQueueDurabilityProof(proof)
	}

	reasons := make([]string, 0, 4)
	if !proof.PrimaryKeyPresent {
		reasons = append(reasons, "composite primary key queue_id/item_id is missing")
	}
	if !proof.LiveBranchIndexPresent {
		reasons = append(reasons, "live branch uniqueness index is missing or not lifecycle-aware")
	}
	if !proof.ClaimIndexPresent {
		reasons = append(reasons, "claim lookup index is missing")
	}
	if !proof.MaterializationIndexPresent {
		reasons = append(reasons, "materialization evidence index is missing")
	}
	if !proof.SupersessionIndexPresent {
		reasons = append(reasons, "supersession lookup index is missing")
	}
	if !proof.ReviewTaskReceiptPresent {
		reasons = append(reasons, "review task receipt projection primitives are missing")
	}
	if !proof.DecisionContinuationPresent {
		reasons = append(reasons, "decision continuation outbox primitives are missing")
	}
	if proof.DecisionContinuationMissing > 0 {
		reasons = append(reasons, fmt.Sprintf("decision continuation outbox is missing %d terminal item receipts", proof.DecisionContinuationMissing))
	}
	if !proof.LifecycleColumnsPresent {
		reasons = append(reasons, "lifecycle columns are missing: "+strings.Join(proof.MissingLifecycleColumns, ", "))
	}
	if !proof.BindingColumnsPresent {
		reasons = append(reasons, "binding columns are missing: "+strings.Join(proof.MissingBindingColumns, ", "))
	}
	proof.Message = "project patch queue durability is incomplete: " + strings.Join(reasons, "; ")
	return finalizeProjectPatchQueueDurabilityProof(proof)
}

func VerifyProjectPatchQueueDurabilityProof(proof ProjectPatchQueueDurabilityProof) error {
	if strings.TrimSpace(proof.Contract) != ProjectPatchQueueDurabilityProofContract {
		return fmt.Errorf("project patch queue durability contract is unsupported")
	}
	if !isProjectPatchQueueDurabilityDigest(proof.Digest) {
		return fmt.Errorf("project patch queue durability digest is required")
	}
	if got := digestProjectPatchQueueDurabilityProof(proof); got != proof.Digest {
		return fmt.Errorf("project patch queue durability digest mismatch")
	}
	expectedDurable := proof.TablePresent &&
		proof.PrimaryKeyPresent &&
		proof.LiveBranchIndexPresent &&
		proof.ClaimIndexPresent &&
		proof.MaterializationIndexPresent &&
		proof.SupersessionIndexPresent &&
		proof.ReviewTaskReceiptPresent &&
		proof.DecisionContinuationPresent &&
		proof.DecisionContinuationMissing == 0 &&
		proof.LifecycleColumnsPresent &&
		proof.BindingColumnsPresent
	if proof.Durable != expectedDurable {
		return fmt.Errorf("project patch queue durability flag does not match schema primitives")
	}
	if proof.MaterializationStoragePolicy != projectPatchQueueMaterializationStoragePolicy() {
		return fmt.Errorf("project patch queue materialization storage policy does not match runtime policy")
	}
	switch strings.ToLower(strings.TrimSpace(proof.State)) {
	case "ok":
		if !proof.Durable {
			return fmt.Errorf("project patch queue durability reports ok without durable proof")
		}
	case "degraded", "unsupported":
		if proof.Durable {
			return fmt.Errorf("project patch queue durability reports %s while durable=true", strings.TrimSpace(proof.State))
		}
	case "error":
		if strings.TrimSpace(proof.Error) == "" {
			return fmt.Errorf("project patch queue durability error state requires error detail")
		}
	default:
		return fmt.Errorf("project patch queue durability state %q is unsupported", proof.State)
	}
	return nil
}

func projectPatchQueueMaterializationStoragePolicy() ProjectPatchQueueMaterializationStoragePolicy {
	return ProjectPatchQueueMaterializationStoragePolicy{
		MaxFiles:                    ProjectPatchQueueMaterializationMaxFiles,
		MaxFileBytes:                ProjectPatchQueueMaterializationMaxFileBytes,
		MaxTotalBytes:               ProjectPatchQueueMaterializationMaxTotalBytes,
		MaxMaterializationJSONBytes: ProjectPatchQueueMaterializationMaxJSONBytes,
		MaxAuthorityProofJSONBytes:  ProjectPatchQueueMaterializationMaxAuthorityProofJSONBytes,
	}
}

func sqliteTablePresent(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var got string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&got)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return strings.TrimSpace(got) == name, nil
}

func projectPatchQueueColumnMetadataMap(ctx context.Context, db *sql.DB) (map[string]projectPatchQueueColumnMetadata, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(project_patch_queue_items)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]projectPatchQueueColumnMetadata)
	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out[name] = projectPatchQueueColumnMetadata{Name: name, PK: pk}
	}
	return out, rows.Err()
}

func sqliteIndexMetadataByName(ctx context.Context, db *sql.DB, tableName string) (map[string]sqliteIndexMetadata, error) {
	if !sqliteBareIdentifier(tableName) {
		return nil, fmt.Errorf("invalid sqlite table identifier %q", tableName)
	}
	rows, err := db.QueryContext(ctx, `PRAGMA index_list(`+tableName+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]sqliteIndexMetadata)
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		meta := sqliteIndexMetadata{
			Name:    name,
			Unique:  unique != 0,
			Partial: partial != 0,
		}
		columns, err := sqliteIndexColumns(ctx, db, name)
		if err != nil {
			return nil, err
		}
		meta.Columns = columns
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(sql, '') FROM sqlite_master WHERE type = 'index' AND name = ?`, name).Scan(&meta.SQL); err != nil {
			return nil, err
		}
		meta.SQL = strings.TrimSpace(meta.SQL)
		out[name] = meta
	}
	return out, rows.Err()
}

func sqliteIndexColumns(ctx context.Context, db *sql.DB, indexName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA index_info(`+sqlitePragmaString(indexName)+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var seqno int
		var cid int
		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		name = strings.TrimSpace(name)
		if name != "" {
			columns = append(columns, name)
		}
	}
	return columns, rows.Err()
}

func projectPatchQueueLiveBranchIndexReady(meta sqliteIndexMetadata) bool {
	if strings.TrimSpace(meta.Name) == "" || !meta.Unique || !meta.Partial {
		return false
	}
	if !stringSliceEqual(meta.Columns, []string{"workspace_id", "branch_id"}) {
		return false
	}
	switch sqliteCanonicalWherePredicate(meta.SQL) {
	case "STATEIN('PROPOSED','CLAIMED')", "STATEIN('CLAIMED','PROPOSED')":
		return true
	default:
		return false
	}
}

func projectPatchQueueClaimIndexReady(meta sqliteIndexMetadata) bool {
	return strings.TrimSpace(meta.Name) != "" &&
		!meta.Unique &&
		stringSliceEqual(meta.Columns, []string{"workspace_id", "project_id", "state", "claimed_by", "claim_expires_at"})
}

func projectPatchQueueMaterializationIndexReady(meta sqliteIndexMetadata) bool {
	if strings.TrimSpace(meta.Name) == "" || meta.Unique || !meta.Partial {
		return false
	}
	if !stringSliceEqual(meta.Columns, []string{"workspace_id", "project_id", "materialization_digest"}) {
		return false
	}
	return sqliteCanonicalWherePredicate(meta.SQL) == "MATERIALIZATION_ACCEPTED=1"
}

func projectPatchQueueSupersessionIndexReady(meta sqliteIndexMetadata) bool {
	if strings.TrimSpace(meta.Name) == "" || meta.Unique || !meta.Partial {
		return false
	}
	if !stringSliceEqual(meta.Columns, []string{"workspace_id", "project_id", "supersedes_queue_id", "supersedes_item_id"}) {
		return false
	}
	return sqliteCanonicalWherePredicate(meta.SQL) == "SUPERSEDES_ITEM_ID<>''"
}

func projectPatchQueueReviewTaskReceiptProjectionReady(ctx context.Context, db *sql.DB) (bool, error) {
	tasks, err := sqliteTableColumns(ctx, db, "tasks")
	if err != nil {
		return false, err
	}
	workspaceTasks, err := sqliteTableColumns(ctx, db, "workspace_tasks")
	if err != nil {
		return false, err
	}
	runtimeEvents, err := sqliteTableColumns(ctx, db, "runtime_events")
	if err != nil {
		return false, err
	}
	for _, column := range []string{"task_id", "status", "project_id", "project_lane", "task_requirements_json"} {
		if _, ok := tasks[column]; !ok {
			return false, nil
		}
	}
	for _, column := range []string{"workspace_id", "task_id"} {
		if _, ok := workspaceTasks[column]; !ok {
			return false, nil
		}
	}
	for _, column := range []string{"event_id", "workspace_id", "event_type", "entity_type", "entity_id", "created_at"} {
		if _, ok := runtimeEvents[column]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func projectPatchQueueDecisionContinuationOutboxReady(ctx context.Context, db *sql.DB) (bool, error) {
	columnMeta, err := sqliteTableColumnMetadataMap(ctx, db, "project_patch_queue_decision_continuation_outbox")
	if err != nil {
		return false, err
	}
	for _, column := range []string{
		"outbox_id",
		"workspace_id",
		"project_id",
		"queue_id",
		"item_id",
		"decision",
		"followup_kind",
		"continuation_task_id",
		"state",
		"decision_event_id",
		"payload_json",
		"created_at",
		"updated_at",
	} {
		if _, ok := columnMeta[column]; !ok {
			return false, nil
		}
	}
	if columnMeta["outbox_id"].PK == 0 {
		return false, nil
	}
	indexes, err := sqliteIndexMetadataByName(ctx, db, "project_patch_queue_decision_continuation_outbox")
	if err != nil {
		return false, err
	}
	uniqueItemDecision := false
	for _, meta := range indexes {
		if meta.Unique && stringSliceEqual(meta.Columns, []string{"workspace_id", "project_id", "queue_id", "item_id", "decision"}) {
			uniqueItemDecision = true
			break
		}
	}
	if !uniqueItemDecision {
		return false, nil
	}
	pending := indexes["idx_project_patch_queue_decision_continuation_pending"]
	if strings.TrimSpace(pending.Name) == "" || pending.Unique || !stringSliceEqual(pending.Columns, []string{"workspace_id", "project_id", "state", "followup_kind", "updated_at"}) {
		return false, nil
	}
	item := indexes["idx_project_patch_queue_decision_continuation_item"]
	if strings.TrimSpace(item.Name) == "" || item.Unique || !stringSliceEqual(item.Columns, []string{"workspace_id", "project_id", "queue_id", "item_id"}) {
		return false, nil
	}
	return true, nil
}

func projectPatchQueueDecisionContinuationMissingCount(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_patch_queue_items AS p
 WHERE p.state IN ('ACCEPTED', 'REJECTED', 'BLOCKED', 'CANCELED')
   AND NOT EXISTS (
     SELECT 1
       FROM project_patch_queue_decision_continuation_outbox AS o
      WHERE o.workspace_id = p.workspace_id
        AND o.project_id = p.project_id
        AND o.queue_id = p.queue_id
        AND o.item_id = p.item_id
        AND o.decision = p.state
   )`).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func sqliteTableColumnMetadataMap(ctx context.Context, db *sql.DB, tableName string) (map[string]projectPatchQueueColumnMetadata, error) {
	if !sqliteBareIdentifier(tableName) {
		return nil, fmt.Errorf("invalid sqlite table identifier %q", tableName)
	}
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+tableName+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]projectPatchQueueColumnMetadata)
	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out[name] = projectPatchQueueColumnMetadata{Name: name, PK: pk}
	}
	return out, rows.Err()
}

func sqliteTableColumns(ctx context.Context, db *sql.DB, tableName string) (map[string]struct{}, error) {
	if !sqliteBareIdentifier(tableName) {
		return nil, fmt.Errorf("invalid sqlite table identifier %q", tableName)
	}
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+tableName+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		name = strings.TrimSpace(name)
		if name != "" {
			out[name] = struct{}{}
		}
	}
	return out, rows.Err()
}

func sqliteBareIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, ch := range value {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '_' {
			return false
		}
	}
	return true
}

func sqlitePragmaString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sqliteCanonicalWherePredicate(sqlText string) string {
	normalized := strings.ToUpper(strings.Join(strings.Fields(sqlText), " "))
	index := strings.Index(normalized, " WHERE ")
	if index < 0 {
		return ""
	}
	predicate := strings.TrimSpace(normalized[index+len(" WHERE "):])
	predicate = strings.TrimSuffix(predicate, ";")
	predicate = strings.ReplaceAll(predicate, " ", "")
	return predicate
}

func stringSliceEqual(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func finalizeProjectPatchQueueDurabilityProof(proof ProjectPatchQueueDurabilityProof) ProjectPatchQueueDurabilityProof {
	proof.Digest = digestProjectPatchQueueDurabilityProof(proof)
	return proof
}

func digestProjectPatchQueueDurabilityProof(proof ProjectPatchQueueDurabilityProof) string {
	proof.Digest = ""
	raw, err := marshalProjectPatchQueueDurabilityProof(proof)
	if err != nil {
		return ""
	}
	return contentSHA256(raw)
}

func marshalProjectPatchQueueDurabilityProof(proof ProjectPatchQueueDurabilityProof) (string, error) {
	raw, err := jsonMarshalStable(proof)
	if err != nil {
		return "", fmt.Errorf("marshal project patch queue durability proof: %w", err)
	}
	return raw, nil
}

func jsonMarshalStable(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func isProjectPatchQueueDurabilityDigest(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 64 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}
