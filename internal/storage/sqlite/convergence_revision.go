package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
)

// R68 KEYSTONE (operator-signed loosening; finalization fixed point at the patch-queue level).
//
// taskIsRedundantRejectedRevisionTx is the server (authority) half of the keystone fix: a project task
// that is a patch-queue REVISION followup for a REJECTED candidate whose EVERY write-scope path is already
// covered by an INTEGRATED sibling artifact is redundant over-production - NOT spec-required - and must NOT
// hold the project's DONE frontier open. The product is spec-complete via the integrated set; the rejected
// candidate is a duplicate beyond the operator spec, and revising it covers zero uncovered AC.
//
// SCOPE SOURCE = the item's own server-owned PATHSET (project_patch_queue_items.pathset_json), recorded at
// submit time, NOT the branch registry. The agent half reads the identical field from the (unfiltered)
// coordination patch-queue items, so both halves see the same scope even after the integrated siblings'
// branches are MERGED/ARCHIVED out of the status-filtered coordination branch list.
//
// SAFETY MODEL (read before signing). This override only SKIPS a task at the frontier check; it never
// admits DONE. For a SPEC-BEARING project the external-spec coverage gate (evaluateProjectExternalSpecCoverageTx)
// runs unconditionally after the frontier loop and re-validates EVERY required AC against integrated state;
// that floor - not this predicate - is the no-false-DONE guarantee. The override is therefore gated by the
// CALLER to spec-bearing projects only (see evaluateProjectPhaseTerminalAdmissionTx): a spec-less project
// has no coverage floor, so it stays on the pre-R68 frontier-only rule and the loosening never applies.
//
// FORGE ACCOUNTING (honest). The revision identity kind='revision' and item_id are derived from agent
// task_submit input, so an agent CAN stamp a normal task as a "revision" of an arbitrary already-rejected
// covered item. We evaluate the candidate scope against that ITEM's server-owned pathset (not the task's
// real scope), so the override is forgeable on its own; the spec-bearing-only gate + the coverage floor
// are what bound this to "no false DONE". Treat the override as a finalization-LIVENESS lever with
// defense-in-depth value.
//
// TRIPWIRE (anti-self-lowering; errs toward BLOCKING). Returns false (KEEP blocking) unless ALL hold:
//   - the task carries a durable patch-queue identity of kind "revision" with a non-empty item_id,
//   - the referenced item's durable state is REJECTED,
//   - EVERY pathset path of that item is DIRECTIONALLY contained by an INTEGRATED sibling's pathset
//     (candidate path == or strictly under an integrated path; NOT mere symmetric overlap).
//
// Any unestablished input returns false; query errors propagate so the caller fails closed. Mirror of
// agent's idleReflectionTaskIsRedundantRejectedRevision (path math r68ScopePaths/r68NormScopePath/
// r68PathCoveredByIntegrated kept identical; symmetry policed by paired tests).
func (s *Store) taskIsRedundantRejectedRevisionTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string, task WorkspaceTaskRecord) (bool, error) {
	taskID := strings.TrimSpace(task.TaskID)
	projectID = strings.TrimSpace(projectID)
	if tx == nil || taskID == "" || projectID == "" {
		return false, nil
	}

	// 1. The task must carry a durable patch-queue identity of kind "revision" with an item pointer.
	var kind, itemID string
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(kind, ''), COALESCE(item_id, '')
  FROM task_patch_queue_identities
 WHERE workspace_id = ? AND task_id = ?`, workspaceID, taskID).Scan(&kind, &itemID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(strings.TrimSpace(kind), "revision") || strings.TrimSpace(itemID) == "" {
		return false, nil
	}

	// 2. The referenced item must be REJECTED (server-owned), and we take the candidate scope from the
	// item's own server-owned pathset.
	var itemState, itemPathsetJSON string
	err = tx.QueryRowContext(ctx, `
SELECT COALESCE(state, ''), COALESCE(pathset_json, '')
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND item_id = ?`, workspaceID, strings.TrimSpace(itemID)).Scan(&itemState, &itemPathsetJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(strings.TrimSpace(itemState), "REJECTED") {
		return false, nil
	}

	// 3. EVERY candidate pathset path must be DIRECTIONALLY covered by an INTEGRATED sibling's pathset.
	candidatePaths := r68ScopePaths(itemPathsetJSON)
	if len(candidatePaths) == 0 {
		return false, nil
	}
	integratedPaths, err := s.integratedSiblingPathsetPathsTx(ctx, tx, workspaceID, projectID, strings.TrimSpace(itemID))
	if err != nil {
		return false, err
	}
	if len(integratedPaths) == 0 {
		return false, nil
	}
	for _, p := range candidatePaths {
		if !r68PathCoveredByIntegrated(p, integratedPaths) {
			return false, nil
		}
	}
	return true, nil
}

// integratedSiblingPathsetPathsTx returns the union of strict-parsed pathset paths of every INTEGRATED
// patch-queue item in the project, excluding excludeItemID, from the open tx.
func (s *Store) integratedSiblingPathsetPathsTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, excludeItemID string) ([]string, error) {
	if tx == nil {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT COALESCE(pathset_json, '')
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND project_id = ?
   AND UPPER(TRIM(state)) = 'INTEGRATED'
   AND item_id <> ?`, workspaceID, strings.TrimSpace(projectID), strings.TrimSpace(excludeItemID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var pathset string
		if err := rows.Scan(&pathset); err != nil {
			return nil, err
		}
		paths = append(paths, r68ScopePaths(pathset)...)
	}
	return paths, rows.Err()
}

// r68ScopePaths strictly parses a write-scope into its trimmed path strings, accepting EITHER a
// {"paths":[...]} object (branch write_scope_json shape) OR a bare ["..."] array (patch-queue
// pathset_json shape). Returns nil on empty or malformed input (NOT a match-everything sentinel), so an
// unparseable scope fails closed (keep blocking) on BOTH halves. KEEP IDENTICAL with agent.
func r68ScopePaths(scopeJSON string) []string {
	scopeJSON = strings.TrimSpace(scopeJSON)
	if scopeJSON == "" {
		return nil
	}
	var wrapped struct {
		Paths []string `json:"paths"`
	}
	if err := json.Unmarshal([]byte(scopeJSON), &wrapped); err == nil {
		return r68TrimPaths(wrapped.Paths)
	}
	var bare []string
	if err := json.Unmarshal([]byte(scopeJSON), &bare); err == nil {
		return r68TrimPaths(bare)
	}
	return nil
}

// r68TrimPaths trims and drops empty entries. KEEP IDENTICAL with agent.
func r68TrimPaths(in []string) []string {
	out := []string{}
	for _, p := range in {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// r68NormScopePath normalizes a write-scope path for DIRECTIONAL containment: lowercase, backslashes to
// slashes, strip a trailing /** or /* glob and surrounding slashes. A global scope (".", "*", "**", or
// empty) normalizes to "" and is treated as non-covering (err toward blocking). KEEP IDENTICAL with
// agent.
func r68NormScopePath(p string) string {
	p = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(p, "\\", "/")))
	p = strings.TrimSuffix(p, "/**")
	p = strings.TrimSuffix(p, "/*")
	p = strings.Trim(p, "/")
	if p == "." || p == "*" || p == "**" {
		return ""
	}
	return p
}

// r68PathCoveredByIntegrated reports whether candidate path c is covered by some integrated path i, where
// "covered" is DIRECTIONAL containment: c == i OR c is strictly UNDER i (HasPrefix(c, i+"/")). A broad
// candidate directory is NOT covered by a narrower integrated file beneath it. A global/empty integrated
// scope never covers; a global/empty candidate path is never covered. KEEP IDENTICAL with agent.
func r68PathCoveredByIntegrated(candidate string, integratedPaths []string) bool {
	c := r68NormScopePath(candidate)
	if c == "" {
		return false
	}
	for _, raw := range integratedPaths {
		i := r68NormScopePath(raw)
		if i == "" {
			continue
		}
		if c == i || strings.HasPrefix(c, i+"/") {
			return true
		}
	}
	return false
}
