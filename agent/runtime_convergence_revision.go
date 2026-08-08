package main

import (
	"context"
	"encoding/json"
	"strings"
)

// R68 KEYSTONE (operator-signed loosening; finalization fixed point at the patch-queue level).
//
// Agent (convergence-trigger) half, mirroring the server authority half
// internal/storage/sqlite/convergence_revision.go (taskIsRedundantRejectedRevisionTx). A patch-queue
// REVISION followup that targets a REJECTED candidate whose EVERY pathset path is already covered by an
// INTEGRATED sibling artifact is redundant over-production - NOT spec-required - and must NOT keep the
// convergence frontier open. With it removed, the strategic lead recognises the spec-complete product,
// writes project.<id>.acceptance_coverage, and converges; the server gate (same predicate + the untouched
// external-spec coverage floor, gated to spec-bearing projects) then admits DONE.
//
// SCOPE SOURCE = the item's own PATHSET (ProjectPatchQueueItemRecord.PathsetJSON), read from the
// coordination patch-queue items, which are NOT status-filtered. This is the mirror of the server's
// pathset_json source and is robust to the integrated siblings' branches being MERGED/ARCHIVED out of the
// status-filtered coordination branch list. The path math (r68ScopePaths / r68NormScopePath /
// r68PathCoveredByIntegrated) is kept IDENTICAL with the server; equivalence is policed by paired tests.
// The shared convergence-blocking primitive (convergence_blocking.go) is untouched. As on the server,
// this is a finalization-LIVENESS lever with defense-in-depth value; the coverage floor is the
// load-bearing no-false-DONE guarantee.

// idleReflectionProjectConvergenceFrontierIsClearWithPatchQueue is the patch-queue-aware frontier-clear
// predicate. A blocking task is treated as non-blocking ONLY when it is a redundant rejected-revision
// over-production task. With nil items it is exactly idleReflectionProjectConvergenceFrontierIsClear (the
// override never fires - strict, the safe default for callers without patch-queue context).
func idleReflectionProjectConvergenceFrontierIsClearWithPatchQueue(openTasks []WorkspaceTaskRecord, items []ProjectPatchQueueItemRecord, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	for _, task := range openTasks {
		if strings.TrimSpace(task.ProjectID) != projectID {
			continue
		}
		if !taskIsConvergenceBlocking(task) {
			continue
		}
		if idleReflectionTaskIsRedundantRejectedRevision(task, items) {
			continue
		}
		return false
	}
	return true
}

// idleReflectionFrontierBlockedOnlyByRevisionFollowups is a cheap snapshot-level pre-check: the
// convergence frontier is NOT clear, yet EVERY convergence-blocking task is a patch-queue revision
// followup. It uses only the idle-reflection snapshot (no patch-queue context), so it cannot tell whether
// those revisions are redundant - that needs the coordination read. Its sole purpose is to gate that read:
// only when the frontier is blocked solely by revision-shaped work is it worth fetching coordination.
// Returns false when any blocking task is NOT a revision followup, or when there is no blocking task.
func idleReflectionFrontierBlockedOnlyByRevisionFollowups(openTasks []WorkspaceTaskRecord, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	sawBlocking := false
	for _, task := range openTasks {
		if strings.TrimSpace(task.ProjectID) != projectID {
			continue
		}
		if !taskIsConvergenceBlocking(task) {
			continue
		}
		if !delegatedAgentTaskIsPatchQueueRevisionFollowup(task) {
			return false
		}
		sawBlocking = true
	}
	return sawBlocking
}

// idleReflectionTaskIsRedundantRejectedRevision reports whether an otherwise convergence-blocking task is
// a redundant rejected-revision over-production task. TRIPWIRE (errs toward BLOCKING; returns false unless
// ALL hold):
//   - it is a patch-queue revision followup with resolvable queue/item/branch refs,
//   - the referenced item's state is REJECTED (server-owned durable state),
//   - the referenced item's EVERY pathset path is DIRECTIONALLY covered by an INTEGRATED sibling.
func idleReflectionTaskIsRedundantRejectedRevision(task WorkspaceTaskRecord, items []ProjectPatchQueueItemRecord) bool {
	if len(items) == 0 {
		return false
	}
	if !delegatedAgentTaskIsPatchQueueRevisionFollowup(task) {
		return false
	}
	refs := idleReflectionPatchQueueTaskRefsFromTask(task)
	item, ok := idleReflectionPatchQueueReferencedItem(items, refs)
	if !ok {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(item.State), "REJECTED") {
		return false
	}
	return patchQueueCandidateLaneFullyIntegrated(item, items)
}

// patchQueueCandidateLaneFullyIntegrated reports whether EVERY pathset path of the candidate item is
// DIRECTIONALLY covered by some OTHER INTEGRATED item's pathset. Empty/unknown candidate scope or zero
// integrated siblings -> false (cannot establish redundancy; keep blocking).
func patchQueueCandidateLaneFullyIntegrated(candidate ProjectPatchQueueItemRecord, items []ProjectPatchQueueItemRecord) bool {
	candidatePaths := r68ItemScopePaths(candidate)
	if len(candidatePaths) == 0 {
		return false
	}
	integratedPaths := []string{}
	for _, sibling := range items {
		if strings.TrimSpace(sibling.ItemID) == strings.TrimSpace(candidate.ItemID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(sibling.State), "INTEGRATED") {
			continue
		}
		integratedPaths = append(integratedPaths, r68ItemScopePaths(sibling)...)
	}
	if len(integratedPaths) == 0 {
		return false
	}
	for _, p := range candidatePaths {
		if !r68PathCoveredByIntegrated(p, integratedPaths) {
			return false
		}
	}
	return true
}

// r68ItemScopePaths resolves a patch-queue item's own scope from its server-recorded pathset (PathsetJSON
// raw, else the parsed Pathset slice). Independent of the branch registry / coordination branch status.
func r68ItemScopePaths(item ProjectPatchQueueItemRecord) []string {
	if paths := r68ScopePaths(item.PathsetJSON); len(paths) > 0 {
		return paths
	}
	return r68TrimPaths(item.Pathset)
}

// projectFrontierClearIgnoringRedundantRejectedRevisions fetches the project coordination (which carries
// the unfiltered patch-queue items, absent from the idle-reflection WorkspaceSnapshot) and reports whether
// the convergence frontier is clear once redundant rejected-revision over-production is discounted. Used
// to upgrade the lead's convergence trigger. Errs toward false on any error or empty frontier.
func (r *Runtime) projectFrontierClearIgnoringRedundantRejectedRevisions(ctx context.Context, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if r == nil || r.client == nil || projectID == "" {
		return false
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
	if err != nil {
		return false
	}
	if len(coordination.Tasks) == 0 {
		return false
	}
	return idleReflectionProjectConvergenceFrontierIsClearWithPatchQueue(coordination.Tasks, coordination.PatchQueueItems, projectID)
}

// r68ScopePaths strictly parses a write-scope into its trimmed path strings, accepting EITHER a
// {"paths":[...]} object (branch write_scope_json shape) OR a bare ["..."] array (patch-queue
// pathset_json shape). Returns nil on empty or malformed input (NOT a match-everything sentinel), so an
// unparseable scope fails closed (keep blocking) on BOTH halves. KEEP IDENTICAL with the server
// (internal/storage/sqlite/convergence_revision.go).
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

// r68TrimPaths trims and drops empty entries. KEEP IDENTICAL with the server.
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
// empty) normalizes to "" and is treated as non-covering (err toward blocking). KEEP IDENTICAL with the
// server.
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
// scope never covers; a global/empty candidate path is never covered. KEEP IDENTICAL with the server.
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
