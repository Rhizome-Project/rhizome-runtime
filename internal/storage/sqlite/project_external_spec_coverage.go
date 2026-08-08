package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/acspec"
)

// project_external_spec_coverage.go - Blocker 2 of the convergence cluster (R50+).
//
// evaluateProjectPhaseTerminalAdmissionTx (terminal_admission.go) admits project.phase ->
// DONE the instant no open non-reflection task remains. On its own that is the exact
// "easy work ran out" oracle the operator forbids: at an empty frontier the EMPTY PRODUCT
// FRONTIER PROTOCOL keeps minting discretionary polish, and a project could be declared
// DONE before its EXTERNAL operator spec is covered.
//
// evaluateProjectExternalSpecCoverageTx adds the SUFFICIENT condition. For a spec-bearing
// project (one with an operator-rooted source_refs doc) DONE is admissible only when EVERY
// acceptance-criteria ID enumerated from the operator-sealed source_requirements_trace is
// ATTESTED - in a typed DONE-evidence doc the strategic lead writes at convergence - against
// an EXISTING integrated product artifact.
//
// REQUIRED-SET (denominator) is EXTERNAL and never agent discretion at DONE time. It is the
// acceptance_criteria_refs list inside project.<id>.source_requirements_trace, a doc hard-gated
// for completeness/specificity at the integration path (validateProjectSourceFidelityTraceTx)
// causally PRIOR to convergence, and re-asserted here inside the DONE tx. It is NOT the per-task
// acceptance_criteria_mapping: that field has zero non-test producers and is EMPTY by
// construction, so feeding it into the required side would silently self-lower the bar to
// "nothing to check -> admit". This gate reads acceptance_criteria_mapping nowhere.
//
// HONEST LIMIT (named, semi-trusted - theme H5 attestation-anchored, not machine-proven): the
// gate structurally forces each required AC-ID to be attested AND to name an artifact that
// actually exists as INTEGRATED product state. The SEMANTIC claim "this artifact satisfies this
// criterion" rests on the lead's judgment, because no machine per-criterion coverage map exists.
// This is strictly stronger than the pre-existing frontier-only oracle and than no gate, and
// weaker than a full machine oracle (unbuildable today: the mapping is empty by construction).
// Optional future hardening (NOT this mission): an adversarial QA agent that re-verifies each
// attestation against its artifact before DONE.
//
// The projection is additive and does not replace the source specification.

const acceptanceCoverageSchema = "rhizome_acceptance_coverage_v1"

func projectAcceptanceCoverageDocKey(projectID string) string {
	projectID = sanitizeProjectDocKeySegment(projectID)
	if projectID == "" {
		return ""
	}
	return "project." + projectID + ".acceptance_coverage"
}

// acceptanceCoverageDenominatorFieldNames is the set of trace fields the required-set (denominator)
// is enumerated from. It is the UNION of:
//   - the acceptance-refs fields (operator-directed primary denominator), mirroring
//     sourceRequirementsTraceBlockComplete's acceptance-refs set (spec_fidelity.go:200); and
//   - the acceptance-critical-anchor fields (spec_fidelity.go:192) - the ANTI-SHRINK union. The
//     seal forces the trace to carry the anchors but never forces acceptance_criteria_refs to
//     enumerate every anchor, so a trace could list four anchors and a single ref and still pass
//     the seal. Taking the union with the anchor AC-IDs makes the bar = every AC-ID named in
//     EITHER field, which can only RAISE the bar, never lower it (undercount = silent
//     self-lowering, which the operator steer forbids).
//
// The AC-ID tokenizer then filters each field's values to canonical IDs, so the anchors' trailing
// prose (e.g. "- AC-LEX-01: lexer produces positioned tokens") contributes only its AC-ID.
var acceptanceCoverageDenominatorFieldNames = []string{
	"acceptance_criteria_refs",
	"acceptance criteria refs",
	"acceptance_criteria",
	"acceptance criteria",
	"mvp_acceptance",
	"mvp acceptance",
	"acceptance_critical_anchors",
	"acceptance critical anchors",
	"non_droppable_requirements",
	"non-droppable requirements",
	"required_product_identity",
	"required product identity",
}

// evaluateProjectExternalSpecCoverageTx is the external-spec convergence oracle composed into
// the P06 phase->DONE admission AFTER the frontier-clear check passes. Frontier-empty stays
// NECESSARY; coverage is the added SUFFICIENT condition.
func (s *Store) evaluateProjectExternalSpecCoverageTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (TerminalAdmission, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)

	fidelity, err := s.projectSourceFidelityContextTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		// source_refs exists but is unusable (archived / zero source_doc_keys). Fail closed: a
		// spec-bearing project must not reach DONE on a mangled spec context.
		return externalSpecReject(projectID, reasonExternalSpecContextBroken,
			fmt.Errorf("source-fidelity context is unusable for DONE coverage: %v", err)), nil
	}
	if !fidelity.Required {
		// Spec-less project (no operator source_refs). Preserve the pre-existing frontier-only
		// self-DONE behaviour (R30). The anti-self-lowering bite exists only where a spec exists.
		// Distinct reason code keeps the coverage-less Admit auditable.
		return TerminalAdmission{Decision: TerminalAdmit, ReasonCode: reasonExternalSpecInert}, nil
	}

	// Re-assert the seal inside the DONE tx: a trace deleted / archived / source-key-regressed
	// between IMPLEMENTATION and DONE fails completeness -> reject (anti late bar-lowering).
	if err := s.validateProjectSourceFidelityTraceTx(ctx, tx, workspaceID, fidelity); err != nil {
		return externalSpecReject(projectID, reasonExternalSpecTraceRegressed,
			fmt.Errorf("source_requirements_trace no longer satisfies the seal at DONE: %v", err)), nil
	}

	traceDoc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, fidelity.TraceDocKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return externalSpecReject(projectID, reasonExternalSpecTraceRegressed,
				fmt.Errorf("source_requirements_trace doc %s missing at DONE", fidelity.TraceDocKey)), nil
		}
		return TerminalAdmission{}, err
	}

	requiredACIDs := projectExternalSpecRequiredACIDs(traceDoc.Content)
	if len(requiredACIDs) == 0 {
		// Required trace but no canonical AC-ID refs to prove coverage against. Fail closed: an
		// empty required-set under Required==true must REJECT, never vacuously admit (the
		// vacuous-admit IS the forbidden "frontier-empty == DONE" oracle).
		return externalSpecReject(projectID, reasonExternalSpecNoCriteria,
			fmt.Errorf("source_requirements_trace declares no canonical acceptance_criteria_refs AC-IDs; cannot prove external-spec coverage for DONE")), nil
	}

	attestations, attDocKey, err := s.projectAcceptanceCoverageAttestationsTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return TerminalAdmission{}, err
	}

	integratedItems, err := listIntegratedProjectPatchQueueItemsWithQuerier(ctx, tx, workspaceID, projectID)
	if err != nil {
		return TerminalAdmission{}, err
	}
	integratedTokens := projectIntegratedArtifactTokens(integratedItems)

	covered := make([]string, 0, len(requiredACIDs))
	for _, acID := range requiredACIDs {
		refs, ok := attestations[acID]
		if !ok {
			return externalSpecReject(projectID, reasonExternalSpecUncovered,
				fmt.Errorf("required acceptance criterion %s has no entry in DONE-evidence doc %s; attest it against an integrated artifact or open one bounded task quoting it",
					acID, attDocKey)), nil
		}
		matched, ok := projectAcceptanceCoverageArtifactMatch(refs, integratedTokens)
		if !ok {
			return externalSpecReject(projectID, reasonExternalSpecArtifactMissing,
				fmt.Errorf("DONE-evidence doc %s attests %s but its artifact reference (%s) matches no INTEGRATED product artifact; reference an integrated queue_id/item_id, branch_id, head_sha, or task_id",
					attDocKey, acID, externalSpecRefsForError(refs))), nil
		}
		_ = matched
		covered = append(covered, acID)
	}

	return TerminalAdmission{
		Decision:   TerminalAdmit,
		ReasonCode: reasonExternalSpecCovered,
		ContractID: contractProjectAcceptanceCoverage,
		Payload: map[string]any{
			"external_spec_required_ac_ids":     requiredACIDs,
			"external_spec_covered_ac_ids":      covered,
			"acceptance_coverage_doc_key":       attDocKey,
			"source_requirements_trace_doc_key": fidelity.TraceDocKey,
			"integrated_artifact_count":         len(integratedItems),
		},
	}, nil
}

func externalSpecReject(projectID, reasonCode string, cause error) TerminalAdmission {
	return TerminalAdmission{
		Decision:   TerminalReject,
		ReasonCode: reasonCode,
		ContractID: contractProjectAcceptanceCoverage,
		Err: fmt.Errorf("%w: project %s cannot transition to DONE: %v",
			ErrTaskCompletionContract, strings.TrimSpace(projectID), cause),
	}
}

func externalSpecRefsForError(refs []string) string {
	if len(refs) == 0 {
		return "none"
	}
	return strings.Join(refs, ",")
}

// projectExternalSpecRequiredACIDs enumerates the authoritative required-set: the AC-IDs listed
// under acceptance_criteria_refs inside the sealed source_requirements_trace fenced block. The
// regex is ONLY a tokenizer of that already-authoritative list - the authority is the refs list,
// not a free grep over the whole doc (a free grep would pull AC-IDs from acceptance_critical_anchors
// or prose and risk over/under-counting the bar).
func projectExternalSpecRequiredACIDs(traceContent string) []string {
	blocks := sourceFidelityFencedBlocks(traceContent, sourceRequirementsTraceSchema)
	if inline, ok := sourceFidelityInlineSchemaBlock(traceContent, sourceRequirementsTraceSchema); ok {
		blocks = append(blocks, inline)
	}
	if len(blocks) == 0 {
		// The DONE-tx seal re-assertion guarantees a complete block exists before we get here;
		// this whole-content fallback is purely defensive.
		blocks = []string{traceContent}
	}
	var raw []string
	for _, block := range blocks {
		raw = append(raw, sourceFidelityFieldValues(block, acceptanceCoverageDenominatorFieldNames)...)
	}
	return acspec.Normalize(raw)
}

// projectAcceptanceCoverageAttestationsTx reads the lead's typed DONE-evidence doc and returns a
// map AC-ID -> attested artifact-reference tokens. A missing or archived doc yields no
// attestations (every required AC-ID then rejects as uncovered).
func (s *Store) projectAcceptanceCoverageAttestationsTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (map[string][]string, string, error) {
	docKey := projectAcceptanceCoverageDocKey(projectID)
	if docKey == "" {
		return map[string][]string{}, docKey, nil
	}
	doc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, docKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string][]string{}, docKey, nil
		}
		return nil, docKey, err
	}
	if doc.ArchivedAt != nil {
		return map[string][]string{}, docKey, nil
	}
	return parseAcceptanceCoverageAttestations(doc.Content), docKey, nil
}

// parseAcceptanceCoverageAttestations parses the rhizome_acceptance_coverage_v1 fenced block into
// AC-ID -> artifact-ref tokens. Each list entry that carries an AC-ID attests that criterion; the
// non-AC-ID identifier tokens in that entry are its candidate artifact references.
func parseAcceptanceCoverageAttestations(content string) map[string][]string {
	out := map[string][]string{}
	blocks := sourceFidelityFencedBlocks(content, acceptanceCoverageSchema)
	if inline, ok := sourceFidelityInlineSchemaBlock(content, acceptanceCoverageSchema); ok {
		blocks = append(blocks, inline)
	}
	for _, block := range blocks {
		for _, record := range acceptanceCoverageRecords(block) {
			ids, refs := acceptanceCoverageEntryIDsAndRefs(record)
			for _, id := range ids {
				out[id] = append(out[id], refs...)
			}
		}
	}
	return out
}

// acceptanceCoverageEntryIDsAndRefs parses one attestation record into its attesting AC-ID(s) and
// its candidate artifact-reference tokens. The instructed (and produced-by-protocol) format is
// column-delimited:
//
//   - <AC-ID> | <integrated artifact ref> | <one-line justification>
//
// In that form the AC-ID(s) come ONLY from the first column and the artifact refs ONLY from the
// second. This closes two cross-reviewed holes at once:
//   - ANTI self-lowering: a word in the free-text justification can no longer become a phantom
//     artifact ref that coincidentally matches a word-shaped branch_id.
//   - ANTI false-reject (livelock): an AC-ID-SHAPED artifact id in the ref column (e.g. a qa-/cr-/
//     fr-prefixed branch or queue id) is no longer mistaken for an AC-ID and dropped from the refs.
//
// A record without a column delimiter falls back to best-effort whole-record extraction; a
// false-reject there is self-correcting (the reject names the AC-ID and the lead re-publishes in
// the column form on the next reflection), never a permanent wedge.
func acceptanceCoverageEntryIDsAndRefs(record string) ([]string, []string) {
	body := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(record), "-"))
	if strings.Contains(body, "|") {
		cols := strings.SplitN(body, "|", 3)
		ids := acspec.FindIDs(cols[0])
		if len(ids) == 0 {
			return nil, nil
		}
		var refs []string
		if len(cols) >= 2 {
			refs = acceptanceCoverageIdentifierTokens(cols[1], nil)
		}
		return ids, refs
	}
	ids := acspec.FindIDs(record)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, acceptanceCoverageIdentifierTokens(record, ids)
}

// acceptanceCoverageRecords splits an attestation block into per-entry records. The instructed
// format is one markdown list item per AC-ID (the item line plus its indented continuation
// lines); when the lead wrote no list items we fall back to treating each non-empty line as a
// record so a flat one-line-per-AC-ID doc still parses.
func acceptanceCoverageRecords(block string) []string {
	lines := strings.Split(block, "\n")
	var records []string
	var current []string
	hasListItem := false
	flush := func() {
		if len(current) > 0 {
			records = append(records, strings.Join(current, "\n"))
			current = nil
		}
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "- ") || trimmed == "-" {
			hasListItem = true
			flush()
			current = []string{line}
			continue
		}
		if len(current) > 0 {
			current = append(current, line)
		}
	}
	flush()
	if hasListItem {
		return records
	}
	records = records[:0]
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		records = append(records, line)
	}
	return records
}

// acceptanceCoverageIdentifierTokens returns the lower-cased identifier tokens in a segment,
// dropping fragments shorter than 3 characters (label noise) and any token in excludeIDs. The
// column-form caller passes nil exclude (the AC-ID is already isolated to a different column);
// the fallback caller passes the record's AC-IDs so the attesting id does not pollute its own
// refs. Splitting keeps "queue_id/item_id" pair tokens intact while breaking "branch:value" /
// "head=sha" label-value forms apart.
func acceptanceCoverageIdentifierTokens(segment string, excludeIDs []string) []string {
	exclude := map[string]struct{}{}
	for _, id := range excludeIDs {
		exclude[strings.ToLower(id)] = struct{}{}
	}
	fields := strings.FieldsFunc(segment, func(r rune) bool {
		switch r {
		case ' ', '\t', '\n', '\r', ',', ';', ':', '"', '\'', '`', '(', ')', '[', ']', '{', '}', '|', '=':
			return true
		default:
			return false
		}
	})
	seen := map[string]struct{}{}
	var out []string
	for _, field := range fields {
		field = strings.Trim(strings.TrimSpace(field), ".")
		lower := strings.ToLower(field)
		if len(lower) < 3 {
			continue
		}
		if _, ok := exclude[lower]; ok {
			continue
		}
		if _, ok := seen[lower]; ok {
			continue
		}
		seen[lower] = struct{}{}
		out = append(out, lower)
	}
	return out
}

// projectIntegratedArtifactTokens builds the set of strong identifier tokens that an attestation
// artifact reference may match: each INTEGRATED patch-queue item's queue_id, item_id,
// queue_id/item_id pair key, branch_id, head_sha (and its 12-char prefix), and source task_id.
// repo_id is deliberately excluded (it can be a generic word and would weaken the match).
func projectIntegratedArtifactTokens(items []ProjectPatchQueueItemRecord) map[string]struct{} {
	tokens := map[string]struct{}{}
	add := func(value string) {
		value = strings.ToLower(strings.TrimSpace(value))
		if len(value) >= 3 {
			tokens[value] = struct{}{}
		}
	}
	for _, item := range items {
		add(item.QueueID)
		add(item.ItemID)
		add(agentWorkPatchQueueItemPairKey(item.QueueID, item.ItemID))
		add(item.BranchID)
		add(item.HeadSHA)
		if head := strings.TrimSpace(item.HeadSHA); len(head) >= 12 {
			add(head[:12])
		}
		add(item.TaskID)
	}
	return tokens
}

func projectAcceptanceCoverageArtifactMatch(refs []string, integrated map[string]struct{}) (string, bool) {
	for _, ref := range refs {
		if _, ok := integrated[ref]; ok {
			return ref, true
		}
	}
	return "", false
}
