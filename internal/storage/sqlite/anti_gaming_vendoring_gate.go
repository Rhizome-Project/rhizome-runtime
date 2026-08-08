package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// NO-VENDORING accept-gate wiring. The verdict is computed BEFORE the accept write-transaction
// (the go.mod ground-truth read execs git and must never run under the SQLite write lock) and
// enforced INSIDE the transaction next to the visual / source-fidelity gates. Scoping is
// opt-in per project via a config doc - mirroring source-fidelity's source_refs opt-in - so
// non-interpreter projects are unaffected and only campaigns that declare the rule are gated.

const projectNoVendoringConfigDocSuffix = ".no_vendoring_interpreter"

// projectNoVendoringConfigDocKey is the opt-in doc whose presence makes NO-VENDORING required
// for a project (e.g. `project.<id>.no_vendoring_interpreter`).
func projectNoVendoringConfigDocKey(projectID string) string {
	projectID = sanitizeProjectDocKeySegment(projectID)
	if projectID == "" {
		return ""
	}
	return "project." + projectID + projectNoVendoringConfigDocSuffix
}

type interpreterVendoringVerdict struct {
	Required   bool
	HeadSHA    string
	Violations []string
}

// computeInterpreterVendoringVerdict loads the candidate item + repo + scoping config in a
// read-only transaction, then (only if scoped-required) reads go.mod at the candidate head
// from the managed-remote and returns the verdict. Call this BEFORE opening the accept
// write-transaction. Fail-closed: when required, an unresolved head or an unreadable
// ground-truth go.mod yields a non-empty Violations slice.
func (s *Store) computeInterpreterVendoringVerdict(ctx context.Context, workspaceID, projectID, queueID, itemID string) (interpreterVendoringVerdict, error) {
	var (
		required  bool
		headSHA   string
		remoteURL string
		loadErr   error
	)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return interpreterVendoringVerdict{}, err
	}
	func() {
		defer func() { _ = tx.Rollback() }()
		cfgKey := projectNoVendoringConfigDocKey(projectID)
		if strings.TrimSpace(cfgKey) == "" {
			return
		}
		if _, derr := s.loadWorkspaceDocTx(ctx, tx, workspaceID, cfgKey); derr != nil {
			if errors.Is(derr, sql.ErrNoRows) {
				return // opt-in doc absent -> not required (non-interpreter project)
			}
			loadErr = derr
			return
		}
		// The config doc exists. Once a project opts in, the gate stays required even if the
		// doc is ARCHIVED - archival must not be a bypass that disables NO-VENDORING mid-campaign.
		required = true
		item, ok, ierr := getProjectPatchQueueItemTx(ctx, tx, queueID, itemID)
		if ierr != nil {
			loadErr = ierr
			return
		}
		if !ok {
			return
		}
		headSHA = strings.TrimSpace(item.HeadSHA)
		repo, rerr := getProjectRepositoryTx(ctx, tx, workspaceID, projectID, item.RepoID)
		if rerr == nil {
			remoteURL = strings.TrimSpace(repo.RemoteURL)
		}
	}()
	if loadErr != nil {
		return interpreterVendoringVerdict{}, loadErr
	}
	if !required {
		return interpreterVendoringVerdict{Required: false}, nil
	}
	if headSHA == "" {
		return interpreterVendoringVerdict{Required: true, Violations: []string{"no_vendoring_unproven: candidate head sha unresolved"}}, nil
	}
	return interpreterVendoringVerdict{
		Required:   true,
		HeadSHA:    headSHA,
		Violations: projectInterpreterVendoringVerdict(ctx, remoteURL, headSHA),
	}, nil
}
