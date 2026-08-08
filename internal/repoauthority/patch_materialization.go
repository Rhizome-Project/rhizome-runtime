package repoauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	PatchMaterializationSchemaVersion = "repo_patch_materialization.v1"
	PatchMaterializationEncodingUTF8  = "utf-8"

	PatchMaterializationMaxFiles      = 128
	PatchMaterializationMaxFileBytes  = int64(1 << 20)
	PatchMaterializationMaxTotalBytes = int64(4 << 20)

	PatchMaterializationAuthorityProofSchemaVersion = "repo_patch_materialization_authority_proof.v1"
	PatchMaterializationAuthorityProofSourceSQLite  = "sqlite.project_patch_queue.materialization_record"
)

type PatchMaterialization struct {
	Schema                string                  `json:"schema"`
	WorkspaceID           string                  `json:"workspace_id"`
	ProjectID             string                  `json:"project_id,omitempty"`
	QueueID               string                  `json:"queue_id"`
	ItemID                string                  `json:"item_id"`
	OperationID           string                  `json:"operation_id"`
	OperationKind         string                  `json:"operation_kind"`
	CASPatchDigest        string                  `json:"cas_patch_digest"`
	CASEvaluationDigest   string                  `json:"cas_evaluation_digest"`
	Files                 []PatchMaterializedFile `json:"files"`
	RecordedBy            string                  `json:"recorded_by,omitempty"`
	RecordedAt            string                  `json:"recorded_at,omitempty"`
	MaterializationDigest string                  `json:"materialization_digest,omitempty"`
}

type PatchMaterializedFile struct {
	Path            string `json:"path"`
	ChangeKind      string `json:"change_kind,omitempty"`
	BaseHash        string `json:"base_hash"`
	CandidateHash   string `json:"candidate_hash"`
	ContentEncoding string `json:"content_encoding"`
	Content         string `json:"content"`
	ContentDigest   string `json:"content_digest"`
}

type PatchMaterializationAuthorityProof struct {
	Schema                    string                                    `json:"schema"`
	Source                    string                                    `json:"source"`
	WorkspaceID               string                                    `json:"workspace_id"`
	ProjectID                 string                                    `json:"project_id,omitempty"`
	QueueID                   string                                    `json:"queue_id"`
	ItemID                    string                                    `json:"item_id"`
	OperationID               string                                    `json:"operation_id"`
	OperationKind             string                                    `json:"operation_kind"`
	CASPatchDigest            string                                    `json:"cas_patch_digest"`
	CASEvaluationDigest       string                                    `json:"cas_evaluation_digest"`
	MaterializationDigest     string                                    `json:"materialization_digest"`
	MaterializationJSONDigest string                                    `json:"materialization_json_digest"`
	FileCount                 int                                       `json:"file_count"`
	Files                     []PatchMaterializedFileAuthorityProofPath `json:"files,omitempty"`
	RecordedBy                string                                    `json:"recorded_by,omitempty"`
	RecordedAt                string                                    `json:"recorded_at,omitempty"`
	AuthorityDigest           string                                    `json:"authority_digest,omitempty"`
}

type PatchMaterializedFileAuthorityProofPath struct {
	Path            string `json:"path"`
	ChangeKind      string `json:"change_kind,omitempty"`
	BaseHash        string `json:"base_hash,omitempty"`
	CandidateHash   string `json:"candidate_hash,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	ContentDigest   string `json:"content_digest,omitempty"`
}

func NormalizePatchMaterialization(input PatchMaterialization, item PatchQueueItem, recordedAt time.Time) (PatchMaterialization, error) {
	if err := ValidatePatchMaterializationContentBounds(input); err != nil {
		return PatchMaterialization{}, err
	}
	recordedAt = normalizeLeaseTime(recordedAt)
	materialization := PatchMaterialization{
		Schema:              patchMaterializationFirstNonEmpty(strings.TrimSpace(input.Schema), PatchMaterializationSchemaVersion),
		WorkspaceID:         patchMaterializationFirstNonEmpty(strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(item.WorkspaceID)),
		ProjectID:           patchMaterializationFirstNonEmpty(strings.TrimSpace(input.ProjectID), strings.TrimSpace(item.ProjectID)),
		QueueID:             patchMaterializationFirstNonEmpty(strings.TrimSpace(input.QueueID), strings.TrimSpace(item.QueueID)),
		ItemID:              patchMaterializationFirstNonEmpty(strings.TrimSpace(input.ItemID), strings.TrimSpace(item.ItemID)),
		OperationID:         patchMaterializationFirstNonEmpty(strings.TrimSpace(input.OperationID), strings.TrimSpace(item.OperationID)),
		OperationKind:       patchMaterializationFirstNonEmpty(strings.TrimSpace(input.OperationKind), strings.TrimSpace(item.OperationKind)),
		CASPatchDigest:      patchMaterializationFirstNonEmpty(strings.TrimSpace(input.CASPatchDigest), strings.TrimSpace(item.CASPatchDigest)),
		CASEvaluationDigest: patchMaterializationFirstNonEmpty(strings.TrimSpace(input.CASEvaluationDigest), strings.TrimSpace(item.CASEvaluationDigest)),
		RecordedBy:          strings.TrimSpace(input.RecordedBy),
	}
	if strings.TrimSpace(input.RecordedAt) != "" {
		materialization.RecordedAt = strings.TrimSpace(input.RecordedAt)
	} else if !recordedAt.IsZero() {
		materialization.RecordedAt = formatLeaseTime(recordedAt)
	}

	casPaths := patchMaterializationCASPathMap(item.CASResult)
	materialization.Files = make([]PatchMaterializedFile, 0, len(input.Files))
	for _, rawFile := range input.Files {
		path, err := NormalizePath(rawFile.Path)
		if err != nil {
			return PatchMaterialization{}, fmt.Errorf("materialized file path: %w", err)
		}
		casPath := casPaths[path]
		content := rawFile.Content
		contentDigest := patchMaterializationContentDigest(content)
		if supplied := strings.TrimSpace(rawFile.ContentDigest); supplied != "" && supplied != contentDigest {
			return PatchMaterialization{}, fmt.Errorf("materialized file %s content_digest does not match content", path)
		}
		file := PatchMaterializedFile{
			Path:            path,
			ChangeKind:      patchMaterializationFirstNonEmpty(strings.TrimSpace(rawFile.ChangeKind), strings.TrimSpace(casPath.ChangeKind)),
			BaseHash:        patchMaterializationFirstNonEmpty(strings.TrimSpace(rawFile.BaseHash), strings.TrimSpace(casPath.BaseHash)),
			CandidateHash:   patchMaterializationFirstNonEmpty(strings.TrimSpace(rawFile.CandidateHash), strings.TrimSpace(casPath.CandidateHash)),
			ContentEncoding: patchMaterializationFirstNonEmpty(strings.TrimSpace(rawFile.ContentEncoding), PatchMaterializationEncodingUTF8),
			Content:         content,
			ContentDigest:   contentDigest,
		}
		materialization.Files = append(materialization.Files, file)
	}
	sort.Slice(materialization.Files, func(i, j int) bool {
		return materialization.Files[i].Path < materialization.Files[j].Path
	})
	materialization.MaterializationDigest = PatchMaterializationDigest(materialization)
	if err := ValidatePatchMaterialization(materialization, item); err != nil {
		return PatchMaterialization{}, err
	}
	return materialization, nil
}

func ValidatePatchMaterialization(materialization PatchMaterialization, item PatchQueueItem) error {
	if strings.TrimSpace(materialization.Schema) != PatchMaterializationSchemaVersion {
		return fmt.Errorf("patch materialization schema is unsupported")
	}
	if err := ValidatePatchMaterializationContentBounds(materialization); err != nil {
		return err
	}
	if strings.TrimSpace(item.Schema) != PatchQueueItemSchemaVersion {
		return fmt.Errorf("patch queue item schema is unsupported")
	}
	if strings.TrimSpace(item.State) != PatchQueueStateApplied {
		return fmt.Errorf("patch materialization requires applied patch queue item, got %q", item.State)
	}
	if item.CASResult.Status != CASPatchStatusApplied {
		return fmt.Errorf("patch materialization requires applied CAS result, got %q", item.CASResult.Status)
	}
	if item.OperationID == "" || item.OperationKind == "" {
		return fmt.Errorf("patch materialization requires operation binding")
	}
	if err := patchMaterializationMatch("workspace_id", materialization.WorkspaceID, item.WorkspaceID); err != nil {
		return err
	}
	if strings.TrimSpace(item.ProjectID) != "" {
		if err := patchMaterializationMatch("project_id", materialization.ProjectID, item.ProjectID); err != nil {
			return err
		}
	}
	if err := patchMaterializationMatch("queue_id", materialization.QueueID, item.QueueID); err != nil {
		return err
	}
	if err := patchMaterializationMatch("item_id", materialization.ItemID, item.ItemID); err != nil {
		return err
	}
	if err := patchMaterializationMatch("operation_id", materialization.OperationID, item.OperationID); err != nil {
		return err
	}
	if err := patchMaterializationMatch("operation_kind", materialization.OperationKind, item.OperationKind); err != nil {
		return err
	}
	if err := patchMaterializationMatch("cas_patch_digest", materialization.CASPatchDigest, item.CASPatchDigest); err != nil {
		return err
	}
	if err := patchMaterializationMatch("cas_evaluation_digest", materialization.CASEvaluationDigest, item.CASEvaluationDigest); err != nil {
		return err
	}
	if !isCanonicalSHA256Digest(materialization.MaterializationDigest) {
		return fmt.Errorf("patch materialization digest is required")
	}
	if materialization.MaterializationDigest != PatchMaterializationDigest(materialization) {
		return fmt.Errorf("patch materialization digest mismatch")
	}

	expectedPaths, casPaths, err := patchMaterializationAppliedCASPaths(item)
	if err != nil {
		return err
	}
	if len(materialization.Files) != len(expectedPaths) {
		return fmt.Errorf("patch materialization file count %d does not match applied CAS path count %d", len(materialization.Files), len(expectedPaths))
	}
	for i, file := range materialization.Files {
		path, err := NormalizePath(file.Path)
		if err != nil {
			return fmt.Errorf("materialized file[%d] path invalid: %w", i, err)
		}
		if file.Path != path {
			return fmt.Errorf("materialized file[%d] path is not normalized: got %q want %q", i, file.Path, path)
		}
		if i > 0 && materialization.Files[i-1].Path >= file.Path {
			return fmt.Errorf("materialized files must be sorted and unique")
		}
		if expectedPaths[i] != file.Path {
			return fmt.Errorf("materialized file[%d] path %q does not match applied CAS path %q", i, file.Path, expectedPaths[i])
		}
		casPath, ok := casPaths[file.Path]
		if !ok {
			return fmt.Errorf("materialized file %s is missing from CAS result", file.Path)
		}
		if casPath.Status != CASPatchStatusApplied {
			return fmt.Errorf("materialized file %s CAS status is %q", file.Path, casPath.Status)
		}
		changeKind, err := patchMaterializationChangeKind(file, casPath)
		if err != nil {
			return fmt.Errorf("materialized file %s: %w", file.Path, err)
		}
		if changeKind == CASPatchChangeAdd {
			if strings.TrimSpace(file.BaseHash) != "" || strings.TrimSpace(casPath.BaseHash) != "" {
				return fmt.Errorf("materialized file %s: added path must not carry base_hash", file.Path)
			}
		} else if err := patchMaterializationMatch("base_hash", file.BaseHash, casPath.BaseHash); err != nil {
			return fmt.Errorf("materialized file %s: %w", file.Path, err)
		}
		if err := patchMaterializationMatch("candidate_hash", file.CandidateHash, casPath.CandidateHash); err != nil {
			return fmt.Errorf("materialized file %s: %w", file.Path, err)
		}
		if strings.TrimSpace(file.ContentEncoding) != PatchMaterializationEncodingUTF8 {
			return fmt.Errorf("materialized file %s has unsupported content_encoding %q", file.Path, file.ContentEncoding)
		}
		contentDigest := patchMaterializationContentDigest(file.Content)
		if strings.TrimSpace(file.ContentDigest) != contentDigest {
			return fmt.Errorf("materialized file %s content_digest mismatch", file.Path)
		}
		if strings.TrimSpace(file.CandidateHash) != contentDigest {
			return fmt.Errorf("materialized file %s candidate_hash does not match content_digest", file.Path)
		}
	}
	return nil
}

func ValidatePatchMaterializationContentBounds(materialization PatchMaterialization) error {
	if len(materialization.Files) > PatchMaterializationMaxFiles {
		return fmt.Errorf("patch materialization content policy exceeded: file count %d exceeds limit %d", len(materialization.Files), PatchMaterializationMaxFiles)
	}
	var totalBytes int64
	for _, file := range materialization.Files {
		contentBytes := int64(len([]byte(file.Content)))
		if contentBytes > PatchMaterializationMaxFileBytes {
			return fmt.Errorf("patch materialization content policy exceeded: file %q size %d exceeds limit %d bytes", strings.TrimSpace(file.Path), contentBytes, PatchMaterializationMaxFileBytes)
		}
		totalBytes += contentBytes
		if totalBytes > PatchMaterializationMaxTotalBytes {
			return fmt.Errorf("patch materialization content policy exceeded: total size %d exceeds limit %d bytes", totalBytes, PatchMaterializationMaxTotalBytes)
		}
	}
	return nil
}

func PatchMaterializationDigest(materialization PatchMaterialization) string {
	materialization.MaterializationDigest = ""
	raw, _ := json.Marshal(materialization)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func PatchMaterializationJSONDigest(materialization PatchMaterialization) string {
	raw, _ := json.Marshal(materialization)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func BuildPatchMaterializationAuthorityProof(materialization PatchMaterialization, item PatchQueueItem) (PatchMaterializationAuthorityProof, error) {
	if err := ValidatePatchMaterialization(materialization, item); err != nil {
		return PatchMaterializationAuthorityProof{}, err
	}
	proof := PatchMaterializationAuthorityProof{
		Schema:                    PatchMaterializationAuthorityProofSchemaVersion,
		Source:                    PatchMaterializationAuthorityProofSourceSQLite,
		WorkspaceID:               strings.TrimSpace(materialization.WorkspaceID),
		ProjectID:                 strings.TrimSpace(materialization.ProjectID),
		QueueID:                   strings.TrimSpace(materialization.QueueID),
		ItemID:                    strings.TrimSpace(materialization.ItemID),
		OperationID:               strings.TrimSpace(materialization.OperationID),
		OperationKind:             strings.TrimSpace(materialization.OperationKind),
		CASPatchDigest:            strings.TrimSpace(materialization.CASPatchDigest),
		CASEvaluationDigest:       strings.TrimSpace(materialization.CASEvaluationDigest),
		MaterializationDigest:     strings.TrimSpace(materialization.MaterializationDigest),
		MaterializationJSONDigest: PatchMaterializationJSONDigest(materialization),
		FileCount:                 len(materialization.Files),
		RecordedBy:                strings.TrimSpace(materialization.RecordedBy),
		RecordedAt:                strings.TrimSpace(materialization.RecordedAt),
	}
	if len(materialization.Files) > 0 {
		proof.Files = make([]PatchMaterializedFileAuthorityProofPath, 0, len(materialization.Files))
		for _, file := range materialization.Files {
			proof.Files = append(proof.Files, PatchMaterializedFileAuthorityProofPath{
				Path:            strings.TrimSpace(file.Path),
				ChangeKind:      strings.TrimSpace(file.ChangeKind),
				BaseHash:        strings.TrimSpace(file.BaseHash),
				CandidateHash:   strings.TrimSpace(file.CandidateHash),
				ContentEncoding: strings.TrimSpace(file.ContentEncoding),
				ContentDigest:   strings.TrimSpace(file.ContentDigest),
			})
		}
	}
	proof.AuthorityDigest = PatchMaterializationAuthorityProofDigest(proof)
	if err := ValidatePatchMaterializationAuthorityProof(proof, materialization, item); err != nil {
		return PatchMaterializationAuthorityProof{}, err
	}
	return proof, nil
}

func ValidatePatchMaterializationAuthorityProof(proof PatchMaterializationAuthorityProof, materialization PatchMaterialization, item PatchQueueItem) error {
	if err := ValidatePatchMaterialization(materialization, item); err != nil {
		return err
	}
	if strings.TrimSpace(proof.Schema) != PatchMaterializationAuthorityProofSchemaVersion {
		return fmt.Errorf("patch materialization authority proof schema is unsupported")
	}
	if strings.TrimSpace(proof.Source) != PatchMaterializationAuthorityProofSourceSQLite {
		return fmt.Errorf("patch materialization authority proof source is unsupported")
	}
	expected := map[string][2]string{
		"workspace_id":                {proof.WorkspaceID, materialization.WorkspaceID},
		"project_id":                  {proof.ProjectID, materialization.ProjectID},
		"queue_id":                    {proof.QueueID, materialization.QueueID},
		"item_id":                     {proof.ItemID, materialization.ItemID},
		"operation_id":                {proof.OperationID, materialization.OperationID},
		"operation_kind":              {proof.OperationKind, materialization.OperationKind},
		"cas_patch_digest":            {proof.CASPatchDigest, materialization.CASPatchDigest},
		"cas_evaluation_digest":       {proof.CASEvaluationDigest, materialization.CASEvaluationDigest},
		"materialization_digest":      {proof.MaterializationDigest, materialization.MaterializationDigest},
		"materialization_json_digest": {proof.MaterializationJSONDigest, PatchMaterializationJSONDigest(materialization)},
		"recorded_by":                 {proof.RecordedBy, materialization.RecordedBy},
		"recorded_at":                 {proof.RecordedAt, materialization.RecordedAt},
	}
	for field, pair := range expected {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			return fmt.Errorf("patch materialization authority proof %s mismatch", field)
		}
	}
	if proof.FileCount != len(materialization.Files) || len(proof.Files) != len(materialization.Files) {
		return fmt.Errorf("patch materialization authority proof file count mismatch")
	}
	for i, file := range materialization.Files {
		got := proof.Files[i]
		expectedFile := map[string][2]string{
			"path":             {got.Path, file.Path},
			"change_kind":      {got.ChangeKind, file.ChangeKind},
			"base_hash":        {got.BaseHash, file.BaseHash},
			"candidate_hash":   {got.CandidateHash, file.CandidateHash},
			"content_encoding": {got.ContentEncoding, file.ContentEncoding},
			"content_digest":   {got.ContentDigest, file.ContentDigest},
		}
		for field, pair := range expectedFile {
			if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
				return fmt.Errorf("patch materialization authority proof file[%d] %s mismatch", i, field)
			}
		}
	}
	if !isCanonicalSHA256Digest(proof.MaterializationDigest) {
		return fmt.Errorf("patch materialization authority proof materialization digest is required")
	}
	if !isCanonicalSHA256Digest(proof.MaterializationJSONDigest) {
		return fmt.Errorf("patch materialization authority proof materialization JSON digest is required")
	}
	if !isCanonicalSHA256Digest(proof.AuthorityDigest) {
		return fmt.Errorf("patch materialization authority proof digest is required")
	}
	if proof.AuthorityDigest != PatchMaterializationAuthorityProofDigest(proof) {
		return fmt.Errorf("patch materialization authority proof digest mismatch")
	}
	return nil
}

func PatchMaterializationAuthorityProofDigest(proof PatchMaterializationAuthorityProof) string {
	proof.AuthorityDigest = ""
	raw, _ := json.Marshal(proof)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func PatchMaterializationContentDigest(content string) string {
	return patchMaterializationContentDigest(content)
}

func patchMaterializationContentDigest(content string) string {
	sum := sha256.Sum256([]byte(content))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func patchMaterializationCASPathMap(result CASPatchApplyResult) map[string]CASPatchPathResult {
	out := make(map[string]CASPatchPathResult, len(result.Paths))
	for _, pathResult := range result.Paths {
		path := strings.TrimSpace(pathResult.Path)
		if path == "" {
			continue
		}
		out[path] = pathResult
	}
	return out
}

func patchMaterializationAppliedCASPaths(item PatchQueueItem) ([]string, map[string]CASPatchPathResult, error) {
	pathset, err := NormalizePathSet(item.Pathset)
	if err != nil {
		return nil, nil, fmt.Errorf("patch materialization item pathset invalid: %w", err)
	}
	if len(pathset) == 0 {
		return nil, nil, fmt.Errorf("patch materialization requires non-empty pathset")
	}
	seen := make(map[string]CASPatchPathResult, len(item.CASResult.Paths))
	paths := make([]string, 0, len(item.CASResult.Paths))
	for i, pathResult := range item.CASResult.Paths {
		path, err := NormalizePath(pathResult.Path)
		if err != nil {
			return nil, nil, fmt.Errorf("patch materialization CAS path[%d] invalid: %w", i, err)
		}
		if pathResult.Path != path {
			return nil, nil, fmt.Errorf("patch materialization CAS path[%d] is not normalized: got %q want %q", i, pathResult.Path, path)
		}
		if pathResult.Status != CASPatchStatusApplied {
			return nil, nil, fmt.Errorf("patch materialization CAS path %s status is %q", path, pathResult.Status)
		}
		if !pathsetCoversPath(pathset, path) {
			return nil, nil, fmt.Errorf("patch materialization CAS path %s is outside patch queue pathset", path)
		}
		if _, ok := seen[path]; ok {
			return nil, nil, fmt.Errorf("patch materialization CAS path %s is duplicated", path)
		}
		seen[path] = pathResult
		paths = append(paths, path)
	}
	if len(paths) == 0 {
		return nil, nil, fmt.Errorf("patch materialization requires applied CAS paths")
	}
	sort.Strings(paths)
	for _, scope := range pathset {
		if pathsetEntryIsScoped(scope) {
			continue
		}
		if _, ok := seen[scope]; !ok {
			return nil, nil, fmt.Errorf("patch materialization applied CAS evidence missing concrete path %q from patch queue pathset", scope)
		}
	}
	return paths, seen, nil
}

func patchMaterializationMatch(field string, got string, want string) error {
	got = strings.TrimSpace(got)
	want = strings.TrimSpace(want)
	if got == "" || want == "" {
		return fmt.Errorf("%s is required", field)
	}
	if got != want {
		return fmt.Errorf("%s mismatch: got %q want %q", field, got, want)
	}
	return nil
}

func patchMaterializationChangeKind(file PatchMaterializedFile, casPath CASPatchPathResult) (string, error) {
	casChangeKind, err := validateCASPathChangeKind(casPath)
	if err != nil {
		return "", err
	}
	fileChangeKind := strings.TrimSpace(file.ChangeKind)
	if fileChangeKind == "" {
		fileChangeKind = CASPatchChangeModify
	}
	if fileChangeKind != casChangeKind {
		return "", fmt.Errorf("change_kind mismatch: got %q want %q", fileChangeKind, casChangeKind)
	}
	return casChangeKind, nil
}

func patchMaterializationFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
