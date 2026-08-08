package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const maxReadSize = 64 * 1024 // 64KB

// ReadFileTool reads a file from the workspace.
type ReadFileTool struct {
	workdir        string
	policy         workspacePathPolicy
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewReadFileTool(workdir string) *ReadFileTool {
	return &ReadFileTool{workdir: workdir}
}

func NewReadFileToolWithDeniedSubpaths(workdir string, denied []string) *ReadFileTool {
	return &ReadFileTool{
		workdir: workdir,
		policy:  newWorkspacePathPolicy(workdir, denied),
	}
}

func (t *ReadFileTool) WithProjectCheckoutGuard(client *RhizomeClient, workspaceID, agentID string, runtimeBinding func() AgentRuntimeBinding) *ReadFileTool {
	t.client = client
	t.workspaceID = strings.TrimSpace(workspaceID)
	t.agentID = strings.TrimSpace(agentID)
	t.runtimeBinding = runtimeBinding
	return t
}

func (t *ReadFileTool) Name() string { return "read_file" }

func (t *ReadFileTool) Description() string {
	return "Read a local file under the agent workdir. Do not use this for Rhizome workspace doc_key values; use workspace_doc_get for those. Returns file contents (max 64KB)."
}

func (t *ReadFileTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{
				"type":        "string",
				"description": "File path relative to workspace",
			},
		},
		"required": []string{"path"},
	}
}

func (t *ReadFileTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	path, _ := args["path"].(string)
	if path == "" {
		return &ToolResult{Output: "path is required", IsError: true}
	}

	fullPath, err := t.resolveReadPath(ctx, path)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if err := t.policy.Validate(fullPath); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if err := t.validateProjectCheckoutRead(ctx, fullPath); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("read error: %v. If %q is a Rhizome workspace doc_key, use workspace_doc_get instead of read_file.", err, path), IsError: true}
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, maxReadSize+1))
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("read error: %v", err), IsError: true}
	}
	if len(data) > maxReadSize {
		return &ToolResult{Output: string(data[:maxReadSize]) + "\n... (truncated at 64KB)"}
	}
	return &ToolResult{Output: string(data)}
}

func (t *ReadFileTool) resolveReadPath(ctx context.Context, path string) (string, error) {
	if !claimBoundPathUsesAgentRoot(path) {
		_, claimRoot, ok, err := t.activeClaimCheckoutRoot(ctx)
		if err != nil {
			return "", err
		}
		if ok {
			return safePath(claimRoot, path)
		}
	}
	return safePath(t.workdir, path)
}

func (t *ReadFileTool) validateProjectCheckoutRead(ctx context.Context, fullPath string) error {
	checkout, claimRoot, ok, err := t.activeClaimCheckoutRoot(ctx)
	if err != nil || !ok {
		return err
	}
	if !pathUnderProjectCheckouts(t.workdir, fullPath) || shellWorkdirUnderCheckoutRoot(fullPath, claimRoot) {
		return nil
	}
	binding := t.runtimeBinding()
	return fmt.Errorf("read_file refuses to inspect %s because current task %s is bound to active claim checkout %s at %s. Use the active claim checkout path; stale sibling project-checkouts are hidden for claimed implementation work", fullPath, strings.TrimSpace(binding.TaskID), strings.TrimSpace(checkout.CheckoutID), claimRoot)
}

// WriteFileTool writes a file to the workspace.
type WriteFileTool struct {
	workdir        string
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewWriteFileTool(workdir string) *WriteFileTool {
	return &WriteFileTool{workdir: workdir}
}

func (t *WriteFileTool) WithProjectCheckoutGuard(client *RhizomeClient, workspaceID, agentID string, runtimeBinding func() AgentRuntimeBinding) *WriteFileTool {
	t.client = client
	t.workspaceID = strings.TrimSpace(workspaceID)
	t.agentID = strings.TrimSpace(agentID)
	t.runtimeBinding = runtimeBinding
	return t
}

func (t *WriteFileTool) Name() string { return "write_file" }

func (t *WriteFileTool) Description() string {
	return "Write content to a file in the workspace. Creates parent directories if needed. Use content_base64 instead of content when source text contains many quotes, backslashes, or multiline syntax that makes JSON escaping fragile."
}

func (t *WriteFileTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{
				"type":        "string",
				"description": "File path relative to workspace",
			},
			"content": map[string]any{
				"type":        "string",
				"description": "File content to write",
			},
			"content_base64": map[string]any{
				"type":        "string",
				"description": "Optional base64-encoded file content. Use this instead of content for JSON-safe source writes; do not provide both content and content_base64.",
			},
		},
		"required": []string{"path"},
	}
}

func (t *WriteFileTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	path, _ := args["path"].(string)
	if path == "" {
		return &ToolResult{Output: "path is required", IsError: true}
	}
	content, decodedBase64, err := writeFileContentFromArgs(args)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}

	fullPath, err := t.resolveWritePath(ctx, path)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if err := t.validateProjectCheckoutWrite(ctx, fullPath); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return &ToolResult{Output: fmt.Sprintf("mkdir error: %v", err), IsError: true}
	}

	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		return &ToolResult{Output: fmt.Sprintf("write error: %v", err), IsError: true}
	}
	if decodedBase64 {
		return &ToolResult{Output: fmt.Sprintf("wrote %d bytes to %s from content_base64", len(content), path)}
	}
	return &ToolResult{Output: fmt.Sprintf("wrote %d bytes to %s", len(content), path)}
}

func writeFileContentFromArgs(args map[string]any) (string, bool, error) {
	content, hasContent := args["content"].(string)
	encoded, hasEncoded := args["content_base64"].(string)
	if hasContent && hasEncoded {
		return "", false, fmt.Errorf("write_file accepts either content or content_base64, not both")
	}
	if hasContent {
		return content, false, nil
	}
	if !hasEncoded {
		return "", false, fmt.Errorf("content or content_base64 is required")
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return "", false, fmt.Errorf("content_base64 decode error: %v", err)
	}
	return string(decoded), true, nil
}

func (t *WriteFileTool) resolveWritePath(ctx context.Context, path string) (string, error) {
	if !claimBoundPathUsesAgentRoot(path) {
		_, claimRoot, ok, err := t.activeClaimCheckoutRoot(ctx)
		if err != nil {
			return "", err
		}
		if ok {
			return safePath(claimRoot, path)
		}
	}
	return safePath(t.workdir, path)
}

func (t *WriteFileTool) validateProjectCheckoutWrite(ctx context.Context, fullPath string) error {
	if !pathUnderProjectCheckouts(t.workdir, fullPath) {
		return nil
	}
	if t.client == nil || t.workspaceID == "" || t.agentID == "" || t.runtimeBinding == nil {
		return nil
	}
	binding := t.runtimeBinding()
	projectID := strings.TrimSpace(binding.ProjectID)
	taskID := strings.TrimSpace(binding.TaskID)
	if projectID == "" || taskID == "" {
		return fmt.Errorf("write_file refuses to mutate project-checkouts without current project/task binding; use project_checkout_materialize from a claimed implementation task before editing product files")
	}
	repoRoot, err := projectCheckoutWriteGitRoot(ctx, fullPath)
	if err != nil {
		return fmt.Errorf("write_file refuses to mutate unregistered project-checkouts path: %v. Use project_checkout_materialize to create a registered checkout first", err)
	}
	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return fmt.Errorf("write_file could not verify project checkout ownership for project %s: %v", projectID, err)
	}
	if _, _, err := projectCheckoutWriteAuthority(ctx, coordination, repoRoot, t.agentID, taskID, "write_file"); err != nil {
		return err
	}
	return nil
}

func projectCheckoutWriteAuthority(ctx context.Context, coordination ProjectCoordinationRecord, repoRoot, agentID, taskID, toolName string) (ProjectCheckoutRecord, ProjectBranchRecord, error) {
	repoRoot = strings.TrimSpace(repoRoot)
	taskID = strings.TrimSpace(taskID)
	if projectCheckoutMarkedInvalid(repoRoot) {
		return ProjectCheckoutRecord{}, ProjectBranchRecord{}, fmt.Errorf("%s refuses to mutate %s because the checkout is marked invalid after a failed registration; materialize a fresh owned checkout before editing", toolName, repoRoot)
	}
	checkout, ok := projectCheckoutWriteOwnedCheckout(coordination.Checkouts, repoRoot, agentID, taskID)
	if !ok {
		return ProjectCheckoutRecord{}, ProjectBranchRecord{}, fmt.Errorf("%s refuses to mutate %s because it is not this agent's active implementation checkout for task %s. Review/validation checkouts are read-only; claim/create a revision implementation task and materialize an owned branch before editing", toolName, repoRoot, taskID)
	}
	currentBranch, err := currentGitBranch(ctx, repoRoot)
	if err != nil || strings.TrimSpace(currentBranch) == "" {
		if err == nil {
			err = fmt.Errorf("empty branch name")
		}
		return ProjectCheckoutRecord{}, ProjectBranchRecord{}, fmt.Errorf("%s refuses to mutate %s because current git branch cannot be verified: %v", toolName, repoRoot, err)
	}
	branch, ok := projectCheckoutWriteOwnedBranch(coordination.Branches, checkout, agentID, taskID, currentBranch)
	if !ok {
		return ProjectCheckoutRecord{}, ProjectBranchRecord{}, fmt.Errorf("%s refuses to mutate %s on branch %q because the branch is not registered as this agent's writable branch for task %s. Create a new owned revision branch instead of editing a review checkout or another agent's branch", toolName, repoRoot, currentBranch, taskID)
	}
	return checkout, branch, nil
}

func projectCheckoutMarkedInvalid(repoRoot string) bool {
	if strings.TrimSpace(repoRoot) == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(repoRoot, ".rhizome-invalid-checkout"))
	return err == nil
}

func pathUnderProjectCheckouts(workdir, fullPath string) bool {
	root := workspacePathPolicyRoot(workdir)
	if root == "" {
		return false
	}
	projectCheckouts := filepath.Join(root, "project-checkouts")
	rel, err := filepath.Rel(projectCheckouts, filepath.Clean(fullPath))
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func projectCheckoutWriteGitRoot(ctx context.Context, fullPath string) (string, error) {
	anchor, _, err := nearestExistingPath(fullPath)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(anchor)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		anchor = filepath.Dir(anchor)
	}
	out, err := exec.CommandContext(ctx, "git", "-C", anchor, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", err
	}
	return filepath.Clean(strings.TrimSpace(string(out))), nil
}

func projectCheckoutWriteOwnedCheckout(checkouts []ProjectCheckoutRecord, localPath, agentID, taskID string) (ProjectCheckoutRecord, bool) {
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if !projectCheckoutWriteStatusActive(checkout.Status) {
			continue
		}
		if !sameProjectLocalPath(checkout.LocalPath, localPath) {
			continue
		}
		if strings.TrimSpace(checkout.ActiveTaskID) != strings.TrimSpace(taskID) {
			continue
		}
		if kind := strings.ToLower(strings.TrimSpace(checkout.CheckoutKind)); kind == "review" || kind == "validation" {
			continue
		}
		return checkout, true
	}
	return ProjectCheckoutRecord{}, false
}

func projectCheckoutWriteOwnedBranch(branches []ProjectBranchRecord, checkout ProjectCheckoutRecord, agentID, taskID, branchName string) (ProjectBranchRecord, bool) {
	for _, branch := range branches {
		if strings.TrimSpace(branch.AgentID) != strings.TrimSpace(agentID) {
			continue
		}
		if strings.TrimSpace(branch.BranchName) != strings.TrimSpace(branchName) {
			continue
		}
		if checkoutID := strings.TrimSpace(checkout.CheckoutID); checkoutID != "" && strings.TrimSpace(branch.CheckoutID) != checkoutID {
			continue
		}
		if repoID := strings.TrimSpace(checkout.RepoID); repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		if strings.TrimSpace(branch.ActiveTaskID) != strings.TrimSpace(taskID) {
			continue
		}
		if !projectBranchWriteStatusWritable(branch.Status) {
			continue
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}

func sameProjectLocalPath(a, b string) bool {
	a = workspacePathPolicyRoot(a)
	b = workspacePathPolicyRoot(b)
	if a == "" || b == "" {
		return false
	}
	return strings.EqualFold(filepath.Clean(a), filepath.Clean(b))
}

func projectCheckoutWriteStatusActive(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "", "ACTIVE", "BLOCKED":
		return true
	default:
		return false
	}
}

func projectBranchWriteStatusWritable(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "", "RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW":
		return true
	default:
		return false
	}
}

// ListDirectoryTool lists files in a workspace directory.
type ListDirectoryTool struct {
	workdir        string
	policy         workspacePathPolicy
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewListDirectoryTool(workdir string) *ListDirectoryTool {
	return &ListDirectoryTool{workdir: workdir}
}

func NewListDirectoryToolWithDeniedSubpaths(workdir string, denied []string) *ListDirectoryTool {
	return &ListDirectoryTool{
		workdir: workdir,
		policy:  newWorkspacePathPolicy(workdir, denied),
	}
}

func (t *ListDirectoryTool) WithProjectCheckoutGuard(client *RhizomeClient, workspaceID, agentID string, runtimeBinding func() AgentRuntimeBinding) *ListDirectoryTool {
	t.client = client
	t.workspaceID = strings.TrimSpace(workspaceID)
	t.agentID = strings.TrimSpace(agentID)
	t.runtimeBinding = runtimeBinding
	return t
}

func (t *ListDirectoryTool) Name() string { return "list_directory" }

func (t *ListDirectoryTool) Description() string {
	return "List files and directories in a workspace path. Returns names with / suffix for directories."
}

func (t *ListDirectoryTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{
				"type":        "string",
				"description": "Directory path relative to workspace (default: root)",
			},
		},
	}
}

func (t *ListDirectoryTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	path, _ := args["path"].(string)
	if path == "" {
		path = "."
	}

	fullPath, err := t.resolveListPath(ctx, path)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if err := t.policy.Validate(fullPath); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("readdir error: %v", err), IsError: true}
	}
	entries, err = t.filterClaimBoundProjectCheckoutEntries(ctx, fullPath, entries)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}

	var b strings.Builder
	for _, e := range entries {
		if !t.policy.AllowDirEntry(fullPath, e.Name()) {
			continue
		}
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		b.WriteString(name)
		b.WriteString("\n")
	}
	if b.Len() == 0 {
		return &ToolResult{Output: "(empty directory)"}
	}
	return &ToolResult{Output: b.String()}
}

func (t *ListDirectoryTool) resolveListPath(ctx context.Context, path string) (string, error) {
	if !claimBoundPathUsesAgentRoot(path) {
		_, claimRoot, ok, err := t.activeClaimCheckoutRoot(ctx)
		if err != nil {
			return "", err
		}
		if ok {
			return safePath(claimRoot, path)
		}
	}
	return safePath(t.workdir, path)
}

func (t *ListDirectoryTool) filterClaimBoundProjectCheckoutEntries(ctx context.Context, fullPath string, entries []os.DirEntry) ([]os.DirEntry, error) {
	if !pathUnderProjectCheckouts(t.workdir, fullPath) {
		return entries, nil
	}
	checkout, claimRoot, ok, err := t.activeClaimCheckoutRoot(ctx)
	if err != nil || !ok {
		return entries, err
	}
	projectCheckoutsRoot := filepath.Join(workspacePathPolicyRoot(t.workdir), "project-checkouts")
	if sameProjectLocalPath(fullPath, projectCheckoutsRoot) {
		rel, err := filepath.Rel(filepath.Clean(projectCheckoutsRoot), filepath.Clean(claimRoot))
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
			return nil, fmt.Errorf("list_directory cannot map active claim checkout %s into project-checkouts", strings.TrimSpace(checkout.CheckoutID))
		}
		firstSegment := strings.Split(filepath.ToSlash(rel), "/")[0]
		var filtered []os.DirEntry
		for _, entry := range entries {
			if entry != nil && entry.Name() == firstSegment {
				filtered = append(filtered, entry)
			}
		}
		return filtered, nil
	}
	if shellWorkdirUnderCheckoutRoot(fullPath, claimRoot) {
		return entries, nil
	}
	binding := t.runtimeBinding()
	return nil, fmt.Errorf("list_directory refuses to inspect %s because current task %s is bound to active claim checkout %s at %s. Use the active claim checkout path; stale sibling project-checkouts are hidden for claimed implementation work", fullPath, strings.TrimSpace(binding.TaskID), strings.TrimSpace(checkout.CheckoutID), claimRoot)
}

func claimBoundPathUsesAgentRoot(path string) bool {
	if filepath.IsAbs(path) {
		return true
	}
	rel := normalizeWorkspaceRelativePath(path)
	if rel == "" || rel == "." {
		return false
	}
	first := rel
	if idx := strings.IndexRune(rel, filepath.Separator); idx >= 0 {
		first = rel[:idx]
	}
	return strings.EqualFold(first, "project-checkouts")
}

func (t *ReadFileTool) activeClaimCheckoutRoot(ctx context.Context) (ProjectCheckoutRecord, string, bool, error) {
	if t == nil || t.client == nil || t.workspaceID == "" || t.agentID == "" || t.runtimeBinding == nil {
		return ProjectCheckoutRecord{}, "", false, nil
	}
	checkout, ok, err := activeRuntimeClaimCheckout(ctx, t.client, t.workspaceID, t.agentID, t.runtimeBinding())
	if err != nil || !ok {
		return ProjectCheckoutRecord{}, "", ok, err
	}
	resolved, err := verifiedActiveClaimCheckoutRoot("read_file", t.workdir, checkout)
	if err != nil {
		return ProjectCheckoutRecord{}, "", true, err
	}
	return checkout, resolved, true, nil
}

func (t *WriteFileTool) activeClaimCheckoutRoot(ctx context.Context) (ProjectCheckoutRecord, string, bool, error) {
	if t == nil || t.client == nil || t.workspaceID == "" || t.agentID == "" || t.runtimeBinding == nil {
		return ProjectCheckoutRecord{}, "", false, nil
	}
	checkout, ok, err := activeRuntimeClaimCheckout(ctx, t.client, t.workspaceID, t.agentID, t.runtimeBinding())
	if err != nil || !ok {
		return ProjectCheckoutRecord{}, "", ok, err
	}
	resolved, err := verifiedActiveClaimCheckoutRoot("write_file", t.workdir, checkout)
	if err != nil {
		return ProjectCheckoutRecord{}, "", true, err
	}
	return checkout, resolved, true, nil
}

func (t *ListDirectoryTool) activeClaimCheckoutRoot(ctx context.Context) (ProjectCheckoutRecord, string, bool, error) {
	if t == nil || t.client == nil || t.workspaceID == "" || t.agentID == "" || t.runtimeBinding == nil {
		return ProjectCheckoutRecord{}, "", false, nil
	}
	checkout, ok, err := activeRuntimeClaimCheckout(ctx, t.client, t.workspaceID, t.agentID, t.runtimeBinding())
	if err != nil || !ok {
		return ProjectCheckoutRecord{}, "", ok, err
	}
	resolved, err := verifiedActiveClaimCheckoutRoot("list_directory", t.workdir, checkout)
	if err != nil {
		return ProjectCheckoutRecord{}, "", true, err
	}
	return checkout, resolved, true, nil
}

func verifiedActiveClaimCheckoutRoot(toolName, workdir string, checkout ProjectCheckoutRecord) (string, error) {
	resolved, err := trustedShellWorkdir("", checkout.LocalPath)
	if err != nil {
		return "", fmt.Errorf("%s cannot verify active claim checkout %s at %s: %w", toolName, strings.TrimSpace(checkout.CheckoutID), strings.TrimSpace(checkout.LocalPath), err)
	}
	if !pathUnderProjectCheckouts(workdir, resolved) {
		return "", fmt.Errorf("%s refuses active claim checkout %s at %s because it is outside the agent project-checkouts root", toolName, strings.TrimSpace(checkout.CheckoutID), resolved)
	}
	return resolved, nil
}

type workspacePathPolicy struct {
	root         string
	deniedRelSet []string
}

var workspaceSecretLikeDirNames = map[string]struct{}{
	".aws":         {},
	".docker":      {},
	".git":         {},
	".kube":        {},
	".ssh":         {},
	".terraform.d": {},
}

var workspaceSecretLikeBaseNames = map[string]struct{}{
	".env":               {},
	".envrc":             {},
	".git-credentials":   {},
	".netrc":             {},
	".npmrc":             {},
	".pypirc":            {},
	".terraformrc":       {},
	"agent.runtime.json": {},
	"credentials.json":   {},
	"id_ed25519":         {},
	"id_rsa":             {},
	"secrets.json":       {},
}

var workspaceSecretLikeSuffixes = []string{
	".key",
	".p12",
	".pem",
	".pfx",
}

func newWorkspacePathPolicy(workdir string, denied []string) workspacePathPolicy {
	root := workspacePathPolicyRoot(workdir)
	if root == "" || len(denied) == 0 {
		return workspacePathPolicy{}
	}
	seen := make(map[string]struct{}, len(denied))
	out := make([]string, 0, len(denied))
	for _, rel := range denied {
		rel = normalizeWorkspaceRelativePath(rel)
		if rel == "" || rel == "." {
			continue
		}
		if _, ok := seen[rel]; ok {
			continue
		}
		seen[rel] = struct{}{}
		out = append(out, rel)
	}
	return workspacePathPolicy{root: root, deniedRelSet: out}
}

func workspacePathPolicyRoot(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return ""
	}
	abs, err := filepath.Abs(workdir)
	if err != nil {
		return ""
	}
	abs = filepath.Clean(abs)
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(resolved)
	}
	return abs
}

func normalizeWorkspaceRelativePath(rel string) string {
	rel = strings.TrimSpace(rel)
	if rel == "" {
		return ""
	}
	rel = filepath.Clean(rel)
	if rel == "." {
		return rel
	}
	return strings.TrimPrefix(rel, "."+string(filepath.Separator))
}

func (p workspacePathPolicy) Validate(fullPath string) error {
	if p.root == "" || len(p.deniedRelSet) == 0 {
		return validateWorkspaceSecretLikePath(fullPath, p.root)
	}
	rel, err := filepath.Rel(p.root, fullPath)
	if err != nil {
		return validateWorkspaceSecretLikePath(fullPath, p.root)
	}
	rel = normalizeWorkspaceRelativePath(rel)
	for _, denied := range p.deniedRelSet {
		if rel == denied || strings.HasPrefix(rel, denied+string(filepath.Separator)) {
			return fmt.Errorf("inspect path unavailable: %s", rel)
		}
	}
	return validateWorkspaceSecretLikeRelPath(rel)
}

func (p workspacePathPolicy) AllowDirEntry(dirPath, entryName string) bool {
	if p.root == "" || len(p.deniedRelSet) == 0 {
		return validateWorkspaceSecretLikePath(filepath.Join(dirPath, entryName), p.root) == nil
	}
	return p.Validate(filepath.Join(dirPath, entryName)) == nil
}

func validateWorkspaceSecretLikePath(fullPath, root string) error {
	if strings.TrimSpace(root) == "" {
		return nil
	}
	rel, err := filepath.Rel(root, fullPath)
	if err != nil {
		return nil
	}
	return validateWorkspaceSecretLikeRelPath(normalizeWorkspaceRelativePath(rel))
}

func validateWorkspaceSecretLikeRelPath(rel string) error {
	if !workspacePathLooksSecretLike(rel) {
		return nil
	}
	return fmt.Errorf("inspect path unavailable: %s", rel)
}

func workspacePathLooksSecretLike(rel string) bool {
	rel = normalizeWorkspaceRelativePath(rel)
	if rel == "" || rel == "." {
		return false
	}
	for _, segment := range strings.Split(rel, string(filepath.Separator)) {
		segment = strings.ToLower(strings.TrimSpace(segment))
		if segment == "" || segment == "." {
			continue
		}
		if _, ok := workspaceSecretLikeDirNames[segment]; ok {
			return true
		}
	}
	return workspaceLeafLooksSecretLike(filepath.Base(rel))
}

func workspaceLeafLooksSecretLike(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}
	if _, ok := workspaceSecretLikeBaseNames[name]; ok {
		return true
	}
	if strings.HasPrefix(name, ".env.") && !workspaceLeafIsEnvTemplate(name) {
		return true
	}
	for _, suffix := range workspaceSecretLikeSuffixes {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	if strings.HasSuffix(name, ".tfvars") ||
		strings.HasSuffix(name, ".tfvars.json") ||
		strings.HasSuffix(name, ".tfstate") ||
		strings.HasSuffix(name, ".tfstate.backup") ||
		strings.HasSuffix(name, ".tfrc") ||
		strings.HasSuffix(name, ".tfrc.json") {
		return true
	}
	return strings.HasPrefix(name, "service-account") && strings.HasSuffix(name, ".json")
}

func workspaceLeafIsEnvTemplate(name string) bool {
	return strings.HasSuffix(name, ".example") ||
		strings.HasSuffix(name, ".sample") ||
		strings.HasSuffix(name, ".template")
}

// safePath resolves a relative path within workdir and prevents traversal.
func safePath(workdir, rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("absolute paths not allowed: %s", rel)
	}
	wdAbs, err := filepath.Abs(workdir)
	if err != nil {
		return "", err
	}
	wdReal, err := filepath.EvalSymlinks(wdAbs)
	if err != nil {
		return "", fmt.Errorf("resolve workspace root: %w", err)
	}
	joined := filepath.Clean(filepath.Join(wdAbs, rel))
	relToWD, err := filepath.Rel(wdAbs, joined)
	if err != nil {
		return "", err
	}
	if relToWD == ".." || strings.HasPrefix(relToWD, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes workspace: %s", rel)
	}

	resolved, err := resolveSafePathCandidate(wdReal, joined)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

func resolveSafePathCandidate(workspaceRoot, candidate string) (string, error) {
	resolved, err := filepath.EvalSymlinks(candidate)
	if err == nil {
		return enforceWorkspaceBoundary(workspaceRoot, resolved)
	}
	if !os.IsNotExist(err) {
		return "", fmt.Errorf("resolve path: %w", err)
	}

	ancestor, suffix, err := nearestExistingPath(candidate)
	if err != nil {
		return "", err
	}

	info, err := os.Stat(ancestor)
	if err != nil {
		return "", fmt.Errorf("stat path prefix: %w", err)
	}
	if suffix != "" && !info.IsDir() {
		return "", fmt.Errorf("path escapes workspace: %s", candidate)
	}

	resolvedAncestor, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		return "", fmt.Errorf("resolve path prefix: %w", err)
	}
	if suffix != "" && suffix != "." {
		resolvedAncestor = filepath.Join(resolvedAncestor, suffix)
	}
	return enforceWorkspaceBoundary(workspaceRoot, resolvedAncestor)
}

func nearestExistingPath(candidate string) (string, string, error) {
	current := candidate
	for {
		if _, err := os.Lstat(current); err == nil {
			rel, err := filepath.Rel(current, candidate)
			if err != nil {
				return "", "", err
			}
			return current, rel, nil
		} else if !os.IsNotExist(err) {
			return "", "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", "", fmt.Errorf("workspace path does not exist: %s", candidate)
		}
		current = parent
	}
}

func enforceWorkspaceBoundary(workspaceRoot, candidate string) (string, error) {
	relToRoot, err := filepath.Rel(workspaceRoot, candidate)
	if err != nil {
		return "", err
	}
	if relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes workspace: %s", candidate)
	}
	return candidate, nil
}
