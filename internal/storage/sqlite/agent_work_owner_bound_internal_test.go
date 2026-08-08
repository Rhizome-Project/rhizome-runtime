package sqlite

import "testing"

func TestAgentWorkOwnerBoundBranchMentionPrefersExactBranchIDOverReusedBranchName(t *testing.T) {
	const (
		targetBranchID = "projbranch-1778648866113702379-5892"
		staleBranchID  = "projbranch-1778629299299060243-10986"
		branchName     = "agent-beta-p-656e957c8a-t-83d0d5cd28"
	)
	branches := []ProjectBranchRecord{
		{
			BranchID:   targetBranchID,
			BranchName: branchName,
			AgentID:    "beta",
			Status:     ProjectBranchStatusReadyForReview,
		},
		{
			BranchID:   staleBranchID,
			BranchName: branchName,
			AgentID:    "beta",
			Status:     ProjectBranchStatusMerged,
		},
		{
			BranchID:   "projbranch-1778652221490963795-11570",
			BranchName: "agent-iota-p-656e957c8a-t-ca0cb177fd",
			AgentID:    "iota",
			Status:     ProjectBranchStatusReserved,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-owner-submit-requeue",
		Title:       "Beta owner-side same-head requeue submit for " + targetBranchID,
		Description: "Create the owner-side patch queue requeue submission on beta-owned branch `" + targetBranchID + "` / `" + branchName + "` for the existing head. Review doc: `project.icon-sprite-forge.branch." + targetBranchID + ".review`.",
		Tags:        []string{"project", "patch-queue", "requeue", "coordination", "owner-submit"},
	}

	branch, ok, ambiguous := agentWorkOwnerBoundBranchMentionedInTask(branches, task)
	if !ok || ambiguous {
		t.Fatalf("expected exact branch id to resolve despite reused branch name, got ok=%v ambiguous=%v branch=%+v", ok, ambiguous, branch)
	}
	if branch.BranchID != targetBranchID {
		t.Fatalf("expected target branch %s, got %+v", targetBranchID, branch)
	}
}

func TestAgentWorkActiveLanePublicationRepairSignalsStaySidecarNarrow(t *testing.T) {
	cases := []struct {
		name string
		task WorkspaceTaskRecord
		want bool
	}{
		{
			name: "tagged publication repair sidecar",
			task: WorkspaceTaskRecord{
				TaskID:    "task-repair-foundation-side-effects",
				ProjectID: "project-lua",
				Title:     "Repair foundation side-effects split from cmd/glua/main.go",
				Tags:      []string{"backend", "publication-repair"},
			},
			want: true,
		},
		{
			name: "publication repair follow-up sidecar",
			task: WorkspaceTaskRecord{
				TaskID:    "task-runner-boundary-follow-up",
				ProjectID: "project-lua",
				Title:     "Publication repair follow-up: verify runner boundary and smoke lane",
				Tags:      []string{"cli", "publication", "verification"},
			},
			want: true,
		},
		{
			name: "active implementation repair lane is not sidecar",
			task: WorkspaceTaskRecord{
				TaskID:      "task-cli-publication-active",
				ProjectID:   "project-lua",
				ProjectLane: "implementation",
				Title:       "CLI publication repair: expand runner and smoke boundary",
				Tags:        []string{"cli", "publication", "implementation"},
			},
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := agentWorkTaskLooksActiveLanePublication(tc.task); got != tc.want {
				t.Fatalf("agentWorkTaskLooksActiveLanePublication() = %v, want %v for %+v", got, tc.want, tc.task)
			}
		})
	}
}

func TestAgentWorkOwnerBoundBranchMentionUsesSingleLiveBranchForReusedName(t *testing.T) {
	const (
		targetBranchID = "projbranch-live"
		staleBranchID  = "projbranch-merged"
		branchName     = "agent-beta-p-reused"
	)
	branches := []ProjectBranchRecord{
		{BranchID: targetBranchID, BranchName: branchName, AgentID: "beta", Status: ProjectBranchStatusReadyForReview},
		{BranchID: staleBranchID, BranchName: branchName, AgentID: "beta", Status: ProjectBranchStatusMerged},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-owner-submit-requeue-by-name",
		Title:       "Owner requeue submit for beta branch",
		Description: "Create the owner-side patch queue requeue submission on branch `" + branchName + "`.",
		Tags:        []string{"project", "patch-queue", "requeue", "coordination", "owner-submit"},
	}

	branch, ok, ambiguous := agentWorkOwnerBoundBranchMentionedInTask(branches, task)
	if !ok || ambiguous {
		t.Fatalf("expected single live duplicate-name branch to resolve, got ok=%v ambiguous=%v branch=%+v", ok, ambiguous, branch)
	}
	if branch.BranchID != targetBranchID {
		t.Fatalf("expected live branch %s, got %+v", targetBranchID, branch)
	}
}

func TestAgentWorkOwnerBoundBranchMentionIdentityNameWinsOverDescriptionBranchID(t *testing.T) {
	const (
		targetBranchID = "projbranch-title-name"
		staleBranchID  = "projbranch-description-id"
		branchName     = "agent/beta/title-target"
	)
	branches := []ProjectBranchRecord{
		{BranchID: targetBranchID, BranchName: branchName, AgentID: "beta", Status: ProjectBranchStatusReadyForReview},
		{BranchID: staleBranchID, BranchName: "agent/gamma/description-noise", AgentID: "gamma", Status: ProjectBranchStatusReadyForReview},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "task-owner-submit-title-name",
		Title:       "Owner requeue submit for " + branchName,
		Description: "Copied context also mentions unrelated branch `" + staleBranchID + "`.",
		Tags:        []string{"owner-submit"},
	}

	branch, ok, ambiguous := agentWorkOwnerBoundBranchMentionedInTask(branches, task)
	if !ok || ambiguous {
		t.Fatalf("expected identity branch name to resolve before description branch id, got ok=%v ambiguous=%v branch=%+v", ok, ambiguous, branch)
	}
	if branch.BranchID != targetBranchID {
		t.Fatalf("expected target branch %s, got %+v", targetBranchID, branch)
	}
}

func TestAgentWorkOwnerBoundBranchMentionDoesNotMatchBranchIDPrefix(t *testing.T) {
	branches := []ProjectBranchRecord{
		{
			BranchID:   "projbranch-123",
			BranchName: "agent/beta/short",
			AgentID:    "beta",
			Status:     ProjectBranchStatusReadyForReview,
		},
		{
			BranchID:   "projbranch-123-5892",
			BranchName: "agent/beta/long",
			AgentID:    "beta",
			Status:     ProjectBranchStatusReadyForReview,
		},
	}
	task := WorkspaceTaskRecord{
		TaskID: "task-owner-submit",
		Title:  "Owner requeue submit for projbranch-123-5892",
		Tags:   []string{"owner-submit"},
	}

	branch, ok, ambiguous := agentWorkOwnerBoundBranchMentionedInTask(branches, task)
	if !ok || ambiguous {
		t.Fatalf("expected long branch id to resolve without prefix ambiguity, got ok=%v ambiguous=%v branch=%+v", ok, ambiguous, branch)
	}
	if branch.BranchID != "projbranch-123-5892" {
		t.Fatalf("expected long branch id, got %+v", branch)
	}
}

func TestAgentWorkOwnerBoundResolveBranchByNameRequiresSingleLiveBranch(t *testing.T) {
	branches := []ProjectBranchRecord{
		{BranchID: "branch-live-a", BranchName: "agent/beta/reused", AgentID: "beta", Status: ProjectBranchStatusActive},
		{BranchID: "branch-live-b", BranchName: "agent/beta/reused", AgentID: "beta", Status: ProjectBranchStatusReserved},
		{BranchID: "branch-terminal", BranchName: "agent/beta/reused", AgentID: "beta", Status: ProjectBranchStatusMerged},
	}

	if branch, ok := agentWorkOwnerBoundResolveBranch(branches, "", "agent/beta/reused"); ok {
		t.Fatalf("expected ambiguous live branch name to fail closed, got %+v", branch)
	}

	branches[1].Status = ProjectBranchStatusMerged
	branch, ok := agentWorkOwnerBoundResolveBranch(branches, "", "agent/beta/reused")
	if !ok {
		t.Fatalf("expected single live branch name to resolve")
	}
	if branch.BranchID != "branch-live-a" {
		t.Fatalf("expected branch-live-a, got %+v", branch)
	}
}
