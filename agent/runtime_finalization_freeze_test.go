package main

import "testing"

// R67 finalization-liveness: runtimeProjectFinalizationConvergeable gates the freeze of discretionary
// coordination minting. It must be TRUE only when the project's blocking frontier is genuinely clear
// (product spec-complete), and FALSE (allow minting) when any spec-rooted blocking task remains or the
// task frontier is unknown/empty (errs toward allow).
func TestRuntimeProjectFinalizationConvergeable(t *testing.T) {
	const pid = "project-rq"
	cases := []struct {
		name  string
		coord ProjectCoordinationRecord
		want  bool
	}{
		{
			name:  "empty task frontier -> not convergeable (errs toward allow)",
			coord: ProjectCoordinationRecord{},
			want:  false,
		},
		{
			name: "only discretionary coordination open -> convergeable",
			coord: ProjectCoordinationRecord{Tasks: []WorkspaceTaskRecord{
				{TaskID: "task-project-claim-repair-x", ProjectID: pid, Status: "RUNNING", TaskKind: "COORDINATION", ProjectLane: "strategy", Tags: []string{"project-claim-repair"}},
				{TaskID: "task-role-scope-y", ProjectID: pid, Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "coordination", Tags: []string{"project-role-scope"}},
			}},
			want: true,
		},
		{
			name: "an open spec-rooted blocking task remains -> NOT convergeable",
			coord: ProjectCoordinationRecord{Tasks: []WorkspaceTaskRecord{
				{TaskID: "task-project-claim-repair-x", ProjectID: pid, Status: "RUNNING", TaskKind: "COORDINATION", ProjectLane: "strategy", Tags: []string{"project-claim-repair"}},
				{TaskID: "task-patchq-revision-z", ProjectID: pid, Status: "RUNNING", TaskKind: "EXECUTION", ProjectLane: "implementation"},
			}},
			want: false,
		},
		{
			name: "blocking task in a DIFFERENT project does not gate this one",
			coord: ProjectCoordinationRecord{Tasks: []WorkspaceTaskRecord{
				{TaskID: "task-other-impl", ProjectID: "project-other", Status: "RUNNING", TaskKind: "EXECUTION", ProjectLane: "implementation"},
				{TaskID: "task-role-scope-y", ProjectID: pid, Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "coordination", Tags: []string{"project-role-scope"}},
			}},
			want: true,
		},
		{
			name: "empty project id -> not convergeable",
			coord: ProjectCoordinationRecord{Tasks: []WorkspaceTaskRecord{
				{TaskID: "task-role-scope-y", ProjectID: pid, Status: "PENDING", TaskKind: "COORDINATION", ProjectLane: "coordination", Tags: []string{"project-role-scope"}},
			}},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			projectID := pid
			if tc.name == "empty project id -> not convergeable" {
				projectID = ""
			}
			if got := runtimeProjectFinalizationConvergeable(tc.coord, projectID); got != tc.want {
				t.Fatalf("runtimeProjectFinalizationConvergeable = %v, want %v", got, tc.want)
			}
		})
	}
}
