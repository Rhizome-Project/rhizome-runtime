package sqlite

import "testing"

func TestClassifyCorridorTaskMapsTemplateAndKindToTaskClassHint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		task          corridorTaskContext
		wantClass     string
		wantCorridor  string
		minConfidence float64
		wantBasis     []string
	}{
		{
			name: "research coordination becomes exploration",
			task: corridorTaskContext{
				TaskID:       "task-research",
				Title:        "Explore rollout options",
				Description:  "Research the best deployment path before execution.",
				TaskKind:     "COORDINATION",
				TaskTemplate: "research",
				Tags:         []string{"discovery"},
			},
			wantClass:     taskClassHintExploration,
			wantCorridor:  "exploration",
			minConfidence: 0.85,
			wantBasis:     []string{"task_template:research"},
		},
		{
			name: "coordination alone stays unknown",
			task: corridorTaskContext{
				TaskID:       "task-coordination-only",
				Title:        "Weekly sync",
				Description:  "Talk through status.",
				TaskKind:     "COORDINATION",
				TaskTemplate: "generic",
			},
			wantClass:     taskClassHintUnknown,
			wantCorridor:  "",
			minConfidence: 0,
			wantBasis:     nil,
		},
		{
			name: "integration execution becomes integration",
			task: corridorTaskContext{
				TaskID:       "task-integration",
				Title:        "Wire adapter bridge",
				Description:  "Connect the transport and align the adapter boundary.",
				TaskKind:     "EXECUTION",
				TaskTemplate: "integration",
			},
			wantClass:     taskClassHintIntegration,
			wantCorridor:  "integration",
			minConfidence: 0.85,
			wantBasis:     []string{"task_template:integration"},
		},
		{
			name: "strong template beats weak coordination prior",
			task: corridorTaskContext{
				TaskID:       "task-template-dominates",
				Title:        "Service adapter follow-up",
				Description:  "Connect the migration boundary.",
				TaskKind:     "COORDINATION",
				TaskTemplate: "integration",
			},
			wantClass:     taskClassHintIntegration,
			wantCorridor:  "integration",
			minConfidence: 0.85,
			wantBasis:     []string{"task_template:integration"},
		},
		{
			name: "bugfix execution becomes incident",
			task: corridorTaskContext{
				TaskID:       "task-bugfix",
				Title:        "Fix deploy regression",
				Description:  "Repair the runtime outage and restore the path.",
				TaskKind:     "EXECUTION",
				TaskTemplate: "bugfix",
			},
			wantClass:     taskClassHintIncident,
			wantCorridor:  "incident",
			minConfidence: 0.88,
			wantBasis:     []string{"task_template:bugfix"},
		},
		{
			name: "coordination plus verification keywords becomes proof",
			task: corridorTaskContext{
				TaskID:       "task-proof",
				Title:        "Validate acceptance evidence",
				Description:  "Review the release package before handoff.",
				TaskKind:     "COORDINATION",
				TaskTemplate: "generic",
			},
			wantClass:     taskClassHintProof,
			wantCorridor:  "proof",
			minConfidence: 0.45,
			wantBasis:     []string{"task_kind:coordination", "title:validate"},
		},
		{
			name: "generic execution without metadata stays unknown",
			task: corridorTaskContext{
				TaskID:       "task-unknown",
				Title:        "Follow-up task",
				Description:  "Continue work.",
				TaskKind:     "EXECUTION",
				TaskTemplate: "generic",
			},
			wantClass:     taskClassHintUnknown,
			wantCorridor:  "",
			minConfidence: 0,
			wantBasis:     nil,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			record := classifyCorridorTask(tc.task)
			if record.TaskClassHint != tc.wantClass {
				t.Fatalf("expected task class %s, got %+v", tc.wantClass, record)
			}
			if record.CorridorHint != tc.wantCorridor {
				t.Fatalf("expected corridor hint %q, got %+v", tc.wantCorridor, record)
			}
			if tc.wantCorridor != "" && record.CorridorLookup.CatalogKey != tc.wantCorridor {
				t.Fatalf("expected lookup catalog %q, got %+v", tc.wantCorridor, record.CorridorLookup)
			}
			if record.HintConfidence < tc.minConfidence {
				t.Fatalf("expected confidence >= %.2f, got %+v", tc.minConfidence, record)
			}
			for _, basis := range tc.wantBasis {
				if !containsCorridorBasis(record.TaskClassBasis, basis) {
					t.Fatalf("expected basis %q in %+v", basis, record.TaskClassBasis)
				}
			}
			if tc.wantClass == taskClassHintUnknown {
				if record.HintConfidence != 0 {
					t.Fatalf("expected unknown classification to have zero confidence, got %+v", record)
				}
				if len(record.TaskClassBasis) != 0 {
					t.Fatalf("expected unknown classification to keep empty basis, got %+v", record)
				}
			}
		})
	}
}

func containsCorridorBasis(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
