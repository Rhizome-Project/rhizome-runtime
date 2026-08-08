package sqlite

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAgentWorkTaskFitSemanticSkillsDistinguishSpecializedImplementers(t *testing.T) {
	frontendProfile := AgentProfileRecord{
		AgentID:        "beta",
		Specialization: "Frontend application implementer; strong in React/Vite app scaffolds, responsive layout, component architecture, and styling systems.",
		Bio:            "Builds usable browser-facing product surfaces.",
		Tags:           []string{"frontend", "builder"},
		ToolsAccess:    []string{"shell", "browser"},
	}
	dataProfile := AgentProfileRecord{
		AgentID:        "delta",
		Specialization: "Data model testing implementer; strong in seed data, domain rules, fixtures, deterministic tests, and local state.",
		Bio:            "Builds reliable data and behavior slices.",
		Tags:           []string{"data-modeling", "testing"},
		ToolsAccess:    []string{"shell"},
	}
	shellTask := agentWorkFitTestTask(t, "task-shell", "Build responsive command-center shell", []string{"implementation", "design"}, []string{"frontend", "ui-architecture", "responsive-layout"}, []string{"shell"})
	dataTask := agentWorkFitTestTask(t, "task-data", "Build incident data model and fixtures", []string{"implementation"}, []string{"data-modeling", "testing", "fixtures", "local-state"}, []string{"shell"})

	frontendShellFit := agentWorkTaskFitForProfile(frontendProfile, shellTask, "claim", "", nil, nil)
	dataShellFit := agentWorkTaskFitForProfile(dataProfile, shellTask, "claim", "", nil, nil)
	if frontendShellFit.Score <= dataShellFit.Score || frontendShellFit.Level != "recommended" || dataShellFit.Level == "recommended" {
		t.Fatalf("frontend shell task should prefer frontend implementer, frontend=%+v data=%+v", frontendShellFit, dataShellFit)
	}

	frontendDataFit := agentWorkTaskFitForProfile(frontendProfile, dataTask, "claim", "", nil, nil)
	dataDataFit := agentWorkTaskFitForProfile(dataProfile, dataTask, "claim", "", nil, nil)
	if dataDataFit.Score <= frontendDataFit.Score || dataDataFit.Level != "recommended" {
		t.Fatalf("data/model task should prefer data implementer, frontend=%+v data=%+v", frontendDataFit, dataDataFit)
	}
}

func TestAgentWorkSemanticSignalMatchingAvoidsSubstringFalsePositives(t *testing.T) {
	if agentWorkSemanticSignalMatches("preview surface implementer", "review") {
		t.Fatal("preview must not satisfy review skill matching")
	}
	if agentWorkTextHasSignal("Design a contest dashboard", "test") {
		t.Fatal("contest must not satisfy test skill extraction")
	}
	if !agentWorkSemanticSignalMatches("artifact reviewer", "review") {
		t.Fatal("reviewer should still satisfy review skill matching")
	}
	if !agentWorkTextHasSignal("Run browser testing against the candidate", "test") {
		t.Fatal("testing should still satisfy test signal extraction")
	}
}

func TestAgentWorkSemanticTaskScopePrefersScaffoldBoundary(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-clearpress-scaffold",
		Title:       "Scaffold Clearpress app shell with mock auth and shared config ownership",
		Description: "Create the Vite baseline, shared config, package scripts, and app shell before feature lanes start.",
		Tags:        []string{"implementation", "frontend"},
	}
	got := agentWorkSemanticTaskWriteScopeHints(task, []string{"package.json", "public/**", "src/**", "tests/**"})
	for _, want := range []string{"package*.json", "vite.config.*", "index.html", "src/App.*", "src/ui/**"} {
		if !agentWorkStringSliceContainsFold(got, want) {
			t.Fatalf("expected scaffold scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"src/auth/**", "src/profile/**", "src/settings/**"} {
		if agentWorkStringSliceContainsFold(got, forbidden) {
			t.Fatalf("scaffold scope should not be narrowed to feature path %q, got %+v", forbidden, got)
		}
	}
}

func TestAgentWorkSemanticTaskScopeDoesNotLetViteOverbroadenFeatureScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-auth-settings",
		Title:       "Scaffold auth settings in existing Vite app",
		Description: "Wire login, profile avatar, and quote preferences inside the current frontend app.",
		Tags:        []string{"implementation", "frontend"},
	}
	got := agentWorkSemanticTaskWriteScopeHints(task, []string{"package.json", "public/**", "src/**", "tests/**"})
	for _, want := range []string{"src/auth/**", "src/profile/**", "src/settings/**"} {
		if !agentWorkStringSliceContainsFold(got, want) {
			t.Fatalf("expected feature scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"package*.json", "vite.config.*", "src/App.*", "src/ui/**"} {
		if agentWorkStringSliceContainsFold(got, forbidden) {
			t.Fatalf("feature scope should not be broadened to scaffold path %q, got %+v", forbidden, got)
		}
	}
}

func TestAgentWorkSemanticTaskScopeDoesNotRouteFrontendParserToGoInternalScope(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:          "task-clearpress-markdown-parser",
		Title:           "Implement markdown parser in existing Vite app",
		Description:     "Build the React frontend editor parser for browser markdown shortcuts.",
		Tags:            []string{"implementation", "frontend", "react", "vite"},
		ProjectLane:     "implementation",
		WriteScopeHints: []string{"src/**", "tests/**", "package.json"},
	}
	got := agentWorkSemanticTaskWriteScopeHints(task, task.WriteScopeHints)
	for _, want := range []string{"src/editor/**", "src/lib/editor/**", "tests/editor/**"} {
		if !agentWorkStringSliceContainsFold(got, want) {
			t.Fatalf("expected frontend parser scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"internal/parser/**", "internal/ast/**"} {
		if agentWorkStringSliceContainsFold(got, forbidden) {
			t.Fatalf("frontend parser scope must not route to Go internal path %q, got %+v", forbidden, got)
		}
	}
}

func TestAgentWorkSemanticTaskScopeNarrowsGoRQInterpreterLanes(t *testing.T) {
	tests := []struct {
		name      string
		task      WorkspaceTaskRecord
		want      []string
		forbidden []string
	}{
		{
			name: "lexer",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-lexer",
				Title:           "Implement rq lexer with positioned diagnostics",
				Description:     "Build tokenizer and token stream support for the rq interpreter.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/lexer/**", "internal/token/**"},
			forbidden: []string{"cmd/**", "internal/ast/**", "internal/diagnostics/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "parser",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-parser",
				Title:           "Implement rq parser and AST for full grammar",
				Description:     "Parse tokens into the rq AST.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/parser/**", "internal/ast/**"},
			forbidden: []string{"cmd/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "parser-lambda-syntax-broad",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-parser-lambda-syntax",
				Title:           "Implement rq lexer and parser grammar including lambda syntax",
				Description:     "Parse map/filter forms into AST nodes. Do not implement runtime builtins, function library, or lambda execution semantics.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/lexer/**", "internal/token/**", "internal/parser/**", "internal/ast/**"},
			forbidden: []string{"internal/builtins/**", "internal/functions/**", "internal/lambda/**", "cmd/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "parser-negated-evaluator-runtime-broad",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-parser-no-eval",
				Title:           "Implement rq parser grammar",
				Description:     "Parse JSON path syntax into AST nodes. Do not implement evaluator, runtime, query semantics, or path execution in this lane.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/parser/**", "internal/ast/**"},
			forbidden: []string{"internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/path/**", "internal/jsonpath/**", "cmd/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "evaluator",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-evaluator",
				Title:           "Implement rq evaluator core and JSON path semantics",
				Description:     "Evaluate rq AST nodes against JSON input values.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/evaluator/**", "internal/runtime/**", "internal/value/**"},
			forbidden: []string{"cmd/**", "internal/parser/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "builtins",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-builtins",
				Title:           "Implement rq builtins including map/filter lambdas",
				Description:     "Add built-in function library and lambda helpers.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"internal/builtins/**", "internal/lambda/**"},
			forbidden: []string{"cmd/**", "internal/evaluator/**", "**/*test.go", "go.mod", "README.md"},
		},
		{
			name: "cli-repl",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-cli-repl",
				Title:           "Implement rq CLI file mode and REPL",
				Description:     "Wire command line entrypoints and read-eval-print loop.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/**", "internal/**", "**/*test.go", "go.mod", "README.md"},
			},
			want:      []string{"cmd/**", "internal/cli/**", "internal/repl/**"},
			forbidden: []string{"internal/lexer/**", "internal/parser/**", "**/*test.go", "go.mod", "README.md"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := agentWorkSemanticTaskWriteScopeHints(tt.task, tt.task.WriteScopeHints)
			for _, want := range tt.want {
				if !agentWorkStringSliceContainsFold(got, want) {
					t.Fatalf("expected rq scope to include %q, got %+v", want, got)
				}
			}
			for _, forbidden := range tt.forbidden {
				if agentWorkStringSliceContainsFold(got, forbidden) {
					t.Fatalf("rq scope should not keep broad/conflicting path %q, got %+v", forbidden, got)
				}
			}
		})
	}
}

func TestAgentWorkLuaAcceptanceScopeNarrowsBroadLexerTask(t *testing.T) {
	requirements := `{"schema":"task_requirements.v1","acceptance_criteria_refs":["AC-LUA-LEX-01"],"write_scope_hints":["cmd/**","internal/**","testdata/**","tools/oracle/**","scripts/**","README.md"]}`
	task := WorkspaceTaskRecord{
		TaskID:               "task-1781622429496831800-2dab1f83",
		ProjectID:            "project-signal01-lua-capability",
		ProjectLane:          "implementation",
		Title:                "Implement AC-LUA-LEX-01: Lua lexer subset",
		Description:          "Lex Lua 5.1 subset tokens and source positions.",
		WriteScopeHints:      []string{"cmd/**", "internal/**", "testdata/**", "tools/oracle/**", "scripts/**", "README.md"},
		TaskRequirementsJSON: requirements,
	}

	got := agentWorkSemanticTaskWriteScopeHints(task, task.WriteScopeHints)
	for _, want := range []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**"} {
		if !agentWorkStringSliceContainsFold(got, want) {
			t.Fatalf("expected Lua lexer scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"cmd/**", "cmd/glua/**", "internal/**", "internal/runner/**", "internal/parser/**", "testdata/**", "testdata/smoke/**", "tools/oracle/**", "scripts/**", "README.md"} {
		if agentWorkStringSliceContainsFold(got, forbidden) {
			t.Fatalf("Lua lexer scope must not keep broad/harness path %q, got %+v", forbidden, got)
		}
	}

	scopeJSON, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok {
		t.Fatal("expected Lua lexer claim write scope")
	}
	claimPaths := writeScopePaths(scopeJSON)
	for _, forbidden := range []string{"cmd", "cmd/glua", "internal/runner", "testdata", "tools/oracle", "scripts", "README.md"} {
		if agentWorkStringSliceContainsScopePath(claimPaths, forbidden) {
			t.Fatalf("Lua lexer claim scope must exclude %q, json=%s", forbidden, scopeJSON)
		}
	}
}

func TestAgentWorkLuaAcceptanceScopeKeepsCLIHarnessLane(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-lua-cli-harness",
		ProjectID:   "project-signal01-lua-capability",
		ProjectLane: "implementation",
		Title:       "Implement AC-LUA-CLI-01 / AC-LUA-ERR-01 harness",
		Description: "Provide file-mode CLI, basic REPL, useful errors, oracle harness, and differential smoke corpus.",
		WriteScopeHints: []string{
			"cmd/**",
			"internal/**",
			"scripts/**",
			"testdata/**",
			"tools/oracle/**",
			"README.md",
		},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","acceptance_criteria_refs":["AC-LUA-CLI-01","AC-LUA-ERR-01"]}`,
	}

	got := agentWorkSemanticTaskWriteScopeHints(task, task.WriteScopeHints)
	for _, want := range []string{"cmd/glua/**", "internal/runner/**", "scripts/**", "testdata/smoke/**", "tools/oracle/**", "README.md", "internal/errors/**"} {
		if !agentWorkStringSliceContainsFold(got, want) {
			t.Fatalf("expected Lua CLI harness scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"internal/lexer/**", "internal/parser/**", "internal/eval/**", "testdata/**", "cmd/**", "internal/**"} {
		if agentWorkStringSliceContainsFold(got, forbidden) {
			t.Fatalf("Lua CLI harness scope should not keep unrelated broad path %q, got %+v", forbidden, got)
		}
	}
}

func TestTaskCreateNormalizesLuaAcceptanceScopeAtBirth(t *testing.T) {
	requirements := `{"schema":"task_requirements.v1","acceptance_criteria_refs":["AC-LUA-LEX-01"],"write_scope_hints":["cmd/**","internal/**","testdata/**","tools/oracle/**","scripts/**","README.md"]}`
	hints, normalizedRequirements := normalizeTaskCreateSemanticWriteScopeHints(TaskCreateInput{
		Title:                "Implement AC-LUA-LEX-01: Lua lexer subset",
		Description:          "Lex Lua 5.1 subset tokens and source positions.",
		TaskKind:             "EXECUTION",
		ProjectID:            "project-signal01-lua-capability",
		ProjectLane:          "implementation",
		TaskRequirementsJSON: requirements,
		WriteScopeHints:      []string{"cmd/**", "internal/**", "testdata/**", "tools/oracle/**", "scripts/**", "README.md"},
	}, "task-1781622429496831800-2dab1f83", normalizeTaskRequirementsJSON(requirements))

	for _, want := range []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**"} {
		if !agentWorkStringSliceContainsFold(hints, want) {
			t.Fatalf("expected birth-normalized Lua lexer scope to include %q, got %+v", want, hints)
		}
	}
	for _, forbidden := range []string{"cmd/**", "testdata/**", "tools/oracle/**", "scripts/**", "README.md"} {
		if agentWorkStringSliceContainsFold(hints, forbidden) || strings.Contains(normalizedRequirements, forbidden) {
			t.Fatalf("birth-normalized Lua lexer scope must remove %q, hints=%+v requirements=%s", forbidden, hints, normalizedRequirements)
		}
	}
}

func TestAgentWorkSemanticTaskScopeKeepsRun10RQLanesDisjoint(t *testing.T) {
	tests := []struct {
		name      string
		task      WorkspaceTaskRecord
		want      []string
		forbidden []string
	}{
		{
			name: "lexer-diagnostics-lane-stays-lexer-owned",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-lexer-diag",
				Title:           "Signal-01 Lane A: implement lexer and diagnostic primitives for rq",
				Description:     "Implement position-aware lexer/tokenization and diagnostic primitives for the rq interpreter.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/rq/**", "internal/lexer/**", "internal/diag/**", "internal/token/**", "internal/ast/**", "testdata/**", "go.mod", "go.sum"},
			},
			want:      []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**"},
			forbidden: []string{"cmd/rq/**", "internal/parser/**", "internal/ast/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
		},
		{
			name: "parser-ast-lane-does-not-overlap-lexer",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-parser-ast",
				Title:           "Signal-01 Lane B: implement parser and AST for rq expressions",
				Description:     "Parse rq tokens into AST nodes. Do not implement evaluator or runtime semantics in this lane.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"internal/parser/**", "internal/ast/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
			},
			want:      []string{"internal/parser/**", "internal/ast/**"},
			forbidden: []string{"internal/lexer/**", "internal/token/**", "internal/eval/**", "internal/evaluator/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
		},
		{
			name: "evaluator-coalition-lane-does-not-overlap-parser-or-lexer",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-evaluator-coalition",
				Title:           "Signal-01 Lane C: implement evaluator, built-ins, and lambda semantics for rq",
				Description:     "Evaluate rq AST nodes against JSON input values and add runtime builtins.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"internal/eval/**", "internal/jsonctx/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
			},
			want:      []string{"internal/eval/**", "internal/jsonctx/**", "internal/evaluator/**", "internal/builtins/**", "internal/lambda/**"},
			forbidden: []string{"internal/lexer/**", "internal/token/**", "internal/parser/**", "internal/ast/**", "internal/diag/**", "testdata/**", "go.mod", "go.sum"},
		},
		{
			name: "cli-repl-readme-lane-does-not-own-evaluator",
			task: WorkspaceTaskRecord{
				TaskID:          "task-signal01-rq-cli-repl-readme",
				Title:           "Signal-01 Lane D: implement CLI, REPL, and reviewer-facing README for rq",
				Description:     "Wire command line entrypoints and REPL to the actual evaluator without changing evaluator semantics.",
				ProjectLane:     "implementation",
				WriteScopeHints: []string{"cmd/rq/**", "internal/jsonctx/**", "README.md", "testdata/**", "go.mod", "go.sum"},
			},
			want:      []string{"cmd/rq/**", "README.md"},
			forbidden: []string{"internal/eval/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/path/**", "internal/jsonpath/**", "internal/jsonctx/**", "testdata/**", "go.mod", "go.sum"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := agentWorkSemanticTaskWriteScopeHints(tt.task, tt.task.WriteScopeHints)
			for _, want := range tt.want {
				if !agentWorkStringSliceContainsFold(got, want) {
					t.Fatalf("expected Run10 rq scope to include %q, got %+v", want, got)
				}
			}
			for _, forbidden := range tt.forbidden {
				if agentWorkStringSliceContainsFold(got, forbidden) {
					t.Fatalf("Run10 rq scope should not contain conflicting/shared path %q, got %+v", forbidden, got)
				}
			}
		})
	}
}

func TestAgentWorkSemanticTaskScopeKeepsRun23RQTestSuiteHintsAuthoritative(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-signal01-rq-test-suite-run23",
		Title:       "Signal-01 Lane: build authoritative rq test suite",
		Description: "Create table-driven and golden tests plus testdata fixtures for the rq interpreter. Do not implement auth, profile, or frontend flows.",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"**/*_test.go",
			"testdata/**",
			"internal/**",
			"cmd/rq/**",
		},
	}

	got := agentWorkSemanticTaskWriteScopeHints(task, task.WriteScopeHints)
	for _, want := range []string{"**/*_test.go", "testdata/**"} {
		if !agentWorkStringSliceContainsFold(got, want) {
			t.Fatalf("expected Run23 test scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{"src/auth/**", "tests/auth/**", "src/profile/**", "tests/profile/**", "internal/lexer/**", "internal/parser/**", "internal/evaluator/**", "internal/**", "cmd/rq/**"} {
		if agentWorkStringSliceContainsFold(got, forbidden) {
			t.Fatalf("Run23 test scope must not inherit broad or unrelated feature path %q, got %+v", forbidden, got)
		}
	}

	scopeJSON, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok {
		t.Fatal("expected Run23 test task claim write scope")
	}
	claimPaths := writeScopePaths(scopeJSON)
	for _, forbidden := range []string{"src/auth", "tests/auth", "src/profile", "tests/profile", "internal", "cmd/rq"} {
		if agentWorkStringSliceContainsScopePath(claimPaths, forbidden) {
			t.Fatalf("claim scope must preserve Run23 test hints and exclude %q, json=%s", forbidden, scopeJSON)
		}
	}
}

func TestAgentWorkSemanticTaskScopeKeepsRun8ParserHintsAuthoritative(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-1780212319327944000-0ad1b510",
		Title:       "Signal-01 Lane: lexer and parser foundation for rq",
		Description: "Implement the rq lexer, token stream, parser, and AST grammar, including lambda syntax plus map/filter parse forms. Do not implement evaluator, runtime builtins, function library, or lambda execution semantics in this lane.",
		ProjectLane: "implementation",
		WriteScopeHints: []string{
			"cmd/**",
			"internal/lexer/**",
			"internal/parser/**",
			"internal/ast/**",
			"go.mod",
			"go.sum",
			"tests/**",
			"testdata/**",
		},
	}

	got := agentWorkSemanticTaskWriteScopeHints(task, task.WriteScopeHints)
	for _, want := range []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**", "internal/parser/**", "internal/ast/**"} {
		if !agentWorkStringSliceContainsFold(got, want) {
			t.Fatalf("expected Run8 parser scope to include %q, got %+v", want, got)
		}
	}
	for _, forbidden := range []string{
		"internal/evaluator/**",
		"internal/runtime/**",
		"internal/value/**",
		"internal/path/**",
		"internal/jsonpath/**",
		"internal/builtins/**",
		"internal/builtin/**",
		"internal/functions/**",
		"internal/lambda/**",
		"cmd/**",
		"go.mod",
		"go.sum",
		"tests/**",
		"testdata/**",
	} {
		if agentWorkStringSliceContainsFold(got, forbidden) {
			t.Fatalf("Run8 parser scope must not inherit prose or broad glue path %q, got %+v", forbidden, got)
		}
	}

	scopeJSON, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok {
		t.Fatal("expected Run8 parser task claim write scope")
	}
	claimPaths := writeScopePaths(scopeJSON)
	for _, forbidden := range []string{"internal/builtins/**", "internal/functions/**", "internal/lambda/**", "cmd/**"} {
		if agentWorkStringSliceContainsScopePath(claimPaths, forbidden) {
			t.Fatalf("claim scope must preserve authoritative parser hints and exclude %q, json=%s", forbidden, scopeJSON)
		}
	}
	if !agentWorkStringSliceContainsScopePath(claimPaths, "internal/token/**") || !agentWorkStringSliceContainsScopePath(claimPaths, "internal/parser/**") {
		t.Fatalf("claim scope should retain parser/token family, got %s", scopeJSON)
	}
}

func TestAgentWorkImplementationTaskClaimScopeTreatsRequirementHintsAsAuthoritative(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"schema": "task_requirements.v1",
		"write_scope_hints": []string{
			"internal/lexer/**",
			"internal/parser/**",
			"internal/ast/**",
			"tests/**",
		},
	})
	if err != nil {
		t.Fatalf("marshal requirements: %v", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-run8-parser-requirements-hints",
		Title:                "Signal-01 Lane: lexer and parser foundation for rq",
		Description:          "Parse lambda syntax, map/filter forms, and function call grammar without implementing evaluator builtins or lambda runtime semantics.",
		ProjectLane:          "implementation",
		TaskRequirementsJSON: string(raw),
	}

	scopeJSON, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok {
		t.Fatal("expected claim scope derived from task requirements write_scope_hints")
	}
	claimPaths := writeScopePaths(scopeJSON)
	for _, want := range []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**", "internal/parser/**", "internal/ast/**"} {
		if !agentWorkStringSliceContainsScopePath(claimPaths, want) {
			t.Fatalf("requirements-derived scope should include %q, got %s", want, scopeJSON)
		}
	}
	for _, forbidden := range []string{"internal/builtins/**", "internal/functions/**", "internal/lambda/**", "tests/**"} {
		if agentWorkStringSliceContainsScopePath(claimPaths, forbidden) {
			t.Fatalf("requirements-derived scope must not inherit prose or broad glue path %q, got %s", forbidden, scopeJSON)
		}
	}
}

func TestAgentWorkImplementationTaskClaimScopeKeepsFileLevelRequirementHintsAuthoritative(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"schema":            "task_requirements.v1",
		"write_scope_hints": []string{"internal/parser/grammar.go"},
	})
	if err != nil {
		t.Fatalf("marshal requirements: %v", err)
	}
	task := WorkspaceTaskRecord{
		TaskID:               "task-run8-parser-file-requirements-hints",
		Title:                "Signal-01 Lane: parser grammar for rq lambda syntax",
		Description:          "Parse map/filter grammar. Do not implement builtins, functions, evaluator, or lambda runtime.",
		ProjectLane:          "implementation",
		TaskRequirementsJSON: string(raw),
	}

	scopeJSON, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok {
		t.Fatal("expected claim scope derived from file-level task requirements write_scope_hints")
	}
	claimPaths := writeScopePaths(scopeJSON)
	if !agentWorkStringSliceContainsScopePath(claimPaths, "internal/parser/grammar.go") {
		t.Fatalf("file-level requirements-derived scope should preserve original hint, got %s", scopeJSON)
	}
	for _, forbidden := range []string{"internal/parser/**", "internal/builtins/**", "internal/functions/**", "internal/lambda/**"} {
		if agentWorkStringSliceContainsScopePath(claimPaths, forbidden) {
			t.Fatalf("file-level requirements-derived scope must not broaden to %q, got %s", forbidden, scopeJSON)
		}
	}
}

func TestAgentWorkProfileRoutingEvidenceWinsOverStaleRegisteredRole(t *testing.T) {
	profile := AgentProfileRecord{
		AgentID:        "zeta",
		Specialization: "rq integration steward",
		Tags:           []string{"integrator", "patch-queue"},
		Metadata:       map[string]any{"default_work_mode": "integrator"},
	}
	staleRegistered := AgentRecord{
		AgentID: "zeta",
		Role:    "visual verifier",
	}

	withFallback := agentWorkProfileWithAgentFallback(profile, staleRegistered)
	if agentWorkStringSliceContainsFold(withFallback.Tags, "visual verifier") {
		t.Fatalf("explicit agent_profile routing evidence must not inherit stale agents.role tag, got %+v", withFallback.Tags)
	}
	if got := agentProfileFreshSelectionMode(withFallback); got != "synthesis" {
		t.Fatalf("profile mode = %q, want synthesis from agent_profile source of truth", got)
	}
	if !agentWorkProfileOrRegisteredRoleAllowsProjectLane(profile, staleRegistered, "integration") {
		t.Fatalf("integration lane should be allowed by explicit agent_profile despite stale registered role")
	}
	if agentWorkProfileOrRegisteredRoleAllowsProjectLane(profile, staleRegistered, "review") {
		t.Fatalf("stale registered visual verifier role must not make explicit integrator profile a reviewer")
	}

	emptyProfile := AgentProfileRecord{AgentID: "legacy"}
	legacyRegistered := AgentRecord{AgentID: "legacy", Role: "reviewer"}
	if !agentWorkProfileOrRegisteredRoleAllowsProjectLane(emptyProfile, legacyRegistered, "review") {
		t.Fatalf("empty profile should still fall back to registered role")
	}

	genericSkillProfile := AgentProfileRecord{
		AgentID: "legacy-with-skills",
		Tags:    []string{"go", "rq"},
	}
	genericSkillRegistered := AgentRecord{AgentID: "legacy-with-skills", Role: "visual verifier"}
	if !agentWorkProfileOrRegisteredRoleAllowsPatchQueueReview(genericSkillProfile, genericSkillRegistered) {
		t.Fatalf("generic skill tags must not suppress registered reviewer fallback")
	}
}

func agentWorkStringSliceContainsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(want)) {
			return true
		}
	}
	return false
}

func agentWorkStringSliceContainsScopePath(values []string, want string) bool {
	want = normalizeWriteScopePath(want)
	for _, value := range values {
		if normalizeWriteScopePath(value) == want {
			return true
		}
	}
	return false
}

func TestAgentWorkReviewCriticProfileCannotFreshSelectPureImplementation(t *testing.T) {
	criticProfile := AgentProfileRecord{
		AgentID:        "iota",
		Specialization: "Harsh real-user UI/UX critic; strong in visual QA, usability defects, contrast, clipping, hierarchy, and interaction affordance critique.",
		Tags:           []string{"visual QA", "usability defects", "interaction affordance critique"},
		ToolsAccess:    []string{"shell", "browser"},
	}
	implementationTask := agentWorkFitTestTask(t, "task-workflows", "Wire command workflows across console surfaces", []string{"implementation"}, []string{"frontend", "interaction-design", "ui-state", "react"}, []string{"shell"})
	if got := agentProfileFreshSelectionMode(criticProfile); got != "review" {
		t.Fatalf("critic profile mode = %q, want review", got)
	}
	if agentProfileAllowsFreshTaskSelectionForMode(criticProfile, implementationTask, true) {
		t.Fatalf("review/critic profile must not fresh-select pure implementation work")
	}

	frontendCriticProfile := AgentProfileRecord{
		AgentID:        "iota-frontend",
		Specialization: "Frontend UI/UX critic and browser smoke reviewer; strong in visual QA and usability defects.",
		Tags:           []string{"frontend", "critic", "browser smoke"},
		ToolsAccess:    []string{"shell", "browser"},
	}
	if got := agentProfileFreshSelectionMode(frontendCriticProfile); got != "review" {
		t.Fatalf("frontend critic profile mode = %q, want review", got)
	}
	if agentProfileAllowsFreshTaskSelectionForMode(frontendCriticProfile, implementationTask, true) {
		t.Fatalf("frontend critic profile must not fresh-select pure implementation work")
	}

	developerExperienceReviewerProfile := AgentProfileRecord{
		AgentID:        "dx-reviewer",
		Specialization: "Developer experience reviewer focused on acceptance evidence, setup clarity, and integration defects.",
		Tags:           []string{"developer-experience", "reviewer"},
		ToolsAccess:    []string{"shell"},
	}
	if got := agentProfileFreshSelectionMode(developerExperienceReviewerProfile); got != "review" {
		t.Fatalf("developer experience reviewer mode = %q, want review", got)
	}
	if agentProfileAllowsFreshTaskSelectionForMode(developerExperienceReviewerProfile, implementationTask, true) {
		t.Fatalf("developer experience reviewer must not fresh-select pure implementation work")
	}

	implementationReviewerProfile := AgentProfileRecord{
		AgentID:        "implementation-reviewer",
		Specialization: "Implementation reviewer focused on acceptance evidence, UI state regressions, and integration defects.",
		Tags:           []string{"implementation-review", "qa"},
		ToolsAccess:    []string{"shell", "browser"},
	}
	if got := agentProfileFreshSelectionMode(implementationReviewerProfile); got != "review" {
		t.Fatalf("implementation reviewer mode = %q, want review", got)
	}
	if agentProfileAllowsFreshTaskSelectionForMode(implementationReviewerProfile, implementationTask, true) {
		t.Fatalf("implementation reviewer must not fresh-select pure implementation work")
	}

	implementationQAProfile := AgentProfileRecord{
		AgentID:        "implementation-qa",
		Specialization: "Implementation QA focused on visual acceptance and interaction regressions.",
		Tags:           []string{"implementation-qa"},
		ToolsAccess:    []string{"shell", "browser"},
	}
	if got := agentProfileFreshSelectionMode(implementationQAProfile); got != "review" {
		t.Fatalf("implementation QA mode = %q, want review", got)
	}
	if agentProfileAllowsFreshTaskSelectionForMode(implementationQAProfile, implementationTask, true) {
		t.Fatalf("implementation QA profile must not fresh-select pure implementation work")
	}

	buildQAProfile := AgentProfileRecord{
		AgentID:        "build-qa",
		Specialization: "Build QA reviewer focused on smoke checks and acceptance evidence.",
		Tags:           []string{"build-qa"},
		ToolsAccess:    []string{"shell", "browser"},
	}
	if got := agentProfileFreshSelectionMode(buildQAProfile); got != "review" {
		t.Fatalf("build QA profile mode = %q, want review", got)
	}
	if agentProfileAllowsFreshTaskSelectionForMode(buildQAProfile, implementationTask, true) {
		t.Fatalf("build QA profile must not fresh-select pure implementation work")
	}

	implementerProfile := AgentProfileRecord{
		AgentID:        "beta",
		Specialization: "Frontend application implementer; strong in React/Vite app scaffolds, responsive layout, and product-quality critique.",
		Tags:           []string{"frontend", "implementer"},
		ToolsAccess:    []string{"shell", "browser"},
	}
	if got := agentProfileFreshSelectionMode(implementerProfile); got != "implementation" {
		t.Fatalf("explicit implementer profile mode = %q, want implementation", got)
	}
	if !agentProfileAllowsFreshTaskSelectionForMode(implementerProfile, implementationTask, true) {
		t.Fatalf("explicit frontend implementer should remain eligible for pure implementation work")
	}

	developerProfile := AgentProfileRecord{
		AgentID:        "gamma",
		Specialization: "Backend developer focused on local services and state integration.",
		Tags:           []string{"backend", "developer"},
		ToolsAccess:    []string{"shell"},
	}
	if got := agentProfileFreshSelectionMode(developerProfile); got != "implementation" {
		t.Fatalf("plain developer profile mode = %q, want implementation", got)
	}
	if !agentProfileAllowsFreshTaskSelectionForMode(developerProfile, implementationTask, true) {
		t.Fatalf("plain developer should remain eligible for pure implementation work")
	}

	explicitImplementationProfile := AgentProfileRecord{
		AgentID:        "lambda",
		Specialization: "Implementation",
		Tags:           []string{"frontend"},
		ToolsAccess:    []string{"shell"},
	}
	if got := agentProfileFreshSelectionMode(explicitImplementationProfile); got != "implementation" {
		t.Fatalf("exact implementation profile mode = %q, want implementation", got)
	}
	if !agentProfileAllowsFreshTaskSelectionForMode(explicitImplementationProfile, implementationTask, true) {
		t.Fatalf("exact implementation profile should remain eligible for pure implementation work")
	}
}

func TestProjectPatchQueueItemsReleaseBranchWriteScopeRequiresCurrentTerminalHeadAndNoLiveItem(t *testing.T) {
	const (
		branchID = "branch-ui"
		oldHead  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		headSHA  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		newHead  = "cccccccccccccccccccccccccccccccccccccccc"
	)

	if !projectBranchWriteScopeReleasedByTerminalPatchQueueDecision(ProjectBranchRecord{
		BranchID: branchID,
		HeadSHA:  headSHA,
	}, []ProjectPatchQueueItemRecord{{
		BranchID: branchID,
		HeadSHA:  headSHA,
		State:    ProjectPatchQueueStateBlocked,
	}}) {
		t.Fatalf("blocked terminal decision for current head should release inactive branch write scope")
	}
	if !projectBranchWriteScopeReleasedByTerminalPatchQueueDecision(ProjectBranchRecord{
		BranchID: branchID,
		HeadSHA:  headSHA,
	}, []ProjectPatchQueueItemRecord{{
		BranchID: branchID,
		HeadSHA:  headSHA,
		State:    ProjectPatchQueueStateIntegrated,
	}}) {
		t.Fatalf("integrated terminal decision for current head should release inactive branch write scope")
	}
	if projectBranchWriteScopeReleasedByTerminalPatchQueueDecision(ProjectBranchRecord{
		BranchID:     branchID,
		HeadSHA:      headSHA,
		ActiveTaskID: "task-active",
	}, []ProjectPatchQueueItemRecord{{
		BranchID: branchID,
		HeadSHA:  headSHA,
		State:    ProjectPatchQueueStateBlocked,
	}}) {
		t.Fatalf("active branch binding must preserve write scope even after terminal patch queue decision")
	}
	if projectBranchWriteScopeReleasedByTerminalPatchQueueDecision(ProjectBranchRecord{
		BranchID: branchID,
		HeadSHA:  newHead,
	}, []ProjectPatchQueueItemRecord{{
		BranchID: branchID,
		HeadSHA:  oldHead,
		State:    ProjectPatchQueueStateBlocked,
	}}) {
		t.Fatalf("old terminal decision must not release a branch re-registered to a new head")
	}
	if projectBranchWriteScopeReleasedByTerminalPatchQueueDecision(ProjectBranchRecord{
		BranchID: branchID,
		HeadSHA:  headSHA,
	}, []ProjectPatchQueueItemRecord{
		{
			BranchID: branchID,
			HeadSHA:  headSHA,
			State:    ProjectPatchQueueStateBlocked,
		},
		{
			BranchID: branchID,
			HeadSHA:  headSHA,
			State:    ProjectPatchQueueStateProposed,
		},
	}) {
		t.Fatalf("live same-branch patch queue item must preserve branch write scope")
	}
	if projectBranchWriteScopeReleasedByTerminalPatchQueueDecision(ProjectBranchRecord{
		BranchID: branchID,
		HeadSHA:  headSHA,
	}, []ProjectPatchQueueItemRecord{
		{
			BranchID: branchID,
			HeadSHA:  headSHA,
			State:    ProjectPatchQueueStateRejected,
		},
		{
			BranchID: branchID,
			HeadSHA:  newHead,
			State:    ProjectPatchQueueStateClaimed,
		},
	}) {
		t.Fatalf("live same-branch requeue/review item must preserve write scope even when its head differs")
	}
	if projectBranchWriteScopeReleasedByTerminalPatchQueueDecision(ProjectBranchRecord{
		BranchID: branchID,
		HeadSHA:  headSHA,
	}, []ProjectPatchQueueItemRecord{
		{
			BranchID: branchID,
			HeadSHA:  headSHA,
			State:    ProjectPatchQueueStateBlocked,
		},
		{
			BranchID: branchID,
			HeadSHA:  headSHA,
			State:    ProjectPatchQueueStateAccepted,
		},
	}) {
		t.Fatalf("accepted same-branch/current-head candidate must preserve write scope until integration closes it")
	}
}

func TestWriteScopesOverlapExcludingSharedSidecarsKeepsLanePathsExclusive(t *testing.T) {
	if writeScopesOverlapExcludingSharedSidecars(
		[]string{"internal/lexer/**", "go.mod", "go.sum"},
		[]string{"internal/eval/**", "go.mod", "go.sum"},
	) {
		t.Fatal("lanes that only share Go manifest sidecars should not block each other")
	}
	if !writeScopesOverlapExcludingSharedSidecars(
		[]string{"internal/lexer/**", "go.mod"},
		[]string{"internal/lexer/tokens.go", "go.mod"},
	) {
		t.Fatal("owned lane paths must remain exclusive even when sidecars are shared")
	}
	if writeScopesOverlapExcludingSharedSidecars(
		[]string{"cmd/rq/**", "package.json", "vite.config.*"},
		[]string{"internal/eval/**", "package-lock.json", "vite.config.ts"},
	) {
		t.Fatal("JS manifest/config sidecars should be shared when no owned lane path overlaps")
	}
}

func TestWriteScopesOverlapTreatsGlobalTestFileSuffixAsFileOnly(t *testing.T) {
	if writeScopesOverlap([]string{"**/*_test.go"}, []string{"internal/lexer/**"}) {
		t.Fatal("global *_test.go scope must not block a whole implementation package lane")
	}
	if writeScopesOverlapExcludingSharedSidecars([]string{"**/*_test.go", "testdata/**"}, []string{"internal/lexer/**", "go.mod"}) {
		t.Fatal("test-suite suffix and fixtures must not make unrelated implementation lanes scope-busy")
	}
	if !writeScopesOverlap([]string{"**/*_test.go"}, []string{"internal/lexer/lexer_test.go"}) {
		t.Fatal("global *_test.go scope must still overlap concrete test files")
	}
}

func agentWorkFitTestTask(t *testing.T, taskID, title string, modes, skills, tools []string) WorkspaceTaskRecord {
	t.Helper()
	payload := map[string]any{
		"required_work_modes": modes,
		"preferred_skills":    skills,
		"preferred_tools":     tools,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal task requirements: %v", err)
	}
	return WorkspaceTaskRecord{
		TaskID:               taskID,
		Title:                title,
		Status:               "PENDING",
		TaskKind:             "EXECUTION",
		ProjectLane:          "implementation",
		TaskRequirementsJSON: string(raw),
	}
}

// TestGoInterpreterRootScopeNeverFallsBackToFrontendTemplate locks R20-F1: a Go-interpreter
// task that matches no specific lane family (e.g. the root "extend baseline" lane) must keep
// its seeded wide scope (nil narrowing) instead of falling through into the frontend/Vite
// scaffold template - which is what bound the R20 root `**` lane to a stale JS-app scope and
// cascaded into side-effect/role-scope churn.
func TestGoInterpreterRootScopeNeverFallsBackToFrontendTemplate(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-signal01-rqs1-root-extend-baseline",
		ProjectID:   "project-signal01-rq-s1",
		ProjectLane: "implementation",
		Title:       "Extend the rq interpreter baseline toward the full operator spec",
		Description: "Root lane: extend the seeded Go interpreter baseline (foundation work across the whole repo) toward full operator spec coverage.",
		Tags:        []string{"rq", "implementation"},
	}
	hints := agentWorkSemanticTaskWriteScopeHints(task, []string{"**"})
	if len(hints) != 0 {
		t.Fatalf("root Go lane must keep seeded scope (nil hints), got %v", hints)
	}
	// A specific Go family still narrows as before.
	lexerTask := task
	lexerTask.TaskID = "task-signal01-rqs1-tokenizer-errors"
	lexerTask.Title = "Tighten lexer error coverage"
	lexerTask.Description = "Improve tokenizer error positions in the lexer."
	lexerHints := agentWorkSemanticTaskWriteScopeHints(lexerTask, []string{"**"})
	if len(lexerHints) == 0 {
		t.Fatalf("lexer task should still narrow to lexer families")
	}
	for _, h := range lexerHints {
		if strings.Contains(h, "vite") || strings.Contains(h, "src/") || strings.Contains(h, "package*") {
			t.Fatalf("lexer narrowing leaked frontend template path %q", h)
		}
	}
	// A genuinely frontend-flavored task (no Go signals) still gets the JS template.
	feTask := WorkspaceTaskRecord{
		TaskID:      "task-fe-foundation",
		ProjectID:   "project-some-webapp",
		ProjectLane: "implementation",
		Title:       "App shell foundation",
		Description: "Set up the frontend app shell and shared config with vite and typescript.",
	}
	feHints := agentWorkSemanticTaskWriteScopeHints(feTask, []string{"**"})
	if len(feHints) == 0 {
		t.Fatalf("frontend task should still receive the scaffold template")
	}
}

func TestAgentWorkScopeOverrideRequiresStructuralAnchor(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-1781635821912583300-cf33d54d",
		ProjectID:   "project-signal01-lua-capability",
		ProjectLane: "implementation",
		Title:       "CLI conformance publication repair follow-up",
		Description: "Repair Lua CLI conformance publication evidence.",
		WriteScopeHints: []string{
			"README.md",
			"internal/runner/**",
			"scripts/**",
			"testdata/smoke/**",
		},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","write_scope_hints":["README.md","internal/runner/**","scripts/**","testdata/smoke/**"]}`,
	}
	taskScope, ok := agentWorkImplementationTaskClaimWriteScope(task)
	if !ok {
		t.Fatal("expected Lua follow-up task scope")
	}
	role := ProjectRoleRecord{
		RoleID:         "projrole-1781635837098879571-16787",
		RoleType:       ProjectRoleImplementer,
		Status:         ProjectRoleStatusActive,
		WriteScopeJSON: `{"paths":["src/articles/**","tests/articles/**"]}`,
		Summary:        "Auto-provisioned by project task claim task-1781635821912583300-cf33d54d scope repair",
	}
	if agentWorkScopeOverrideAnchored(writeScopePaths(taskScope), writeScopePaths(role.WriteScopeJSON)) {
		t.Fatalf("article scope must not be structurally anchored to Lua CLI task scope")
	}
	if agentWorkShouldPreferRoleScopeForTrustFirstTask(task, taskScope, role) {
		t.Fatalf("agent.work.next must not prefer stale article role scope over Lua task scope")
	}
}
