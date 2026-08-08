package sqlite

import "testing"

// TestInterpreterVendoringViolationsGoldenCases locks the NO-VENDORING analyzer against the
// real spec-gaming case and its inverse. The canonical product (signal01-lua-capability,
// managed-remote lua51-subset.git) had its conformance "10/23" served by vendored
// github.com/yuin/gopher-lua - commit 919f156 "Implement AST, control flow, and function
// forms" was a pure dep-add. This analyzer must BLOCK that go.mod and PASS a roster-built
// interpreter's go.mod (no third-party interpreter dependency).
func TestInterpreterVendoringViolationsGoldenCases(t *testing.T) {
	cases := []struct {
		name     string
		goMod    string
		wantPass bool // true => no violations (accept allowed); false => blocked
	}{
		{
			// EXACT go.mod at canonical 524869b / commit 919f156 - the spec-gaming smoking gun.
			name:     "919f156_vendored_gopher_lua_blocks",
			goMod:    "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nrequire github.com/yuin/gopher-lua v1.1.1 // indirect\n",
			wantPass: false,
		},
		{
			// A roster-built interpreter needs zero third-party deps - this MUST pass.
			name:     "clean_roster_interpreter_passes",
			goMod:    "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n",
			wantPass: true,
		},
		{
			// Block-form require with the vendored interpreter.
			name:     "block_form_gopher_lua_blocks",
			goMod:    "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nrequire (\n\tgithub.com/yuin/gopher-lua v1.1.1\n)\n",
			wantPass: false,
		},
		{
			// Deny-list analogue coverage (different vendored interpreter).
			name:     "analogue_starlark_blocks",
			goMod:    "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nrequire go.starlark.net v0.0.0-20231101\n",
			wantPass: false,
		},
		{
			// Heuristic names a not-yet-deny-listed interpreter-ish module path more loudly,
			// but under allow-list polarity it blocks regardless (any third-party gates).
			name:     "unlisted_lua_module_blocks",
			goMod:    "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nrequire github.com/someone/fast-lua-vm v0.3.0\n",
			wantPass: false,
		},
		{
			// ALLOW-LIST polarity: even a benign, non-interpreter third-party dep blocks a
			// dependency-free interpreter (the over-block heuristic risk is gone - the block does
			// not depend on guessing whether a module "looks like" an interpreter).
			name:     "benign_third_party_blocks_under_allowlist",
			goMod:    "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nrequire github.com/spf13/cobra v1.8.0\n",
			wantPass: false,
		},
		{
			// Replace-directive evasion: an innocent require name redirected to a LOCAL copy
			// (which could be a vendored interpreter) must BLOCK.
			name:     "local_replace_directive_blocks",
			goMod:    "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nrequire github.com/innocent/name v1.0.0\n\nreplace github.com/innocent/name => ./third_party/gopher-lua\n",
			wantPass: false,
		},
		{
			// Fail-closed: an empty / unreadable go.mod must BLOCK, never silently pass.
			name:     "empty_gomod_fails_closed",
			goMod:    "",
			wantPass: false,
		},
		{
			// Fail-closed: a module-less go.mod must BLOCK.
			name:     "moduleless_gomod_fails_closed",
			goMod:    "go 1.22\n",
			wantPass: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := interpreterVendoringViolations(tc.goMod)
			pass := len(got) == 0
			if pass != tc.wantPass {
				t.Fatalf("interpreterVendoringViolations pass=%v want=%v; violations=%v", pass, tc.wantPass, got)
			}
		})
	}
}

// TestInterpreterVendoringPermitsAndReplace locks the per-project allow-list and the
// local-vs-remote replace distinction.
func TestInterpreterVendoringPermitsAndReplace(t *testing.T) {
	// An explicitly permitted module passes; an unpermitted one still blocks.
	gomod := "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nrequire (\n\tgithub.com/spf13/cobra v1.8.0\n\tgithub.com/x/y v0.1.0\n)\n"
	if v := interpreterVendoringViolationsWithPermits(gomod, map[string]bool{"github.com/spf13/cobra": true}); len(v) == 0 {
		t.Fatal("an unpermitted third-party dep must still block even when another is permitted")
	}
	if v := interpreterVendoringViolationsWithPermits(gomod, map[string]bool{"github.com/spf13/cobra": true, "github.com/x/y": true}); len(v) != 0 {
		t.Fatalf("all third-party deps permitted -> must pass, got %v", v)
	}
	// A NON-local replace (module redirect, not a filesystem path) is not a local-vendoring
	// signal on its own (the require itself is still evaluated).
	remoteReplace := "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nreplace github.com/a/b => github.com/a/b-fork v1.2.0\n"
	if v := interpreterVendoringViolations(remoteReplace); len(v) != 0 {
		t.Fatalf("a module-to-module replace with no require must not add a local-replace violation, got %v", v)
	}
	// A local-path replace is a vendoring vector and must block.
	localReplace := "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nreplace github.com/a/b => ../local/b\n"
	if v := interpreterVendoringViolations(localReplace); len(v) == 0 {
		t.Fatal("a local-path replace directive must block (vendoring vector)")
	}
}

// TestGoModParsingHelpers locks the go.mod parser the analyzer depends on.
func TestGoModParsingHelpers(t *testing.T) {
	gomod := "module github.com/Rhizome-Project/rhizome-runtime/lua51-subset\n\ngo 1.22\n\nrequire (\n\tgithub.com/yuin/gopher-lua v1.1.1 // indirect\n\tgithub.com/spf13/cobra v1.8.0\n)\n\nrequire github.com/x/y v0.1.0\n"
	if got := goModModulePath(gomod); got != "github.com/Rhizome-Project/rhizome-runtime/lua51-subset" {
		t.Fatalf("goModModulePath = %q want canonical module path", got)
	}
	reqs := goModRequiredModulePaths(gomod)
	want := map[string]bool{"github.com/yuin/gopher-lua": true, "github.com/spf13/cobra": true, "github.com/x/y": true}
	if len(reqs) != len(want) {
		t.Fatalf("goModRequiredModulePaths = %v want keys %v", reqs, want)
	}
	for _, r := range reqs {
		if !want[r] {
			t.Fatalf("unexpected required module %q in %v", r, reqs)
		}
	}
	// own-module subpaths and the own module are never third-party.
	if !moduleIsOwnOrStdlib("github.com/Rhizome-Project/rhizome-runtime/lua51-subset/internal/eval", "github.com/Rhizome-Project/rhizome-runtime/lua51-subset") {
		t.Fatal("own-module subpath must be treated as own/stdlib")
	}
	if moduleIsOwnOrStdlib("github.com/yuin/gopher-lua", "rhizome/lua51-subset") {
		t.Fatal("third-party module must not be treated as own/stdlib")
	}
}
