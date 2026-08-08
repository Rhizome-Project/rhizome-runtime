package sqlite

import "testing"

// storageVisualGateMatrix is the storage-side replica of the agent visualGateMatrix (in
// agent/visual_acceptance_test.go). It is the behavioral DRIFT-GUARD demanded after the
// deny-list regression: the two suppression predicates are byte-aligned leaves but live in separate
// Go modules, so identical input batteries with identical expectations are asserted in BOTH. If
// either twin's classification diverges, that module's test fails. Storage is the AUTHORITATIVE gate
// (DecideProjectPatchQueueItemWithEvent), so a divergence here is the dangerous one.
var storageVisualGateMatrix = []struct {
	name          string
	pathset       []string
	provablyNonUI bool
}{
	// provably non-UI (suppress) -- pure backend only
	{"lua lexer lane", []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**", "testdata/lexer/**"}, true},
	{"single internal glob", []string{"internal/lexer/**"}, true},
	{"cmd main.go", []string{"cmd/rhizome/main.go"}, true},
	{"pkg go file", []string{"pkg/foo/bar.go"}, true},
	{"go mod+sum", []string{"go.mod", "go.sum"}, true},
	{"docs md", []string{"docs/design.md"}, true},
	{"scripts sh", []string{"scripts/build.sh"}, true},
	{"testdata glob", []string{"testdata/lexer/**"}, true},
	{"garbage dropped, backend remains", []string{"", "internal/lexer/**"}, true},
	// operator re-gate contract -- MUST-SUPPRESS (anti-re-freeze; the Lua lane)
	{"cmd glob", []string{"cmd/**"}, true},
	{"bare main.go", []string{"main.go"}, true},
	{"eval concrete go", []string{"internal/eval/eval.go"}, true},
	{"parser concrete go", []string{"internal/parser/parser.go"}, true},
	{"glob .go under backend root", []string{"internal/lexer/*.go"}, true},
	// .lua = Lua's own source/fixture ext (reachable on milestone path via seed smoke fixtures) -> suppress
	{"lua smoke seed fixture", []string{"seeds/lua51-subset/testdata/smoke/hello.lua"}, true},
	{"lua testdata fixture under interp", []string{"internal/lexer/testdata/foo.lua"}, true},
	// render/template are DIR-segment markers only -> interpreter FILES named render.go/template.go suppress
	{"interpreter render.go suppresses (file not dir)", []string{"internal/eval/render.go"}, true},
	{"interpreter template.go suppresses (file not dir)", []string{"internal/eval/template.go"}, true},
	// documented residual (operator-accepted): a .go GLOB over a UI package, or a bare extension-less
	// '*' glob, suppresses via backend-root rescue -> explicit-flag territory, NOT a blocker.
	{"go glob over server package (residual)", []string{"internal/server/*.go"}, true},
	{"bare star glob under root (residual)", []string{"internal/x/*"}, true},
	// UI surfaces with NO path signal pre-fix (the B1/B3 escapes) -- MUST gate
	{"sass stylesheet", []string{"theme.sass"}, false},
	{"less stylesheet", []string{"main.less"}, false},
	{"styl stylesheet", []string{"main.styl"}, false},
	{"raster png", []string{"logo.png"}, false},
	{"raster jpg", []string{"hero.jpg"}, false},
	{"svg asset", []string{"logo.svg"}, false},
	{"font woff", []string{"font.woff"}, false},
	{"font ttf", []string{"font.ttf"}, false},
	{"hbs template", []string{"page.hbs"}, false},
	{"handlebars template", []string{"page.handlebars"}, false},
	{"pug template", []string{"view.pug"}, false},
	{"ejs template", []string{"template.ejs"}, false},
	{"php", []string{"index.php"}, false},
	{"htm", []string{"index.htm"}, false},
	{"astro component", []string{"App.astro"}, false},
	{"razor component", []string{"App.razor"}, false},
	{"mdx", []string{"App.mdx"}, false},
	{"pwa manifest+sw", []string{"manifest.json", "sw.js"}, false},
	{"webmanifest under backend root", []string{"internal/site.webmanifest"}, false},
	{"flat ts ui", []string{"main.ts", "chart.ts"}, false},
	{"postcss config", []string{"postcss.config.js"}, false},
	{"svelte config", []string{"svelte.config.js"}, false},
	{"storybook", []string{".storybook/main.js"}, false},
	// UI directory segments even when the file is .go -- MUST gate
	{"layouts go", []string{"layouts/base.go"}, false},
	{"themes go", []string{"themes/playground.go"}, false},
	{"widgets go (proven regression)", []string{"widgets/chart.go"}, false},
	{"screens go", []string{"screens/home.go"}, false},
	{"views go", []string{"views/index.go"}, false},
	{"templates go", []string{"templates/page.go"}, false},
	{"frontend scope", []string{"frontend/**"}, false},
	{"client scope", []string{"client/**"}, false},
	{"dashboard scope", []string{"dashboard/**"}, false},
	// mixed lane: one UI sibling poisons the whole set -- MUST gate
	{"mixed lua + themes go", []string{"internal/lexer/**", "themes/playground.go"}, false},
	{"mixed lua + web html", []string{"internal/lexer/**", "web/index.html"}, false},
	{"garbage + ui", []string{"  ", "logo.png"}, false},
	// ambiguous roots -- MUST gate (unknown polarity)
	{"src glob", []string{"src/**"}, false},
	{"src go file", []string{"src/main.go"}, false},
	{"app glob", []string{"app/**"}, false},
	{"api ts", []string{"api/client.ts"}, false},
	{"bare ts", []string{"foo.ts"}, false},
	// operator re-gate contract -- MUST-GATE
	{"live dashboard go (Fix2)", []string{"internal/server/dashboard.go"}, false},
	{"web dashboard html go (Fix2)", []string{"agent/web_dashboard_html.go"}, false},
	{"gohtml template (Fix1)", []string{"internal/page.gohtml"}, false},
	{"tmpl template (Fix1)", []string{"cmd/server/page.tmpl"}, false},
	{"theme json data (Fix1)", []string{"internal/theme.json"}, false},
	{"ui_layout yaml data (Fix1)", []string{"internal/ui_layout.yaml"}, false},
	{"layout xml data (Fix1)", []string{"internal/layout.xml"}, false},
	{"view singular dir", []string{"internal/view/index.go"}, false},
	{"layout singular dir", []string{"internal/layout/base.go"}, false},
	{"concrete unknown-ext under root gates (Fix1)", []string{"internal/data.bin"}, false},
	{"double extension concrete gates (Fix1)", []string{"internal/foo.bar.baz"}, false},
	{"concrete gohtml under pure backend root gates (Fix1)", []string{"cmd/gen/report.gohtml"}, false},
	// glob UI payloads under a backend root -- MUST gate (closed the trailing-* escape)
	{"gohtml glob under backend root", []string{"internal/x/*.gohtml"}, false},
	{"tmpl glob under backend root", []string{"cmd/gen/*.tmpl"}, false},
	{"qml glob under backend root", []string{"internal/x/*.qml"}, false},
	{"concrete html dressed as trailing glob", []string{"internal/x/page.html*"}, false},
	{"svg dressed as trailing glob", []string{"internal/x/icon.svg*"}, false},
	{"deep glob ui payload", []string{"internal/x/**/*.gohtml"}, false},
	{"unknown-ext glob under backend root", []string{"internal/x/*.ts"}, false},
	// trailing-dot / unreadable-extension escapes -- MUST gate (closed the dir-rescue fallthrough)
	{"trailing-dot html escape", []string{"internal/x/index.html."}, false},
	{"trailing-dot vue escape", []string{"internal/x/app.vue."}, false},
	{"star-dot-star unreadable ext", []string{"internal/x/*.*"}, false},
	{"question-glob html escape", []string{"internal/x/page.htm?"}, false},
	{"stem-less ui leaf under root", []string{"internal/x/.vue"}, false},
	// degenerate / garbage -- MUST gate (fail-safe; B4)
	{"empty slice", []string{}, false},
	{"empty string entry", []string{""}, false},
	{"whitespace entry", []string{"   "}, false},
	{"dot + dotdot", []string{".", ".."}, false},
	{"bare slash", []string{"/"}, false},
}

func TestVisualGateProvablyNonUIPolarityStorage(t *testing.T) {
	for _, tc := range storageVisualGateMatrix {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := projectPatchQueueAcceptedVisualAcceptancePathsetIsProvablyNonUI(ProjectPatchQueueItemRecord{Pathset: tc.pathset})
			if got != tc.provablyNonUI {
				t.Fatalf("projectPatchQueueAcceptedVisualAcceptancePathsetIsProvablyNonUI(%v) = %v, want %v", tc.pathset, got, tc.provablyNonUI)
			}
		})
	}
}

func TestVisualGateEndToEndAdmissionStorage(t *testing.T) {
	const uiProse = "react frontend visual layout screenshot canvas"

	// Every UI/ambiguous escape, under explicit UI prose, MUST still require visual acceptance at the
	// AUTHORITATIVE storage gate (>=1 required reason). This is the admission-preservation guarantee
	// the deny-list version violated (it returned zero for a whole class of UI surfaces).
	for _, tc := range storageVisualGateMatrix {
		if tc.provablyNonUI || len(tc.pathset) == 0 {
			continue
		}
		got := projectPatchQueueAcceptedVisualAcceptanceRequiredReasons(ProjectPatchQueueItemRecord{Pathset: tc.pathset}, ProjectBranchRecord{}, uiProse, nil)
		if len(got) == 0 {
			t.Fatalf("%s: UI/ambiguous pathset %v under UI prose must require visual acceptance at storage gate, got none", tc.name, tc.pathset)
		}
	}

	// The Lua lexer lane, even under the same UI prose, MUST be suppressed (provably non-UI).
	lua := ProjectPatchQueueItemRecord{Pathset: []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**", "testdata/lexer/**"}}
	if got := projectPatchQueueAcceptedVisualAcceptanceRequiredReasons(lua, ProjectBranchRecord{}, "token layout and visual structure documented; go test ./... green", nil); len(got) != 0 {
		t.Fatalf("provably non-UI lexer lane must not require visual acceptance at storage gate, got %v", got)
	}

	// Explicit core-only declaration still suppresses (orthogonal coreOnly path intact).
	core := ProjectPatchQueueItemRecord{Pathset: []string{"src/**"}}
	if got := projectPatchQueueAcceptedVisualAcceptanceRequiredReasons(core, ProjectBranchRecord{}, "core-only library slice, no browser surface; refined visual layout", nil); len(got) != 0 {
		t.Fatalf("explicit core-only declaration must suppress at storage gate, got %v", got)
	}
}
