package main

import (
	"image"
	"image/color"
	"image/png"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVisualAcceptanceEvidenceRejectsBetaLikeChecklistPacket(t *testing.T) {
	docs := []WorkspaceDocRecord{{
		DocKey: "task.visual_acceptance",
		Title:  "Visual Acceptance Packet",
		Content: strings.Join([]string{
			"schema: rhizome_visual_acceptance_v1",
			"visual_verdict: pass",
			"reviewed_against: AC-13, EV-01",
			"observed_url: http://127.0.0.1:59432/",
			"validation_checkout: project-checkouts/icon-sprite-forge-beta-visual-acceptance-dc954850",
			"branch_id: projbranch-1",
			"head_sha: dc954850cc167dc331d16bb880ca949e22bb764f",
			"screenshot_refs: artifacts/desktop-samples.png, artifacts/desktop-upload.png, artifacts/mobile-samples.png",
			"viewport_matrix: desktop 1440x1100, mobile 390x844",
			"Real User Scenarios Tested: desktop sample flow, desktop upload flow, desktop search/filter flow, mobile responsive flow",
			"checks: overlap, clipping, contrast, readability, responsive fit, typography, hierarchy, spacing, real-user usability",
			"findings: none",
		}, "\n"),
	}}

	ok, evidence, missing, blocking := visualAcceptanceEvidenceSatisfied(docs)
	if ok {
		t.Fatalf("Beta-like generic packet should not satisfy visual acceptance")
	}
	if len(evidence) != 1 || evidence[0] != "task.visual_acceptance" {
		t.Fatalf("expected inspected visual doc key, got evidence=%v", evidence)
	}
	for _, want := range []string{"initial_state screenshot ref/path", "primary_flow screenshot ref/path", "result_state screenshot ref/path"} {
		if !containsAnySignal(strings.Join(missing, "\n"), []string{want}) {
			t.Fatalf("expected missing %q, got missing=%v blocking=%v", want, missing, blocking)
		}
	}
}

func TestVisualAcceptanceEvidenceAcceptsStructuredCanaryPacket(t *testing.T) {
	docs := []WorkspaceDocRecord{{
		DocKey:  "project.visual_acceptance",
		Title:   "Visual Acceptance Packet",
		Content: completeStructuredVisualPacket(),
	}}

	ok, evidence, missing, blocking := visualAcceptanceEvidenceSatisfied(docs)
	if !ok {
		t.Fatalf("expected structured packet to satisfy visual acceptance, evidence=%v missing=%v blocking=%v", evidence, missing, blocking)
	}
}

func TestVisualAcceptanceEvidenceRejectsDuplicateStateScreenshotRefs(t *testing.T) {
	packet := strings.ReplaceAll(completeStructuredVisualPacket(), "artifacts/result-export-desktop.png", "artifacts/primary-upload-desktop.png")
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, missing, _ := visualAcceptanceEvidenceSatisfied(docs)
	if ok {
		t.Fatalf("duplicate state screenshot refs should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(missing, "\n"), []string{"distinct screenshot refs per required visual state"}) {
		t.Fatalf("expected duplicate-ref missing requirement, got %v", missing)
	}
}

func TestVisualAcceptanceEvidenceRejectsMissingMobileScreenshotRef(t *testing.T) {
	packet := strings.ReplaceAll(completeStructuredVisualPacket(), "  mobile_state: screenshot_ref artifacts/mobile-initial-empty.png\n", "")
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, missing, _ := visualAcceptanceEvidenceSatisfied(docs)
	if ok {
		t.Fatalf("packet without narrow/mobile screenshot ref should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(missing, "\n"), []string{"narrow/mobile screenshot ref"}) {
		t.Fatalf("expected missing mobile screenshot ref, got %v", missing)
	}
}

func TestVisualAcceptanceEvidenceRejectsMissingPrimarySurfaceGeometryCheck(t *testing.T) {
	packet := strings.ReplaceAll(completeStructuredVisualPacket(), "  primary surface geometry/density: pass\n", "")
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, missing, _ := visualAcceptanceEvidenceSatisfied(docs)
	if ok {
		t.Fatalf("packet without primary-surface geometry/density check should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(missing, "\n"), []string{"primary-surface geometry/density check"}) {
		t.Fatalf("expected missing primary-surface geometry check, got %v", missing)
	}
}

func TestVisualAcceptanceEvidenceRejectsMismatchedCandidateProvenance(t *testing.T) {
	item := ProjectPatchQueueItemRecord{
		QueueID:  "queue-current",
		ItemID:   "item-current",
		BranchID: "branch-current",
		HeadSHA:  "0123456789abcdef0123456789abcdef01234567",
	}
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: completeStructuredVisualPacket()}}

	ok, _, missing, _ := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, item)
	if ok {
		t.Fatalf("foreign visual packet should not satisfy current candidate")
	}
	for _, want := range []string{"visual packet candidate provenance matching branch_id or queue_id/item_id", "visual packet head_sha matching candidate"} {
		if !containsAnySignal(strings.Join(missing, "\n"), []string{want}) {
			t.Fatalf("expected missing %q, got %v", want, missing)
		}
	}
}

func TestVisualAcceptanceEvidenceUsesDeterministicIndependentPackets(t *testing.T) {
	docs := []WorkspaceDocRecord{
		{
			DocKey:    "z-old-blocker",
			UpdatedAt: "2026-05-14T07:00:00Z",
			Content: strings.Join([]string{
				"schema: rhizome_visual_acceptance_v1",
				"visual_verdict: pass",
				"blocking finding: broken first viewport and giant heading",
			}, "\n"),
		},
		{
			DocKey:    "a-new-pass",
			UpdatedAt: "2026-05-14T08:00:00Z",
			Content:   completeStructuredVisualPacket(),
		},
	}

	ok, evidence, missing, blocking := visualAcceptanceEvidenceSatisfied(docs)
	if !ok {
		t.Fatalf("complete non-blocking packet should satisfy independently, evidence=%v missing=%v blocking=%v", evidence, missing, blocking)
	}
	if len(evidence) == 0 || evidence[0] != "a-new-pass" {
		t.Fatalf("expected deterministic newest packet first, got %v", evidence)
	}
}

func TestVisualAcceptanceEvidenceNewestBlockerMasksOlderPass(t *testing.T) {
	docs := []WorkspaceDocRecord{
		{
			DocKey:    "a-old-pass",
			UpdatedAt: "2026-05-14T07:00:00Z",
			Content:   completeStructuredVisualPacket(),
		},
		{
			DocKey:    "z-new-blocker",
			UpdatedAt: "2026-05-14T08:00:00Z",
			Content:   completeStructuredVisualPacket() + "\nblocking finding: overlapping dropzone copy and unreadable pale hero.",
		},
	}

	ok, evidence, _, blocking := visualAcceptanceEvidenceSatisfied(docs)
	if ok {
		t.Fatalf("newer blocker should not be masked by older pass, evidence=%v blocking=%v", evidence, blocking)
	}
	for _, want := range []string{"text overlap", "unreadable text"} {
		if !containsAnySignal(strings.Join(blocking, "\n"), []string{want}) {
			t.Fatalf("expected blocking signal %q, got %v", want, blocking)
		}
	}
}

func TestVisualAcceptanceUIFacingSignalsCoverBroadFrontendPathsets(t *testing.T) {
	cases := []struct {
		name    string
		item    ProjectPatchQueueItemRecord
		summary string
		want    string
	}{
		{name: "src glob with frontend evidence", item: ProjectPatchQueueItemRecord{Pathset: []string{"src/**"}}, summary: "React frontend app", want: "path:src-ui-scope"},
		{name: "app glob", item: ProjectPatchQueueItemRecord{Pathset: []string{"app/**"}}, want: "path:app-ui-scope"},
		{name: "components glob", item: ProjectPatchQueueItemRecord{Pathset: []string{"components/**"}}, want: "path:components-ui-scope"},
		{name: "styles glob", item: ProjectPatchQueueItemRecord{Pathset: []string{"styles/**"}}, want: "path:styles-ui-scope"},
		{name: "vue component", item: ProjectPatchQueueItemRecord{Pathset: []string{"src/App.vue"}}, want: "path:ui-component"},
		{name: "svelte component", item: ProjectPatchQueueItemRecord{Pathset: []string{"src/App.svelte"}}, want: "path:ui-component"},
		{name: "pages glob", item: ProjectPatchQueueItemRecord{Pathset: []string{"pages/**"}}, want: "path:pages-ui-scope"},
		{name: "ui glob", item: ProjectPatchQueueItemRecord{Pathset: []string{"ui/**"}}, want: "path:ui-scope"},
		{name: "tailwind config", item: ProjectPatchQueueItemRecord{Pathset: []string{"tailwind.config.ts"}}, want: "path:tailwind"},
		{name: "next config", item: ProjectPatchQueueItemRecord{Pathset: []string{"next.config.js"}}, want: "path:next"},
		{name: "package with vite evidence", item: ProjectPatchQueueItemRecord{Pathset: []string{"package.json"}}, summary: "React Vite frontend app", want: "path:package-ui"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := uiFacingSignalsForPatchQueueItem(nil, tc.item, tc.summary)
			if !containsAnySignal(strings.Join(got, "\n"), []string{tc.want}) {
				t.Fatalf("expected %q in signals, got %v", tc.want, got)
			}
		})
	}
}

func TestVisualAcceptanceUIFacingSignalsIgnoreCoreLibraryPathsets(t *testing.T) {
	item := ProjectPatchQueueItemRecord{Pathset: []string{
		"src/**",
		"tests/**",
		"package.json",
		"package-lock.json",
		"tsconfig*.json",
		"vite.config.*",
		"README*",
	}}
	summary := "Accepted as a core slice: deterministic normalization/export core, src/core and tests/core, Vitest green, no browser/app surface."

	if got := uiFacingSignalsForPatchQueueItem(nil, item, summary); len(got) != 0 {
		t.Fatalf("core-only library slice should not require visual acceptance, got %v", got)
	}
}

// visualGateMatrix is the shared escape-input battery for the non-UI suppression. provablyNonUI
// MUST be true ONLY for pathsets that are exhaustively backend; every UI / ambiguous / garbage /
// empty case MUST be false (gate), because the visual-acceptance gate is false-negative-critical
// (a UI candidate slipping through unreviewed is the harm). The IDENTICAL matrix is replicated in
// the storage twin test (internal/storage/sqlite/visual_acceptance_required_reasons_test.go) as a
// behavioral drift-guard: if either twin's classification diverges, one module's test fails.
var visualGateMatrix = []struct {
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

func TestVisualGateProvablyNonUIPolarity(t *testing.T) {
	// Allow-list polarity (agent twin): suppress ONLY provably-backend pathsets; gate everything
	// else. This asserts the predicate directly against the empirical escape battery.
	for _, tc := range visualGateMatrix {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := pathsetIsProvablyNonUI(ProjectPatchQueueItemRecord{Pathset: tc.pathset}); got != tc.provablyNonUI {
				t.Fatalf("pathsetIsProvablyNonUI(%v) = %v, want %v", tc.pathset, got, tc.provablyNonUI)
			}
		})
	}
}

func TestVisualGateEndToEndAdmission(t *testing.T) {
	const uiProse = "react frontend visual layout screenshot canvas"

	// Every UI/ambiguous escape, under explicit UI prose, MUST still require visual acceptance
	// (>=1 signal). This is the admission-preservation guarantee the deny-list version violated.
	for _, tc := range visualGateMatrix {
		if tc.provablyNonUI {
			continue
		}
		if len(tc.pathset) == 0 {
			continue // empty pathset has no concrete scope; covered by the predicate test
		}
		got := uiFacingSignalsForPatchQueueItem(nil, ProjectPatchQueueItemRecord{Pathset: tc.pathset}, uiProse)
		if len(got) == 0 {
			t.Fatalf("%s: UI/ambiguous pathset %v under UI prose must require visual acceptance, got none", tc.name, tc.pathset)
		}
	}

	// The Lua lexer lane, even under the same UI prose, MUST be suppressed (provably non-UI).
	lua := ProjectPatchQueueItemRecord{Pathset: []string{"internal/lexer/**", "internal/token/**", "internal/tokens/**", "testdata/lexer/**"}}
	if got := uiFacingSignalsForPatchQueueItem(nil, lua, "token layout and visual structure documented; go test ./... green"); len(got) != 0 {
		t.Fatalf("provably non-UI lexer lane must not require visual acceptance, got %v", got)
	}

	// An explicit core-only declaration still suppresses (the orthogonal coreOnly path is intact).
	core := ProjectPatchQueueItemRecord{Pathset: []string{"src/**"}}
	if got := uiFacingSignalsForPatchQueueItem(nil, core, "core-only library slice, no browser surface; refined visual layout"); len(got) != 0 {
		t.Fatalf("explicit core-only declaration must suppress, got %v", got)
	}
}

func TestVisualBlockingSignalsRespectCommonNegations(t *testing.T) {
	text := "checks: not clipped, not unreadable, no text overlap, no broken layout observed, not unresponsive."
	if got := visualBlockingSignals(text); len(got) != 0 {
		t.Fatalf("expected common negations not to become blockers, got %v", got)
	}
}

func TestVisualEvidenceRequiresConcreteProductIntent(t *testing.T) {
	generic := strings.ReplaceAll(completeStructuredVisualPacket(), "  acceptance_criteria: AC-13, EV-01\n", "  acceptance_criteria: reviewed\n")
	generic = strings.ReplaceAll(generic, "  core_user_promise: import a local SVG set, review the first screen, select icons, and export a deterministic sprite bundle.\n", "")
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: generic}}

	ok, _, missing, _ := visualAcceptanceEvidenceSatisfied(docs)
	if ok {
		t.Fatalf("generic product-intent prose should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(missing, "\n"), []string{"acceptance criteria or core user promise exercised"}) {
		t.Fatalf("expected product intent missing requirement, got %v", missing)
	}
}

func TestVisualAcceptanceEvidenceForPatchQueueItemAcceptsRealScreenshots(t *testing.T) {
	packet := completeStructuredVisualPacketWithRealScreenshots(t)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, evidence, missing, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if !ok {
		t.Fatalf("expected real screenshot packet to satisfy visual acceptance, evidence=%v missing=%v blocking=%v", evidence, missing, blocking)
	}
}

func TestVisualAcceptanceEvidenceForPatchQueueItemAcceptsFileURLScreenshots(t *testing.T) {
	dir := t.TempDir()
	packet := completeStructuredVisualPacketWithScreenshotRefs(
		visualAcceptanceTestFileURL(writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "initial empty desktop.png"), false)),
		visualAcceptanceTestFileURL(writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "mobile initial empty.png"), false)),
		visualAcceptanceTestFileURL(writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "primary upload desktop.png"), false)),
		visualAcceptanceTestFileURL(writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "result export desktop.png"), false)),
	)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, evidence, missing, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if !ok {
		t.Fatalf("expected file:// screenshot refs to satisfy visual acceptance, evidence=%v missing=%v blocking=%v", evidence, missing, blocking)
	}
}

func TestVisualAcceptanceFileURLParsingCoversWindowsDriveAndUnixAbsolutePaths(t *testing.T) {
	windowsPath, ok := visualAcceptanceLocalPathFromFileURL("file:///C:/fixtures/shot%201.png")
	if !ok {
		t.Fatal("expected Windows file URL to parse")
	}
	wantWindows := filepath.Clean(filepath.FromSlash("C:/fixtures/shot 1.png"))
	if windowsPath != wantWindows {
		t.Fatalf("Windows file URL parsed as %q, want %q", windowsPath, wantWindows)
	}

	unixPath, ok := visualAcceptanceLocalPathFromFileURL("file:///tmp/rhizome-shot.png")
	if !ok {
		t.Fatal("expected Unix file URL to parse")
	}
	wantUnix := filepath.Clean(filepath.FromSlash("/tmp/rhizome-shot.png"))
	if unixPath != wantUnix {
		t.Fatalf("Unix file URL parsed as %q, want %q", unixPath, wantUnix)
	}
}

func TestVisualAcceptanceEvidenceForPatchQueueItemRejectsMissingScreenshotFile(t *testing.T) {
	packet := completeStructuredVisualPacketWithScreenshotRefs(
		filepath.Join(t.TempDir(), "missing-initial.png"),
		filepath.Join(t.TempDir(), "missing-mobile.png"),
		filepath.Join(t.TempDir(), "missing-primary.png"),
		filepath.Join(t.TempDir(), "missing-result.png"),
	)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, missing, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if ok {
		t.Fatalf("missing screenshot files should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(missing, "\n"), []string{"local screenshot artifact missing"}) {
		t.Fatalf("expected missing local screenshot artifact, got missing=%v blocking=%v", missing, blocking)
	}
}

func TestVisualAcceptanceEvidenceForPatchQueueItemRejectsBlankScreenshotFile(t *testing.T) {
	dir := t.TempDir()
	packet := completeStructuredVisualPacketWithScreenshotRefs(
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "initial-empty-desktop.png"), true),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "mobile-initial-empty.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "primary-upload-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "result-export-desktop.png"), false),
	)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, missing, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if ok {
		t.Fatalf("blank screenshot file should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(blocking, "\n"), []string{"screenshot artifact blank/uniform"}) {
		t.Fatalf("expected blank screenshot blocker, got missing=%v blocking=%v", missing, blocking)
	}
}

func TestVisualAcceptanceEvidenceForPatchQueueItemRejectsUndecodableScreenshotFile(t *testing.T) {
	dir := t.TempDir()
	initial := filepath.Join(dir, "initial-empty-desktop.png")
	if err := os.WriteFile(initial, []byte("not a png"), 0o600); err != nil {
		t.Fatal(err)
	}
	packet := completeStructuredVisualPacketWithScreenshotRefs(
		initial,
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "mobile-initial-empty.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "primary-upload-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "result-export-desktop.png"), false),
	)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, missing, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if ok {
		t.Fatalf("undecodable screenshot file should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(blocking, "\n"), []string{"screenshot artifact undecodable"}) {
		t.Fatalf("expected undecodable screenshot blocker, got missing=%v blocking=%v", missing, blocking)
	}
}

func TestVisualAcceptanceEvidenceForPatchQueueItemRejectsLowContrastFirstViewportScreenshot(t *testing.T) {
	dir := t.TempDir()
	packet := completeStructuredVisualPacketWithScreenshotRefs(
		writeVisualAcceptanceLowContrastHeroPNG(t, filepath.Join(dir, "initial-empty-desktop.png")),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "mobile-initial-empty.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "primary-upload-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "result-export-desktop.png"), false),
	)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, missing, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if ok {
		t.Fatalf("low-contrast first viewport should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(blocking, "\n"), []string{"screenshot first viewport low-contrast composition"}) {
		t.Fatalf("expected low-contrast first viewport blocker, got missing=%v blocking=%v", missing, blocking)
	}
}

func TestVisualAcceptanceEvidenceForPatchQueueItemAllowsSparseHighContrastFirstViewport(t *testing.T) {
	dir := t.TempDir()
	packet := completeStructuredVisualPacketWithScreenshotRefs(
		writeVisualAcceptanceSparseHighContrastPNG(t, filepath.Join(dir, "initial-empty-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "mobile-initial-empty.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "primary-upload-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "result-export-desktop.png"), false),
	)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, evidence, missing, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if !ok {
		t.Fatalf("sparse high-contrast first viewport should pass, evidence=%v missing=%v blocking=%v", evidence, missing, blocking)
	}
}

func TestVisualAcceptanceEvidenceForPatchQueueItemAllowsSparseDarkModeFirstViewport(t *testing.T) {
	dir := t.TempDir()
	packet := completeStructuredVisualPacketWithScreenshotRefs(
		writeVisualAcceptanceSparseHighContrastPNG(t, filepath.Join(dir, "initial-empty-desktop.png"), true),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "mobile-initial-empty.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "primary-upload-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "result-export-desktop.png"), false),
	)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, evidence, missing, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if !ok {
		t.Fatalf("sparse dark-mode first viewport should pass, evidence=%v missing=%v blocking=%v", evidence, missing, blocking)
	}
}

func TestVisualAcceptanceEvidenceBlocksHighLayoutRiskProbeResult(t *testing.T) {
	packet := completeStructuredVisualPacketWithRealScreenshots(t) + "\n" + strings.Join([]string{
		"browser_visual_probe_result_v1:",
		`  layout_risk: {"risk_level":"high","risk_score":70,"risk_signals":["oversized_text","horizontal_overflow_risk"]}`,
	}, "\n")
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, _, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if ok {
		t.Fatalf("high layout risk probe result should block visual acceptance")
	}
	for _, want := range []string{"layout risk", "oversized typography"} {
		if !containsAnySignal(strings.Join(blocking, "\n"), []string{want}) {
			t.Fatalf("expected blocking signal %q, got %v", want, blocking)
		}
	}
}

func TestVisualAcceptanceEvidenceBlocksPrimarySurfaceProbeWarnings(t *testing.T) {
	packet := completeStructuredVisualPacketWithRealScreenshots(t) + "\n" + strings.Join([]string{
		"browser_visual_probe_result_v1:",
		`  visual_quality_status: needs_semantic_review`,
		`  primary_surface_analysis: {"surface_kind":"board/grid/game-surface","risk_level":"high","risk_signals":["board_cells_without_visible_css","game_board_likely_line_wrapped"]}`,
	}, "\n")
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, _, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if ok {
		t.Fatalf("primary-surface probe warning should block visual acceptance")
	}
	if !containsAnySignal(strings.Join(blocking, "\n"), []string{"primary surface risk"}) {
		t.Fatalf("expected primary surface risk blocker, got %v", blocking)
	}
}

func TestVisualAcceptanceEvidenceBlocksDirtyCheckoutPass(t *testing.T) {
	packet := completeStructuredVisualPacketWithRealScreenshots(t) + "\n" + strings.Join([]string{
		"provenance_integrity:",
		"  dirty checkout: true",
		"  note: browser probe passed only after uncommitted changes in the local repair checkout",
	}, "\n")
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, _, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if ok {
		t.Fatalf("dirty checkout visual pass should not satisfy visual acceptance")
	}
	if !containsAnySignal(strings.Join(blocking, "\n"), []string{"non-durable dirty evidence"}) {
		t.Fatalf("expected dirty evidence blocker, got %v", blocking)
	}
}

func TestVisualAcceptanceEvidenceBlocksProvisionalNonCanonicalPass(t *testing.T) {
	packet := strings.ReplaceAll(
		completeStructuredVisualPacketWithRealScreenshots(t),
		"schema: rhizome_visual_acceptance_v1",
		"schema: rhizome_visual_acceptance_v1\ncandidate_status: provisional_non_canonical_review_target",
	)
	docs := []WorkspaceDocRecord{{DocKey: "project.visual_acceptance", Content: packet}}

	ok, _, _, blocking := visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, visualAcceptanceTestPatchQueueItem())
	if ok {
		t.Fatalf("provisional non-canonical visual pass should not satisfy candidate acceptance")
	}
	if !containsAnySignal(strings.Join(blocking, "\n"), []string{"non-durable dirty evidence"}) {
		t.Fatalf("expected non-durable evidence blocker, got %v", blocking)
	}
}

func completeStructuredVisualPacket() string {
	return completeStructuredVisualPacketWithScreenshotRefs(
		"artifacts/initial-empty-desktop.png",
		"artifacts/mobile-initial-empty.png",
		"artifacts/primary-upload-desktop.png",
		"artifacts/result-export-desktop.png",
	)
}

func completeStructuredVisualPacketWithRealScreenshots(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return completeStructuredVisualPacketWithScreenshotRefs(
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "initial-empty-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "mobile-initial-empty.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "primary-upload-desktop.png"), false),
		writeVisualAcceptanceTestPNG(t, filepath.Join(dir, "result-export-desktop.png"), false),
	)
}

func completeStructuredVisualPacketWithScreenshotRefs(initial, mobile, primary, result string) string {
	return strings.Join([]string{
		"schema: rhizome_visual_acceptance_v1",
		"visual_verdict: pass",
		"product_intent:",
		"  acceptance_criteria: AC-13, EV-01",
		"  core_user_promise: import a local SVG set, review the first screen, select icons, and export a deterministic sprite bundle.",
		"provenance:",
		"  branch_id: branch-theta",
		"  head_sha: b7f175bf9b8d027163f730e244f2ce2c8f186313",
		"  observed_url: http://127.0.0.1:59432/",
		"  validation_checkout: project-checkouts/icon-sprite-forge-visual-acceptance",
		"  command: npm.cmd run dev -- --host 127.0.0.1 --port 59432 --strictPort",
		"viewport_matrix:",
		"  desktop: 1440x900",
		"  mobile: 390x844",
		"state_evidence:",
		"  initial_state:",
		"    screenshot_ref: " + initial,
		"    judgment: first viewport empty state is readable, not dominated by giant pale text, and primary action is visible.",
		"  mobile_state: screenshot_ref " + mobile,
		"  primary_flow:",
		"    screenshot_ref: " + primary,
		"    scenario: primary flow imports local icons through the file picker and keeps controls discoverable.",
		"  result_state:",
		"    screenshot_ref: " + result,
		"    scenario: output state shows populated previews and export/download controls.",
		"checks:",
		"  overlap: pass",
		"  clipping: pass",
		"  contrast/readability: pass",
		"  responsive fit: pass",
		"  typography/hierarchy/spacing: pass",
		"  primary surface geometry/density: pass",
		"  visible modes/presets/difficulties: not applicable for this icon-sprite workflow",
		"  real-user usability: pass",
		"layout_risk:",
		"  source: browser_visual_probe_result_v1",
		"  risk_level: low",
		"  risk_score: 0",
		"  risk_signals: none",
	}, "\n")
}

func visualAcceptanceTestPatchQueueItem() ProjectPatchQueueItemRecord {
	return ProjectPatchQueueItemRecord{
		QueueID:  "queue-theta",
		ItemID:   "item-theta",
		BranchID: "branch-theta",
		HeadSHA:  "b7f175bf9b8d027163f730e244f2ce2c8f186313",
	}
}

func writeVisualAcceptanceTestPNG(t *testing.T, path string, blank bool) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 144, 96))
	for y := 0; y < 96; y++ {
		for x := 0; x < 144; x++ {
			img.Set(x, y, color.RGBA{R: 246, G: 245, B: 239, A: 255})
		}
	}
	if !blank {
		for y := 12; y < 28; y++ {
			for x := 12; x < 132; x++ {
				img.Set(x, y, color.RGBA{R: 24, G: 32, B: 28, A: 255})
			}
		}
		for y := 38; y < 84; y++ {
			for x := 18; x < 62; x++ {
				img.Set(x, y, color.RGBA{R: 42, G: 115, B: 184, A: 255})
			}
			for x := 74; x < 126; x++ {
				img.Set(x, y, color.RGBA{R: 204, G: 96, B: 62, A: 255})
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := png.Encode(f, img); err != nil {
		t.Fatal(err)
	}
	return path
}

func visualAcceptanceTestFileURL(path string) string {
	slash := filepath.ToSlash(path)
	if volume := filepath.VolumeName(path); volume != "" && !strings.HasPrefix(slash, "/") {
		slash = "/" + slash
	}
	return (&url.URL{Scheme: "file", Path: slash}).String()
}

func writeVisualAcceptanceSparseHighContrastPNG(t *testing.T, path string, dark bool) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 180, 120))
	bg := color.RGBA{R: 248, G: 248, B: 244, A: 255}
	ink := color.RGBA{R: 28, G: 33, B: 30, A: 255}
	accent := color.RGBA{R: 32, G: 110, B: 210, A: 255}
	if dark {
		bg = color.RGBA{R: 18, G: 22, B: 26, A: 255}
		ink = color.RGBA{R: 238, G: 242, B: 246, A: 255}
		accent = color.RGBA{R: 84, G: 170, B: 255, A: 255}
	}
	for y := 0; y < 120; y++ {
		for x := 0; x < 180; x++ {
			img.Set(x, y, bg)
		}
	}
	for y := 30; y < 38; y++ {
		for x := 50; x < 130; x++ {
			img.Set(x, y, ink)
		}
	}
	for y := 50; y < 62; y++ {
		for x := 44; x < 136; x++ {
			img.Set(x, y, ink)
		}
	}
	for y := 74; y < 94; y++ {
		for x := 62; x < 118; x++ {
			img.Set(x, y, accent)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := png.Encode(f, img); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeVisualAcceptanceLowContrastHeroPNG(t *testing.T, path string) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 180, 120))
	background := color.RGBA{R: 246, G: 245, B: 239, A: 255}
	paleInk := color.RGBA{R: 230, G: 230, B: 226, A: 255}
	darkInk := color.RGBA{R: 42, G: 35, B: 29, A: 255}
	for y := 0; y < 120; y++ {
		for x := 0; x < 180; x++ {
			img.Set(x, y, background)
		}
	}
	for y := 8; y < 78; y++ {
		for x := 12; x < 168; x++ {
			if (x+y)%9 < 5 {
				img.Set(x, y, paleInk)
			}
		}
	}
	for y := 94; y < 104; y++ {
		for x := 70; x < 110; x++ {
			img.Set(x, y, darkInk)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := png.Encode(f, img); err != nil {
		t.Fatal(err)
	}
	return path
}
