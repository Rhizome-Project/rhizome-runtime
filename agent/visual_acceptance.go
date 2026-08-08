package main

import (
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// visualAcceptanceCriteriaIDPattern matches acceptance-criteria IDs of the operator/spec shape
// PREFIX-<WORD>-<NUM> (e.g. AC-LEX-01) as well as the bare PREFIX-<NUM> shape. The previous
// `PREFIX-\d+...` form did NOT match AC-LEX-01 (a letter, not a digit, follows the prefix dash).
// CANONICAL definition: rhizome/internal/acspec.IDPattern. agent is a separate Go module and
// cannot import that package without coupling the bot binary to the root module's dependency
// graph, so this copy MUST be kept byte-identical to acspec.IDPattern.
var visualAcceptanceCriteriaIDPattern = regexp.MustCompile(`(?i)\b(?:ac|ev|ux|ui|qa|mvp|fr|cr)-[a-z0-9]+(?:-[a-z0-9]+)*\b`)
var visualAcceptanceFileImageURLPattern = regexp.MustCompile("(?i)\\bfile://[^\\s\"'`,;<>{}\\[\\]\\(\\)|]+?\\.(?:png|jpe?g|webp|bmp|gif)(?:\\?[^\\s\"'`,;<>{}\\[\\]\\(\\)|]*)?")
var visualAcceptanceLocalImagePathPattern = regexp.MustCompile(`(?i)(?:@fs[\\/])?(?:\\\\\?\\[A-Za-z]:|[A-Za-z]:|\\\\[^\\/:*?"<>|\r\n]+\\[^\\/:*?"<>|\r\n]+|/(?:users|home|tmp|var|private))[/\\][^<>"'\r\n]*?\.(?:png|jpe?g|webp|bmp|gif)`)

type visualAcceptanceGateResult struct {
	Required        bool
	Satisfied       bool
	Reasons         []string
	EvidenceDocKeys []string
	Missing         []string
	BlockingSignals []string
}

func visualAcceptanceGateForPatchQueueItem(docs []WorkspaceDocRecord, item ProjectPatchQueueItemRecord, decisionSummary string) visualAcceptanceGateResult {
	result := visualAcceptanceGateResult{}
	result.Reasons = uiFacingSignalsForPatchQueueItem(docs, item, decisionSummary)
	if len(result.Reasons) == 0 {
		return result
	}
	result.Required = true
	result.Satisfied, result.EvidenceDocKeys, result.Missing, result.BlockingSignals = visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs, item)
	return result
}

func uiFacingSignalsForPatchQueueItem(docs []WorkspaceDocRecord, item ProjectPatchQueueItemRecord, decisionSummary string) []string {
	signals := make([]string, 0, 4)
	hasPackageJSON := false
	hasGenericSrcScope := false
	hasViteConfig := false
	for _, p := range patchQueueItemPathset(item) {
		normalized := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(p, "\\", "/")))
		if normalized == "" {
			continue
		}
		switch {
		case normalized == "index.html":
			signals = append(signals, "path:index.html")
		case strings.HasPrefix(normalized, "public/") || normalized == "public/**":
			signals = append(signals, "path:public")
		case strings.HasPrefix(normalized, "web/") || normalized == "web/**":
			signals = append(signals, "path:web")
		case strings.HasSuffix(normalized, ".vue") || strings.HasSuffix(normalized, ".svelte"):
			signals = append(signals, "path:ui-component")
		case strings.Contains(normalized, "src/app") || strings.Contains(normalized, "src/components") || strings.Contains(normalized, "src/pages"):
			signals = append(signals, "path:react-layout")
		case normalized == "src" || normalized == "src/**" || strings.HasPrefix(normalized, "src/"):
			hasGenericSrcScope = true
		case normalized == "app" || normalized == "app/**" || strings.HasPrefix(normalized, "app/"):
			signals = append(signals, "path:app-ui-scope")
		case normalized == "components" || normalized == "components/**" || strings.HasPrefix(normalized, "components/"):
			signals = append(signals, "path:components-ui-scope")
		case normalized == "pages" || normalized == "pages/**" || strings.HasPrefix(normalized, "pages/"):
			signals = append(signals, "path:pages-ui-scope")
		case normalized == "ui" || normalized == "ui/**" || strings.HasPrefix(normalized, "ui/"):
			signals = append(signals, "path:ui-scope")
		case normalized == "static" || normalized == "static/**" || strings.HasPrefix(normalized, "static/"):
			signals = append(signals, "path:static-ui-asset")
		case normalized == "assets" || normalized == "assets/**" || strings.HasPrefix(normalized, "assets/"):
			signals = append(signals, "path:assets-ui-asset")
		case normalized == "styles" || normalized == "styles/**" || strings.HasPrefix(normalized, "styles/"):
			signals = append(signals, "path:styles-ui-scope")
		case strings.HasSuffix(normalized, ".tsx") || strings.HasSuffix(normalized, ".jsx") || strings.HasSuffix(normalized, ".css") || strings.HasSuffix(normalized, ".scss"):
			signals = append(signals, "path:ui-asset")
		case strings.Contains(normalized, "vite.config"):
			hasViteConfig = true
		case strings.Contains(normalized, "next.config"):
			signals = append(signals, "path:next")
		case strings.Contains(normalized, "tailwind.config"):
			signals = append(signals, "path:tailwind")
		case normalized == "package.json":
			hasPackageJSON = true
		}
	}
	text := strings.ToLower(strings.Join([]string{decisionSummary, workspaceDocsText(docs)}, "\n"))
	coreOnly := visualAcceptanceCoreOnlySignals(text)
	// provablyNonUI suppresses the standalone doc-text signals ONLY for a pathset that is PROVABLY
	// non-UI (ALLOW-LIST polarity). The visual-acceptance gate is false-negative-critical (a UI
	// candidate slipping through unreviewed is the harm), so the default must be GATE: anything not
	// provably backend - unknown extension, ambiguous src/, garbage, empty - keeps the doc-text
	// signals live. Path-level signals and the path-correlated text branches are untouched. Keep
	// byte-aligned with the storage twin projectPatchQueueAcceptedVisualAcceptancePathsetIsProvablyNonUI.
	provablyNonUI := pathsetIsProvablyNonUI(item)
	uiTextSignals := containsAnySignal(text, []string{"frontend", "front-end", "web app", "browser app", "react", "next.js", "nextjs", "tsx", "jsx"})
	if !coreOnly && hasGenericSrcScope && uiTextSignals {
		signals = append(signals, "path:src-ui-scope")
	}
	if !coreOnly && hasViteConfig && uiTextSignals {
		signals = append(signals, "path:vite")
	}
	if !coreOnly && hasPackageJSON && containsAnySignal(text, []string{"frontend", "front-end", "web app", "browser app", "react", "vite", "next.js", "nextjs", "tsx", "jsx"}) {
		signals = append(signals, "path:package-ui")
	}
	for _, candidate := range []struct {
		label   string
		needles []string
	}{
		{label: "doc:frontend", needles: []string{"frontend", "front-end", "web app", "browser app"}},
		{label: "doc:react", needles: []string{"react", "vite", "next.js", "nextjs", "tsx", "jsx"}},
		{label: "doc:visual", needles: []string{"ui/ux", "visual", "layout", "screenshot", "viewport", "canvas"}},
	} {
		if !coreOnly && !provablyNonUI && containsAnySignal(text, candidate.needles) {
			signals = append(signals, candidate.label)
		}
	}
	return uniqueTrimmedCSVStrings(signals)
}

// --- Visual-acceptance non-UI ALLOW-LIST (keep byte-aligned with the storage twin in
// internal/storage/sqlite/projects_patch_queue.go). The gate is false-negative-critical, so the
// suppression is an allow-list: a path is suppressible ONLY if it carries no UI marker AND
// positively lives in backend territory. Unknown -> gate. ---

// (No UI-extension list by design: enumerating UI extensions is deny-list thinking, and an
// incomplete list is exactly how a UI surface leaks. The allow-list lists only BACKEND extensions;
// every other extension - known-UI, unknown, or glob-dressed - gates.)

// visualGateUIDirSegments: any path segment that conventionally holds frontend/UI assets, in both
// plural and singular forms. REJECT.
var visualGateUIDirSegments = map[string]bool{
	"public": true, "web": true, "frontend": true, "client": true, "dashboard": true,
	"src": true, "app": true, "ui": true,
	"components": true, "component": true, "pages": true, "page": true,
	"static": true, "assets": true, "styles": true,
	"layouts": true, "layout": true, "themes": true, "theme": true,
	"widgets": true, "widget": true, "screens": true, "screen": true,
	"views": true, "view": true, "templates": true, "template": true, "render": true,
}

// visualGateUIManifestMarkers: frontend build manifests / configs. REJECT (substring match).
var visualGateUIManifestMarkers = []string{
	"vite.config", "next.config", "tailwind.config", "postcss.config", "svelte.config",
	"webpack.config", "rollup.config", ".storybook", "package.json", "manifest.json", "sw.js",
}

// visualGateUIFilenameMarkers: substring markers that flag a file (commonly a .go file) that renders
// a browser/HTML/visual surface - which a file EXTENSION cannot distinguish from backend Go. Checked
// BEFORE the backend-extension ALLOW so a UI-rendering .go file gates. Pragmatic and admittedly
// INCOMPLETE (a novel-named Go-UI file still leaks; the robust closure is an explicit per-item
// renders-browser-UI flag, tracked as a follow-up) - but it covers the present-day live dashboard
// set (internal/server/dashboard.go, agent/web_dashboard_*.go). REJECT (substring match).
// NOTE: "render"/"template" are deliberately NOT substring-markers here - they over-match legitimate
// interpreter files (eval/render.go, template.go). They live in visualGateUIDirSegments instead, so
// they gate only as a whole path SEGMENT (a render/ or template/ DIR), never inside a filename.
var visualGateUIFilenameMarkers = []string{
	"dashboard", "web_", "_html", "html_", "webview", "_styles", "_script",
}

// visualGateBackendExtensions: unambiguously non-UI file extensions. ALLOW (positive match).
// .lua is the Lua campaign's own source/fixture extension - never a browser surface, and reachable
// on the milestone path (the seed ships tracked .lua smoke fixtures), so it must not over-gate.
var visualGateBackendExtensions = []string{
	".go", ".lua", ".md", ".sql", ".proto", ".sh", ".txt", ".rst",
}

// visualGateBackendFiles: exact non-UI repo files. ALLOW.
var visualGateBackendFiles = map[string]bool{
	"go.mod": true, "go.sum": true, "makefile": true, "dockerfile": true,
	".gitignore": true, ".dockerignore": true,
}

// visualGateBackendRoots: top-level directories that, by Go/backend convention, hold no browser UI.
// Deliberately EXCLUDES ambiguous roots (src, app, api, server, view) so they gate. ALLOW.
var visualGateBackendRoots = map[string]bool{
	"internal": true, "cmd": true, "pkg": true, "lib": true, "vendor": true,
	"testdata": true, "test": true, "tests": true, "docs": true, "doc": true,
	"scripts": true, "examples": true, "example": true, "tools": true,
	"migrations": true, "migration": true, "proto": true,
}

// pathsetIsProvablyNonUI reports true ONLY when the item declares a concrete pathset whose EVERY
// entry is PROVABLY non-UI. Allow-list by deliberate polarity: empty/garbage/unknown/ambiguous all
// return false (gate). Fail-safe by construction. Keep byte-aligned with the storage twin
// projectPatchQueueAcceptedVisualAcceptancePathsetIsProvablyNonUI.
func pathsetIsProvablyNonUI(item ProjectPatchQueueItemRecord) bool {
	cleaned := make([]string, 0, len(item.Pathset))
	for _, p := range item.Pathset {
		if t := strings.TrimSpace(p); t != "" {
			cleaned = append(cleaned, t)
		}
	}
	if len(cleaned) == 0 {
		return false // no concrete declared scope -> cannot prove non-UI -> gate
	}
	for _, p := range cleaned {
		if !pathIsProvablyNonUI(p) {
			return false // any entry not provably non-UI -> gate the whole candidate
		}
	}
	return true
}

// pathIsProvablyNonUI reports whether ONE pathset entry is provably backend. ALLOW-LIST polarity: it
// NEVER enumerates UI extensions (an incomplete UI list is how a deny-list leaks). A path is
// suppressed ONLY if it positively matches backend territory; every other extension - unknown, UI,
// or glob-dressed - gates. The leaf body (and its sole helper visualGateFileExtension) stays
// byte-identical to the storage twin projectPatchQueuePathIsProvablyNonUI.
func pathIsProvablyNonUI(p string) bool {
	n := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))), "./")
	if n == "" {
		return false
	}
	// REJECT overrides: a UI directory segment, a UI build manifest, or a UI-rendering filename
	// marker gates even an otherwise backend-looking (.go) path.
	for _, seg := range strings.Split(n, "/") {
		if visualGateUIDirSegments[seg] {
			return false
		}
	}
	for _, marker := range visualGateUIManifestMarkers {
		if strings.Contains(n, marker) {
			return false
		}
	}
	for _, marker := range visualGateUIFilenameMarkers {
		if strings.Contains(n, marker) {
			return false
		}
	}
	// An exact backend file (go.mod, go.sum, .gitignore) suppresses despite its dotted name.
	if visualGateBackendFiles[n] {
		return true
	}
	last := n
	if i := strings.LastIndex(n, "/"); i >= 0 {
		last = n[i+1:]
	}
	// ANY dot in the last segment marks a FILE-TARGETING path (concrete, glob, or dotfile). The exact
	// backend file was already matched above; here ONLY a recognized backend EXTENSION (a non-leading
	// dot, glob-aware) suppresses. Everything else gates and must NOT fall through to the directory
	// rescue: an unrecognized extension (theme.json), an unreadable one (page.html*, foo., *.*), and a
	// stem-less leading-dot leaf (.vue, a nested .gitignore). ONLY a dot-LESS last segment
	// (internal/lexer/**, a bare *, a plain dir name) reaches the backend-root rescue.
	if dot := strings.LastIndex(last, "."); dot >= 0 {
		if dot > 0 {
			ext := strings.TrimRight(last[dot:], "*?")
			if ext != "." {
				for _, e := range visualGateBackendExtensions {
					if ext == e {
						return true
					}
				}
			}
		}
		return false
	}
	first := n
	if i := strings.Index(n, "/"); i >= 0 {
		first = n[:i]
	}
	return visualGateBackendRoots[first]
}

func visualAcceptanceCoreOnlySignals(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	if containsAnySignal(text, []string{
		"core-only",
		"core only",
		"core slice",
		"non-ui",
		"non ui",
		"not ui-facing",
		"not ui facing",
		"not browser-facing",
		"not browser facing",
		"no browser/app surface",
		"no browser surface",
		"no app surface",
		"no ui surface",
		"library slice",
		"normalization/export core",
		"normalization and export core",
		"src/core",
		"tests/core",
	}) {
		return true
	}
	return false
}

func patchQueueItemPathset(item ProjectPatchQueueItemRecord) []string {
	if len(item.Pathset) > 0 {
		return uniqueTrimmedCSVStrings(item.Pathset)
	}
	raw := strings.TrimSpace(item.PathsetJSON)
	if raw == "" {
		return nil
	}
	var wrapped struct {
		Paths []string `json:"paths"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapped); err == nil && len(wrapped.Paths) > 0 {
		return uniqueTrimmedCSVStrings(wrapped.Paths)
	}
	var direct []string
	if err := json.Unmarshal([]byte(raw), &direct); err == nil && len(direct) > 0 {
		return uniqueTrimmedCSVStrings(direct)
	}
	return nil
}

func visualAcceptanceEvidenceSatisfied(docs []WorkspaceDocRecord) (bool, []string, []string, []string) {
	return visualAcceptanceEvidenceSatisfiedForCandidateWithOptions(docs, ProjectPatchQueueItemRecord{}, visualAcceptanceEvidenceOptions{})
}

func visualAcceptanceEvidenceSatisfiedForPatchQueueItem(docs []WorkspaceDocRecord, item ProjectPatchQueueItemRecord) (bool, []string, []string, []string) {
	return visualAcceptanceEvidenceSatisfiedForCandidateWithOptions(docs, item, visualAcceptanceEvidenceOptions{VerifyScreenshotArtifacts: true})
}

func visualAcceptanceEvidenceSatisfiedForCandidate(docs []WorkspaceDocRecord, item ProjectPatchQueueItemRecord) (bool, []string, []string, []string) {
	return visualAcceptanceEvidenceSatisfiedForCandidateWithOptions(docs, item, visualAcceptanceEvidenceOptions{})
}

type visualAcceptanceEvidenceOptions struct {
	VerifyScreenshotArtifacts bool
}

func visualAcceptanceEvidenceSatisfiedForCandidateWithOptions(docs []WorkspaceDocRecord, item ProjectPatchQueueItemRecord, options visualAcceptanceEvidenceOptions) (bool, []string, []string, []string) {
	evidenceDocKeys := make([]string, 0, len(docs))
	missing := []string{}
	blocking := []string{}
	for _, doc := range sortedVisualAcceptanceDocs(docs) {
		rawText := strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n")
		text := strings.ToLower(rawText)
		if strings.TrimSpace(text) == "" {
			continue
		}
		if visualAcceptanceDocIsInlineOnly(doc) {
			blocking = append(blocking, visualBlockingSignals(text)...)
			continue
		}
		if !docLooksLikeVisualAcceptancePacket(text) {
			continue
		}
		evidenceDocKeys = append(evidenceDocKeys, doc.DocKey)
		docBlocking := visualBlockingSignals(text)
		docMissing := visualAcceptanceMissingRequirements(text)
		docMissing = append(docMissing, visualAcceptanceCandidateMissingRequirements(text, item)...)
		if options.VerifyScreenshotArtifacts {
			pixelMissing, pixelBlocking := visualAcceptanceArtifactPixelIssues(rawText, item)
			docMissing = append(docMissing, pixelMissing...)
			docBlocking = append(docBlocking, pixelBlocking...)
		}
		blocking = append(blocking, docBlocking...)
		missing = append(missing, docMissing...)
		if len(docMissing) == 0 && len(blocking) == 0 {
			return true, evidenceDocKeys, nil, nil
		}
	}
	if len(evidenceDocKeys) == 0 {
		missing = append(missing, "workspace doc containing rhizome_visual_acceptance_v1 plus screenshot, viewport, scenario, and visual check evidence")
	}
	return false, evidenceDocKeys, uniqueTrimmedCSVStrings(missing), uniqueTrimmedCSVStrings(blocking)
}

func visualAcceptanceDocIsInlineOnly(doc WorkspaceDocRecord) bool {
	return strings.EqualFold(strings.TrimSpace(doc.DocKey), "inline.decision_summary")
}

func sortedVisualAcceptanceDocs(docs []WorkspaceDocRecord) []WorkspaceDocRecord {
	out := append([]WorkspaceDocRecord(nil), docs...)
	sort.SliceStable(out, func(i, j int) bool {
		left := strings.TrimSpace(out[i].UpdatedAt)
		right := strings.TrimSpace(out[j].UpdatedAt)
		if left != "" || right != "" {
			if left != right {
				return left > right
			}
		}
		leftKey := strings.TrimSpace(out[i].DocKey)
		rightKey := strings.TrimSpace(out[j].DocKey)
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		return strings.TrimSpace(out[i].Title) < strings.TrimSpace(out[j].Title)
	})
	return out
}

func docLooksLikeVisualAcceptancePacket(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	if strings.Contains(text, "rhizome_visual_acceptance_v1") || strings.Contains(text, "visual_acceptance") || strings.Contains(text, "visual acceptance") {
		return true
	}
	return containsAnySignal(text, []string{"screenshot", "screenshots", "screenshot_path", "screenshot_ref", "screenshot refs"}) &&
		containsAnySignal(text, []string{"viewport", "viewports", "desktop", "mobile", "narrow"}) &&
		containsAnySignal(text, []string{"scenario", "user scenario", "real user"}) &&
		containsAnySignal(text, []string{"overlap", "clipping", "contrast", "readability", "layout"})
}

func visualAcceptanceMissingRequirements(text string) []string {
	text = strings.ToLower(strings.TrimSpace(text))
	missing := []string{}
	if !strings.Contains(text, "rhizome_visual_acceptance_v1") {
		missing = append(missing, "schema: rhizome_visual_acceptance_v1")
	}
	if !visualEvidenceHasPassVerdict(text) {
		missing = append(missing, "explicit visual_verdict: pass")
	}
	stateRefs := visualStateScreenshotRefs(text)
	if len(stateRefs.initial) == 0 {
		missing = append(missing, "initial_state screenshot ref/path")
	}
	if len(stateRefs.primary) == 0 {
		missing = append(missing, "primary_flow screenshot ref/path")
	}
	if len(stateRefs.result) == 0 && !visualResultStateNotApplicable(text) {
		missing = append(missing, "result_state screenshot ref/path or explicit not-applicable note")
	}
	if !visualStateScreenshotRefsAreDistinct(stateRefs) {
		missing = append(missing, "distinct screenshot refs per required visual state")
	}
	if !visualEvidenceHasNarrowScreenshotRef(text) {
		missing = append(missing, "narrow/mobile screenshot ref")
	}
	if !mentionsAny(text, []string{"viewport_matrix", "viewport matrix", "viewport"}) || !mentionsAny(text, []string{"desktop", "wide viewport", "1440", "1365", "1280"}) || !mentionsAny(text, []string{"mobile", "narrow", "390", "375", "small viewport"}) {
		missing = append(missing, "viewport matrix covering desktop and narrow/mobile")
	}
	if !visualEvidenceHasProvenance(text) {
		missing = append(missing, "branch/head/url/checkout provenance")
	}
	if !visualEvidenceCoversProductIntent(text) {
		missing = append(missing, "acceptance criteria or core user promise exercised")
	}
	if !visualEvidenceCoversInitialState(text) {
		missing = append(missing, "initial empty/first viewport state evidence")
	}
	if !visualEvidenceCoversPrimaryPath(text) {
		missing = append(missing, "primary user path evidence")
	}
	if !visualEvidenceCoversResultState(text) {
		missing = append(missing, "post-action/output/result state evidence or explicit not-applicable note")
	}
	for _, requirement := range visualEvidenceMissingCheckRequirements(text) {
		missing = append(missing, requirement)
	}
	if !visualEvidenceMentionsLayoutRisk(text) {
		missing = append(missing, "browser/layout risk metrics from screenshot probe or explicit no-risk note")
	}
	return uniqueTrimmedCSVStrings(missing)
}

func visualAcceptanceCandidateMissingRequirements(text string, item ProjectPatchQueueItemRecord) []string {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return nil
	}
	missing := []string{}
	branchID := strings.ToLower(strings.TrimSpace(item.BranchID))
	queueID := strings.ToLower(strings.TrimSpace(item.QueueID))
	itemID := strings.ToLower(strings.TrimSpace(item.ItemID))
	headSHA := strings.ToLower(strings.TrimSpace(item.HeadSHA))
	hasCandidateSelector := branchID != "" || queueID != "" || itemID != ""
	if hasCandidateSelector {
		branchMatches := branchID != "" && strings.Contains(text, branchID)
		queueItemMatches := queueID != "" && itemID != "" && strings.Contains(text, queueID) && strings.Contains(text, itemID)
		itemMatches := branchID == "" && itemID != "" && strings.Contains(text, itemID)
		if !branchMatches && !queueItemMatches && !itemMatches {
			missing = append(missing, "visual packet candidate provenance matching branch_id or queue_id/item_id")
		}
	}
	if headSHA != "" {
		headMatches := strings.Contains(text, headSHA)
		if !headMatches && len(headSHA) >= 12 {
			headMatches = strings.Contains(text, headSHA[:12])
		}
		if !headMatches {
			missing = append(missing, "visual packet head_sha matching candidate")
		}
	}
	return missing
}

func visualAcceptanceArtifactPixelIssues(text string, item ProjectPatchQueueItemRecord) ([]string, []string) {
	refs := screenshotRefsInText(text)
	if len(refs) == 0 {
		return nil, nil
	}
	missing := []string{}
	blocking := []string{}
	roots := visualAcceptanceArtifactSearchRoots(text, item)
	initialRefs := visualAcceptanceRefSet(visualStateScreenshotRefs(strings.ToLower(text)).initial)
	for _, ref := range refs {
		localPath, status := resolveVisualAcceptanceScreenshotRef(ref, roots)
		switch status {
		case "unsupported":
			missing = append(missing, "locally decodable screenshot artifact for "+ref)
			continue
		case "missing":
			missing = append(missing, "local screenshot artifact missing: "+ref)
			continue
		}
		findings := inspectVisualAcceptanceScreenshot(localPath, ref, initialRefs[strings.ToLower(strings.TrimSpace(ref))])
		blocking = append(blocking, findings...)
	}
	return uniqueTrimmedCSVStrings(missing), uniqueTrimmedCSVStrings(blocking)
}

func visualAcceptanceRefSet(refs []string) map[string]bool {
	out := map[string]bool{}
	for _, ref := range refs {
		ref = strings.ToLower(strings.TrimSpace(ref))
		if ref != "" {
			out[ref] = true
		}
	}
	return out
}

func visualAcceptanceArtifactSearchRoots(text string, item ProjectPatchQueueItemRecord) []string {
	roots := []string{}
	if root := strings.TrimSpace(item.RepoRoot); root != "" {
		roots = append(roots, root)
	}
	for _, root := range visualAcceptanceCheckoutRoots(text) {
		roots = append(roots, root)
	}
	cwd, err := os.Getwd()
	if err == nil && strings.TrimSpace(cwd) != "" {
		roots = append(roots, cwd)
		if parent := filepath.Dir(cwd); parent != "" && parent != "." && parent != cwd {
			roots = append(roots, parent)
		}
	}
	out := []string{}
	for _, root := range uniqueTrimmedCSVStrings(roots) {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		root = strings.Trim(root, "\"'`")
		if !filepath.IsAbs(root) {
			if abs, err := filepath.Abs(root); err == nil {
				root = abs
			}
		}
		out = append(out, root)
	}
	return uniqueTrimmedCSVStrings(out)
}

func visualAcceptanceCheckoutRoots(text string) []string {
	pattern := regexp.MustCompile(`(?im)\b(?:validation_checkout|checkout_path|checkout|local_path|repo_root)\s*:?\s*([^\n;]+)`)
	matches := pattern.FindAllStringSubmatch(text, -1)
	roots := []string{}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		value := strings.TrimSpace(match[1])
		value = strings.Trim(value, "\"'`")
		if value == "" {
			continue
		}
		lower := strings.ToLower(value)
		for _, marker := range []string{" command ", " command:", " observed_url", " observed url", " branch_id", " head_sha"} {
			if idx := strings.Index(lower, marker); idx >= 0 {
				value = strings.TrimSpace(value[:idx])
				break
			}
		}
		value = strings.Trim(value, " .")
		if value != "" {
			roots = append(roots, value)
		}
	}
	return uniqueTrimmedCSVStrings(roots)
}

func resolveVisualAcceptanceScreenshotRef(ref string, roots []string) (string, string) {
	ref = strings.TrimSpace(strings.Trim(ref, "\"'`"))
	if ref == "" {
		return "", "missing"
	}
	lower := strings.ToLower(ref)
	if strings.HasPrefix(lower, "artifact://") || strings.HasPrefix(lower, "workspace-artifact://") || strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "data:") {
		return "", "unsupported"
	}
	if strings.HasPrefix(lower, "file://") {
		localPath, ok := visualAcceptanceLocalPathFromFileURL(ref)
		if !ok {
			return "", "missing"
		}
		ref = localPath
	}
	local := filepath.Clean(filepath.FromSlash(ref))
	if filepath.IsAbs(local) {
		if visualAcceptanceFileExists(local) {
			return local, "ok"
		}
		return "", "missing"
	}
	candidates := []string{}
	for _, root := range roots {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		candidates = append(candidates, filepath.Join(root, local))
	}
	for _, candidate := range uniqueTrimmedCSVStrings(candidates) {
		if visualAcceptanceFileExists(candidate) {
			return candidate, "ok"
		}
	}
	return "", "missing"
}

func visualAcceptanceLocalPathFromFileURL(raw string) (string, bool) {
	raw = strings.TrimSpace(strings.Trim(raw, "\"'`"))
	if raw == "" {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "file") {
		return "", false
	}
	pathPart, err := url.PathUnescape(parsed.Path)
	if err != nil {
		pathPart = parsed.Path
	}
	host := strings.TrimSpace(parsed.Host)
	if host != "" && !strings.EqualFold(host, "localhost") {
		unc := "//" + host
		if strings.TrimSpace(pathPart) != "" {
			unc += "/" + strings.TrimLeft(pathPart, "/")
		}
		return filepath.Clean(filepath.FromSlash(unc)), true
	}
	if strings.TrimSpace(pathPart) == "" && strings.TrimSpace(parsed.Opaque) != "" {
		pathPart, err = url.PathUnescape(parsed.Opaque)
		if err != nil {
			pathPart = parsed.Opaque
		}
	}
	pathPart = strings.TrimSpace(pathPart)
	if pathPart == "" {
		return "", false
	}
	if len(pathPart) >= 3 && pathPart[0] == '/' && pathPart[2] == ':' {
		pathPart = pathPart[1:]
	}
	return filepath.Clean(filepath.FromSlash(pathPart)), true
}

func visualAcceptanceFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func inspectVisualAcceptanceScreenshot(path, ref string, firstViewport bool) []string {
	f, err := os.Open(path)
	if err != nil {
		return []string{"screenshot artifact unreadable: " + ref}
	}
	defer f.Close()
	img, _, err := image.Decode(f)
	if err != nil {
		return []string{"screenshot artifact undecodable: " + ref}
	}
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width < 64 || height < 64 {
		return []string{"screenshot artifact too small: " + ref}
	}
	stats := visualAcceptanceImageStats(img)
	total, uniqueBins, lumaRange, dominantRatio := stats.Total, stats.UniqueBins, stats.LumaRange, stats.DominantRatio
	if total == 0 {
		return []string{"screenshot artifact blank/uniform: " + ref}
	}
	if uniqueBins <= 2 || lumaRange < 8 || dominantRatio > 0.995 {
		return []string{"screenshot artifact blank/uniform: " + ref}
	}
	if uniqueBins <= 8 && dominantRatio > 0.985 {
		return []string{"screenshot artifact nearly uniform: " + ref}
	}
	if firstViewport {
		if stats.DominantRatio > 0.88 && stats.StrongForegroundRatio < 0.018 && stats.MediumForegroundRatio < 0.075 {
			return []string{"screenshot first viewport low-content composition: " + ref}
		}
		if stats.DominantRatio > 0.70 && stats.StrongForegroundRatio < 0.025 && stats.MediumForegroundRatio > 0.16 && stats.LargestStrongEmptyBandRatio > 0.45 {
			return []string{"screenshot first viewport low-contrast composition: " + ref}
		}
	}
	return nil
}

type visualAcceptanceImageStatsResult struct {
	Total                       int
	UniqueBins                  int
	LumaRange                   int
	DominantRatio               float64
	StrongForegroundRatio       float64
	MediumForegroundRatio       float64
	LargestStrongEmptyBandRatio float64
}

func visualAcceptanceImageStats(img image.Image) visualAcceptanceImageStatsResult {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width <= 0 || height <= 0 {
		return visualAcceptanceImageStatsResult{}
	}
	stepX := maxInt(1, width/96)
	stepY := maxInt(1, height/96)
	bins := map[uint32]int{}
	binSums := map[uint32][3]int{}
	minLuma := 255
	maxLuma := 0
	total := 0
	maxBin := 0
	var dominantKey uint32
	for y := bounds.Min.Y; y < bounds.Max.Y; y += stepY {
		for x := bounds.Min.X; x < bounds.Max.X; x += stepX {
			r16, g16, b16, a16 := img.At(x, y).RGBA()
			if a16 == 0 {
				continue
			}
			r := int(r16 >> 8)
			g := int(g16 >> 8)
			b := int(b16 >> 8)
			luma := (299*r + 587*g + 114*b) / 1000
			if luma < minLuma {
				minLuma = luma
			}
			if luma > maxLuma {
				maxLuma = luma
			}
			key := uint32(r/16)<<8 | uint32(g/16)<<4 | uint32(b/16)
			bins[key]++
			sum := binSums[key]
			sum[0] += r
			sum[1] += g
			sum[2] += b
			binSums[key] = sum
			if bins[key] > maxBin {
				maxBin = bins[key]
				dominantKey = key
			}
			total++
		}
	}
	if total == 0 {
		return visualAcceptanceImageStatsResult{}
	}
	dominantSum := binSums[dominantKey]
	dominantCount := maxInt(1, bins[dominantKey])
	bgR := dominantSum[0] / dominantCount
	bgG := dominantSum[1] / dominantCount
	bgB := dominantSum[2] / dominantCount
	strongForeground := 0
	mediumForeground := 0
	totalRows := 0
	currentNoStrongRows := 0
	largestNoStrongRows := 0
	for y := bounds.Min.Y; y < bounds.Max.Y; y += stepY {
		rowHasStrong := false
		for x := bounds.Min.X; x < bounds.Max.X; x += stepX {
			r16, g16, b16, a16 := img.At(x, y).RGBA()
			if a16 == 0 {
				continue
			}
			r := int(r16 >> 8)
			g := int(g16 >> 8)
			b := int(b16 >> 8)
			distance := absInt(r-bgR) + absInt(g-bgG) + absInt(b-bgB)
			if distance >= 72 {
				strongForeground++
				rowHasStrong = true
			}
			if distance >= 28 {
				mediumForeground++
			}
		}
		totalRows++
		if rowHasStrong {
			currentNoStrongRows = 0
		} else {
			currentNoStrongRows++
			if currentNoStrongRows > largestNoStrongRows {
				largestNoStrongRows = currentNoStrongRows
			}
		}
	}
	largestStrongEmptyBandRatio := 0.0
	if totalRows > 0 {
		largestStrongEmptyBandRatio = float64(largestNoStrongRows) / float64(totalRows)
	}
	return visualAcceptanceImageStatsResult{
		Total:                       total,
		UniqueBins:                  len(bins),
		LumaRange:                   maxLuma - minLuma,
		DominantRatio:               float64(maxBin) / float64(total),
		StrongForegroundRatio:       float64(strongForeground) / float64(total),
		MediumForegroundRatio:       float64(mediumForeground) / float64(total),
		LargestStrongEmptyBandRatio: largestStrongEmptyBandRatio,
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

type visualStateRefs struct {
	initial []string
	primary []string
	result  []string
}

func visualStateScreenshotRefs(text string) visualStateRefs {
	return visualStateRefs{
		initial: screenshotRefsNearAny(text, []string{"initial_state", "initial state", "empty_state", "empty state", "first_viewport", "first viewport", "first_screen", "first screen"}),
		primary: screenshotRefsNearAny(text, []string{"primary_flow", "primary flow", "primary_path", "primary path", "happy_path", "happy path", "user_flow", "user flow"}),
		result:  screenshotRefsNearAny(text, []string{"result_state", "result state", "output_state", "output state", "export_state", "export state", "post_action", "post-action", "post action"}),
	}
}

func screenshotRefsNearAny(text string, anchors []string) []string {
	text = strings.ToLower(text)
	refs := []string{}
	for _, anchor := range anchors {
		anchor = strings.ToLower(strings.TrimSpace(anchor))
		if anchor == "" {
			continue
		}
		searchFrom := 0
		for {
			idx := strings.Index(text[searchFrom:], anchor)
			if idx < 0 {
				break
			}
			idx += searchFrom
			start := idx
			end := idx + len(anchor) + 500
			if end > len(text) {
				end = len(text)
			}
			refs = append(refs, screenshotRefsInText(text[start:end])...)
			searchFrom = idx + len(anchor)
		}
	}
	return uniqueTrimmedCSVStrings(refs)
}

func screenshotRefsInText(text string) []string {
	refs := []string{}
	for _, ref := range visualAcceptanceFileImageURLPattern.FindAllString(text, -1) {
		ref = strings.Trim(strings.TrimSpace(ref), ".:")
		if ref != "" {
			refs = append(refs, ref)
		}
	}
	text = visualAcceptanceFileImageURLPattern.ReplaceAllString(text, " ")
	for _, ref := range visualAcceptanceLocalImagePathPattern.FindAllString(text, -1) {
		ref = strings.Trim(strings.TrimSpace(ref), ".:")
		if ref != "" {
			refs = append(refs, ref)
		}
	}
	text = visualAcceptanceLocalImagePathPattern.ReplaceAllString(text, " ")
	fields := strings.FieldsFunc(text, func(r rune) bool {
		switch r {
		case ' ', '\n', '\t', '\r', '"', '\'', '`', ',', ';', '|', '[', ']', '(', ')', '{', '}':
			return true
		default:
			return false
		}
	})
	for _, field := range fields {
		field = strings.Trim(strings.TrimSpace(field), ".:")
		lower := strings.ToLower(field)
		if lower == "" {
			continue
		}
		if strings.HasPrefix(lower, "artifact://") || strings.HasPrefix(lower, "workspace-artifact://") || strings.HasPrefix(lower, "artifacts/") || strings.HasPrefix(lower, "run-artifacts/") || strings.Contains(lower, "/artifacts/") || strings.Contains(lower, "\\artifacts\\") {
			refs = append(refs, field)
			continue
		}
		if strings.Contains(lower, ".png") || strings.Contains(lower, ".jpg") || strings.Contains(lower, ".jpeg") || strings.Contains(lower, ".webp") {
			refs = append(refs, field)
		}
	}
	return uniqueTrimmedCSVStrings(refs)
}

func visualStateScreenshotRefsAreDistinct(refs visualStateRefs) bool {
	required := [][]string{refs.initial, refs.primary}
	if len(refs.result) > 0 {
		required = append(required, refs.result)
	}
	seen := map[string]bool{}
	for _, group := range required {
		if len(group) == 0 {
			continue
		}
		ref := strings.ToLower(strings.TrimSpace(group[0]))
		if ref == "" {
			continue
		}
		if seen[ref] {
			return false
		}
		seen[ref] = true
	}
	return true
}

func visualEvidenceHasNarrowScreenshotRef(text string) bool {
	for _, ref := range screenshotRefsInText(text) {
		ref = strings.ToLower(ref)
		if strings.Contains(ref, "mobile") || strings.Contains(ref, "narrow") || strings.Contains(ref, "390") || strings.Contains(ref, "375") {
			return true
		}
	}
	return false
}

func visualResultStateNotApplicable(text string) bool {
	return mentionsAny(text, []string{
		"result_state: not_applicable",
		"result_state: n/a",
		"result state not applicable",
		"output state not applicable",
		"export state not applicable",
		"no output state",
		"no result state",
	})
}

func visualEvidenceHasProvenance(text string) bool {
	hasCandidate := mentionsAny(text, []string{"branch_id", "branch name", "branch_name", "candidate_branch", "candidate branch", "head_sha", "candidate_head", "head sha", "queue_id", "item_id"})
	hasURL := mentionsAny(text, []string{"observed_url", "observed url", "url:", "route:", "http://", "https://", "localhost", "127.0.0.1"})
	hasRun := mentionsAny(text, []string{"checkout", "validation_checkout", "command", "dev_command", "browser_engine", "playwright", "chromium", "npm", "preview"})
	return hasCandidate && hasURL && hasRun
}

func visualEvidenceCoversProductIntent(text string) bool {
	if visualAcceptanceCriteriaIDPattern.MatchString(text) {
		return true
	}
	return visualTextHasMeaningfulLabeledValue(text, []string{
		"core_user_promise",
		"core user promise",
		"user-visible contract",
		"user visible contract",
	})
}

func visualTextHasMeaningfulLabeledValue(text string, labels []string) bool {
	text = strings.ToLower(text)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		for _, label := range labels {
			label = strings.ToLower(strings.TrimSpace(label))
			if label == "" || !strings.Contains(line, label) {
				continue
			}
			value := strings.TrimSpace(strings.TrimPrefix(line[strings.Index(line, label)+len(label):], ":"))
			value = strings.Trim(value, " -\t")
			if len(value) >= 24 {
				return true
			}
		}
	}
	return false
}

func visualEvidenceCoversInitialState(text string) bool {
	return mentionsAny(text, []string{
		"empty state",
		"initial state",
		"first viewport",
		"first screen",
		"first render",
		"first paint",
		"pre-upload",
		"pre upload",
		"pre-import",
		"pre import",
		"before upload",
		"before import",
		"blank state",
		"default state",
		"no imports yet",
	})
}

func visualEvidenceCoversPrimaryPath(text string) bool {
	return mentionsAny(text, []string{
		"primary happy path",
		"happy path",
		"primary flow",
		"main flow",
		"upload flow",
		"import flow",
		"load sample",
		"sample flow",
		"choose file",
		"file picker",
		"drag",
		"drop",
		"convert",
		"process",
	})
}

func visualEvidenceCoversResultState(text string) bool {
	return mentionsAny(text, []string{
		"result state",
		"output state",
		"export state",
		"download state",
		"post-action",
		"post action",
		"post-upload",
		"post upload",
		"post-import",
		"post import",
		"after upload",
		"after import",
		"after sample",
		"loaded state",
		"preview populated",
		"generated output",
		"export controls",
		"download",
		"result",
		"output not applicable",
		"result not applicable",
		"export not applicable",
		"no output state",
	})
}

func visualEvidenceMissingCheckRequirements(text string) []string {
	missing := []string{}
	if !mentionsAny(text, []string{"overlap", "overlapping"}) || !mentionsAny(text, []string{"clipping", "clipped", "cut off", "cropped"}) {
		missing = append(missing, "overlap and clipping checks")
	}
	if !mentionsAny(text, []string{"contrast", "readability", "legibility", "readable"}) {
		missing = append(missing, "contrast/readability check")
	}
	if !mentionsAny(text, []string{"responsive", "responsive_fit", "responsive fit", "viewport width", "horizontal overflow", "fits within viewport"}) {
		missing = append(missing, "responsive fit check")
	}
	if !mentionsAny(text, []string{"typography", "type scale", "visual hierarchy", "hierarchy", "spacing", "whitespace", "composition", "first viewport composition"}) {
		missing = append(missing, "typography/hierarchy/spacing check")
	}
	if !mentionsAny(text, []string{"primary surface geometry", "primary-surface geometry", "board geometry", "grid geometry", "canvas geometry", "chart geometry", "editor geometry", "map geometry", "aspect ratio", "cell size", "object size", "density", "line wrap", "line-wrapped"}) {
		missing = append(missing, "primary-surface geometry/density check")
	}
	if !mentionsAny(text, []string{"usability", "real-user", "real user", "discoverable", "operable", "ergonomic"}) {
		missing = append(missing, "real-user usability check")
	}
	return missing
}

func visualEvidenceMentionsLayoutRisk(text string) bool {
	return mentionsAny(text, []string{
		"layout_risk",
		"layout risk",
		"layout_risk_summary",
		"browser_visual_probe_result_v1",
		"risk_score",
		"visual layout risk: none",
		"no layout risk observed",
	})
}

func mentionsAny(text string, needles []string) bool {
	text = strings.ToLower(text)
	for _, needle := range needles {
		needle = strings.ToLower(strings.TrimSpace(needle))
		if needle != "" && strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func workspaceDocsText(docs []WorkspaceDocRecord) string {
	parts := make([]string, 0, len(docs)*3)
	for _, doc := range docs {
		parts = append(parts, doc.DocKey, doc.Title, doc.Content)
	}
	return strings.Join(parts, "\n")
}

func positiveVisualMention(text string, needles []string) bool {
	for _, needle := range needles {
		needle = strings.ToLower(strings.TrimSpace(needle))
		if needle == "" || !strings.Contains(text, needle) {
			continue
		}
		for _, negated := range []string{"no " + needle, "without " + needle, "missing " + needle, "lacks " + needle, "not " + needle} {
			if strings.Contains(text, negated) {
				return false
			}
		}
		return true
	}
	return false
}

func visualEvidenceHasPassVerdict(text string) bool {
	passNeedles := []string{
		"visual_verdict: pass",
		"visual verdict: pass",
		`"visual_verdict": "pass"`,
		`"visual_verdict":"pass"`,
	}
	return containsAnySignal(text, passNeedles)
}

func visualBlockingSignals(text string) []string {
	signals := []string{}
	for label, needles := range map[string][]string{
		"text overlap":               {"text overlap", "overlapping text", "overlap failure", "overlaps preceding", "overlaps subsequent", "overlapping dropzone copy", "dropzone copy overlaps"},
		"clipped content":            {"clipped", "cut off", "cropped text", "text cut"},
		"unreadable text":            {"unreadable", "illegible", "too low contrast", "contrast fail", "contrast failure", "unreadable pale hero", "low-contrast hero", "pale hero"},
		"broken layout":              {"broken layout", "layout broken", "misaligned", "off-screen", "occluded", "occludes"},
		"oversized typography":       {"oversized typography", "giant heading", "hero text too large", "too large inside", "oversized_text", "horizontal_overflow_risk", "low_contrast_text_hint"},
		"broken first viewport":      {"broken first viewport", "first viewport failure", "bad first screen", "first screen broken", "empty state broken", "awkward empty state", "empty state awkward"},
		"excessive whitespace":       {"excessive whitespace", "too much whitespace", "huge blank", "giant empty", "empty panel dominates"},
		"performance symptom":        {"laggy", "jank", "slow interaction", "unresponsive", "main thread stall"},
		"layout risk":                {`"risk_level": "high"`, `"risk_level":"high"`, "layout_risk: high", "layout risk: high", "layout risk high", "layout_risk_summary high"},
		"primary surface risk":       {"primary_surface:board_cells_without_visible_css", "primary_surface:board_grid_columns_without_display_grid", "board_cells_without_visible_css", "board_grid_columns_without_display_grid", "unstyled_primary_surface", "game_board_likely_line_wrapped", "visual_quality_status: needs_semantic_review", `"visual_quality_status": "needs_semantic_review"`, `"visual_quality_status":"needs_semantic_review"`},
		"non-durable dirty evidence": {"dirty checkout", "dirty worktree", "worktree dirty", "working tree dirty", "has uncommitted changes", "uncommitted changes", "uncommitted local changes", "uncommitted product", "local-only fix", "local dirty fix", "provisional_non_canonical_review_target", "provisional non-canonical review target", "provisional/non-canonical", "not committed to head", "not part of head", "head_sha does not include"},
	} {
		for _, needle := range needles {
			if strings.Contains(text, needle) && !visualSignalNegated(text, needle) {
				signals = append(signals, label)
				break
			}
		}
	}
	return uniqueTrimmedCSVStrings(signals)
}

func visualSignalNegated(text, needle string) bool {
	needle = strings.ToLower(strings.TrimSpace(needle))
	if needle == "" {
		return false
	}
	negations := []string{
		"no " + needle,
		"without " + needle,
		"not " + needle,
		needle + " not observed",
		needle + " not present",
		needle + " absent",
	}
	for _, negation := range negations {
		if strings.Contains(text, negation) {
			return true
		}
	}
	words := strings.Fields(needle)
	if len(words) > 0 {
		first := words[0]
		for _, negation := range []string{"no " + first + " observed", "no " + first + " found", "not " + first} {
			if strings.Contains(text, negation) {
				return true
			}
		}
	}
	return false
}

func visualAcceptanceGateError(toolName string, item ProjectPatchQueueItemRecord, result visualAcceptanceGateResult) string {
	var b strings.Builder
	b.WriteString(toolName)
	b.WriteString(" visual_acceptance_gate blocked ACCEPTED decision for UI-facing candidate ")
	b.WriteString(firstNonEmpty(item.QueueID, "<queue>"))
	b.WriteString("/")
	b.WriteString(firstNonEmpty(item.ItemID, "<item>"))
	b.WriteString(".")
	if len(result.Reasons) > 0 {
		b.WriteString(" ui_signals=")
		b.WriteString(strings.Join(result.Reasons, ","))
		b.WriteString(".")
	}
	if len(result.EvidenceDocKeys) > 0 {
		b.WriteString(" inspected_visual_docs=")
		b.WriteString(strings.Join(result.EvidenceDocKeys, ","))
		b.WriteString(".")
	}
	if len(result.Missing) > 0 {
		b.WriteString(" missing=")
		b.WriteString(strings.Join(result.Missing, "; "))
		b.WriteString(".")
	}
	if len(result.BlockingSignals) > 0 {
		b.WriteString(" blocking_visual_signals=")
		b.WriteString(strings.Join(result.BlockingSignals, ","))
		b.WriteString(".")
	}
	b.WriteString(" Publish a workspace doc with rhizome_visual_acceptance_v1, state-specific screenshot refs or paths keyed to initial_state, primary_flow, and result_state, branch/head/url/checkout provenance, acceptance criteria or core user promise exercised, desktop+narrow viewport matrix, overlap/clipping/contrast/readability/responsive/typography/hierarchy/spacing/usability checks, primary-surface geometry/density checks, and visual_verdict: pass before accepting. If the visual packet contains blockers, record action=block or reject and create a focused UI repair follow-up.")
	return b.String()
}

func visualAcceptanceReviewPacketGuidance(pathset []string) string {
	if len(pathset) > 0 && len(uiFacingSignalsForPatchQueueItem(nil, ProjectPatchQueueItemRecord{Pathset: pathset}, "")) == 0 {
		return ""
	}
	return fmt.Sprintf(`For UI-facing work, reviewers must require a visual acceptance packet before patch queue ACCEPTED:
- schema: rhizome_visual_acceptance_v1
- state_evidence: initial_state, primary_flow, and result_state each with its own local screenshot path or workspace artifact ref
- provenance: branch/head or candidate id, observed URL/route, checkout/command/browser evidence
- product_intent: acceptance criteria IDs or core user promise exercised
- viewport_matrix: at least desktop and one narrow/mobile viewport when the product has a UI
- scenarios: first viewport/empty state, primary happy path, and output/export/result state when relevant
- checks: overlap, clipping, contrast/readability, responsive fit, typography/hierarchy/spacing, primary-surface geometry/density, stale preview protection, performance symptoms, and obvious real-user usability defects
- primary_surface: if the UI has a board, grid, canvas, chart, editor, map, or game surface, judge screenshot/DOM geometry for aspect ratio, cell/object size, wrapping, density, empty-space balance, and mode/preset/difficulty-specific fit; stretched, sparse, tiny, or line-wrapped primary surfaces are blocking findings even when generic probe risk is low
- visual_verdict: pass only when there are no blocking visual findings
`)
}
