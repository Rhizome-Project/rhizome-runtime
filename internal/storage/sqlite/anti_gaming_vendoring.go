package sqlite

import (
	"bufio"
	"strings"
)

// Anti-gaming NO-VENDORING gate (ground-truth, fail-closed).
//
// The product interpreter must be served by roster-built code: zero third-party
// interpreter/runtime module dependencies. This is the first GROUND-TRUTH accept gate -
// it reads the actual go.mod at the candidate head (not a reviewer's attestation) - and
// closes the spec-gaming failure mode in which a "build the interpreter" task vendored
// github.com/yuin/gopher-lua and the attestation-only accept gates accepted the substitution
// because the tests were green (green BECAUSE gopher-lua did the work). See
// The gate is intentionally fail-closed when repository evidence is unavailable.
//
// Polarity: a deny-list of known embeddable interpreter/VM modules + an interpreter-ish
// path heuristic, fail-closed (unreadable / unparseable / module-less go.mod => BLOCK). The
// interpreter-path import-closure allow-list (any third-party import on the eval path gates)
// is the documented hardening; this analyzer is the shippable structural core that makes the
// named gopher-lua substitution impossible.

// bannedInterpreterModulePrefixes are third-party Go modules that embed a language
// interpreter / runtime / VM. A product whose go.mod requires any of these is vendoring its
// interpreter capability instead of building it.
var bannedInterpreterModulePrefixes = []string{
	"github.com/yuin/gopher-lua",
	"github.com/Shopify/go-lua",
	"github.com/arnodel/golua",
	"github.com/aarzilli/golua",
	"github.com/stevedonovan/luar",
	"github.com/epikur-io/go-lua",
	"github.com/robertkrimen/otto",
	"github.com/dop251/goja",
	"github.com/d5/tengo",
	"go.starlark.net",
	"github.com/mattn/anko",
	"github.com/tetratelabs/wazero",
	"github.com/wasmerio/wasmer-go",
	"github.com/traefik/yaegi",
}

// interpreterIshSubstrings flag an unknown third-party module whose path strongly suggests
// an embedded interpreter / VM, so a freshly vendored library not yet on the deny-list still
// gates (defense against the deny-list "missing entry leaks" weakness).
var interpreterIshSubstrings = []string{
	"gopher-lua", "go-lua", "golua", "/lua", "lua-", "luajit",
	"interp", "/vm", "wasm", "starlark", "tengo", "duktape", "quickjs", "mruby", "yaegi",
}

func goModModulePath(goModContent string) string {
	sc := bufio.NewScanner(strings.NewReader(goModContent))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module"))
		}
	}
	return ""
}

// goModRequiredModulePaths returns the module path of every require directive - both the
// single-line `require X v` form and the `require ( ... )` block form - dropping versions and
// `// indirect` comments.
func goModRequiredModulePaths(goModContent string) []string {
	var paths []string
	sc := bufio.NewScanner(strings.NewReader(goModContent))
	inBlock := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if inBlock {
			if line == ")" {
				inBlock = false
				continue
			}
			if p := goModRequireLineModulePath(line); p != "" {
				paths = append(paths, p)
			}
			continue
		}
		if line == "require (" || strings.HasPrefix(line, "require (") {
			inBlock = true
			continue
		}
		if strings.HasPrefix(line, "require ") {
			if p := goModRequireLineModulePath(strings.TrimSpace(strings.TrimPrefix(line, "require"))); p != "" {
				paths = append(paths, p)
			}
		}
	}
	return paths
}

func goModRequireLineModulePath(line string) string {
	if i := strings.Index(line, "//"); i >= 0 {
		line = strings.TrimSpace(line[:i])
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// moduleIsOwnOrStdlib reports whether a required module path is the product's own module (or
// a subpath of it). go.mod require directives never name the Go standard library, so any
// required module that is not the own module is third-party by construction.
func moduleIsOwnOrStdlib(modulePath, ownModulePath string) bool {
	modulePath = strings.TrimSpace(modulePath)
	if modulePath == "" {
		return true
	}
	ownModulePath = strings.TrimSpace(ownModulePath)
	if ownModulePath != "" && (modulePath == ownModulePath || strings.HasPrefix(modulePath, ownModulePath+"/")) {
		return true
	}
	return false
}

func moduleIsBannedInterpreter(modulePath string) bool {
	lower := strings.ToLower(strings.TrimSpace(modulePath))
	if lower == "" {
		return false
	}
	for _, b := range bannedInterpreterModulePrefixes {
		bl := strings.ToLower(b)
		if lower == bl || strings.HasPrefix(lower, bl+"/") {
			return true
		}
	}
	for _, s := range interpreterIshSubstrings {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}

// interpreterVendoringViolations is the pure, golden-testable analyzer. An empty result means
// no vendoring was detected. Fail-closed: an empty / module-less go.mod yields a blocking
// "no_vendoring_unproven" reason (the gate must never pass on absence of proof).
//
// Polarity = ALLOW-LIST: a roster-built interpreter declares ZERO third-party dependencies, so
// ANY required module that is not the product's own module (and not explicitly permitted) gates
// - known interpreter/runtime modules are merely NAMED more loudly. This avoids both the
// deny-list "missing entry leaks" weakness and the over-broad-substring false positives: the
// block decision does not depend on guessing whether a module "looks like" an interpreter.
func interpreterVendoringViolations(goModContent string) []string {
	return interpreterVendoringViolationsWithPermits(goModContent, nil)
}

// interpreterVendoringViolationsWithPermits allows a per-project allow-list of explicitly
// permitted third-party modules (default: none). A scoped project that genuinely needs a benign
// dependency lists it in its config; the interpreter is otherwise dependency-free.
func interpreterVendoringViolationsWithPermits(goModContent string, permitted map[string]bool) []string {
	if strings.TrimSpace(goModContent) == "" {
		return []string{"no_vendoring_unproven: go.mod is empty or unreadable at candidate head"}
	}
	ownModule := goModModulePath(goModContent)
	if ownModule == "" {
		return []string{"no_vendoring_unproven: go.mod has no module path at candidate head"}
	}
	var reasons []string
	for _, req := range goModRequiredModulePaths(goModContent) {
		if moduleIsOwnOrStdlib(req, ownModule) || permitted[strings.TrimSpace(req)] {
			continue
		}
		if moduleIsBannedInterpreter(req) {
			reasons = append(reasons, "vendored_interpreter_dependency: "+req+" (the product interpreter must be roster-built, not a third-party interpreter/runtime)")
		} else {
			reasons = append(reasons, "third_party_dependency_on_dependency_free_interpreter: "+req+" (a roster-built interpreter declares zero third-party deps; permit it explicitly if genuinely required)")
		}
	}
	// A replace directive that redirects a module to a LOCAL path is a vendoring vector: an
	// innocent require name can be replaced with a local copy of an interpreter, slipping past
	// the require-list scan. Any local-path replace on a dependency-free interpreter gates.
	for _, rep := range goModLocalReplaceDirectives(goModContent) {
		reasons = append(reasons, "local_replace_directive: "+rep+" (replacing a module with local filesystem code can vendor an interpreter past the require-list check)")
	}
	return reasons
}

// goModLocalReplaceDirectives returns the `old => localpath` replace directives whose target is
// a local filesystem path (./ ../ absolute or a Windows drive path).
func goModLocalReplaceDirectives(goModContent string) []string {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(goModContent))
	inBlock := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if inBlock {
			if line == ")" {
				inBlock = false
				continue
			}
			if d := localReplaceDirective(line); d != "" {
				out = append(out, d)
			}
			continue
		}
		if line == "replace (" || strings.HasPrefix(line, "replace (") {
			inBlock = true
			continue
		}
		if strings.HasPrefix(line, "replace ") {
			if d := localReplaceDirective(strings.TrimSpace(strings.TrimPrefix(line, "replace"))); d != "" {
				out = append(out, d)
			}
		}
	}
	return out
}

func localReplaceDirective(line string) string {
	if i := strings.Index(line, "//"); i >= 0 {
		line = strings.TrimSpace(line[:i])
	}
	idx := strings.Index(line, "=>")
	if idx < 0 {
		return ""
	}
	left := strings.TrimSpace(line[:idx])
	rightFields := strings.Fields(strings.TrimSpace(line[idx+2:]))
	if len(rightFields) == 0 {
		return ""
	}
	target := rightFields[0]
	if replaceTargetIsLocalPath(target) {
		leftFields := strings.Fields(left)
		old := target
		if len(leftFields) > 0 {
			old = leftFields[0] + " => " + target
		}
		return old
	}
	return ""
}

func replaceTargetIsLocalPath(target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	switch {
	case strings.HasPrefix(target, "./"), strings.HasPrefix(target, "../"),
		strings.HasPrefix(target, ".\\"), strings.HasPrefix(target, "..\\"),
		strings.HasPrefix(target, "/"):
		return true
	}
	// Windows drive path e.g. C:\ or C:/
	if len(target) >= 3 && target[1] == ':' && (target[2] == '\\' || target[2] == '/') {
		return true
	}
	return false
}
