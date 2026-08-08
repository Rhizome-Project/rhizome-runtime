package server

import (
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCorridorSharedReadSideSchemasStayInSharedParamParity(t *testing.T) {
	expected := corridorSchemaParamTags(workspaceInstrumentationCorridorParams{})
	methods := corridorSharedReadSideMethods()
	if len(methods) == 0 {
		t.Fatal("expected at least one corridor read-side method in rpc schemas")
	}
	for _, method := range methods {
		schema, ok := rpcMethodSchemas[method]
		if !ok {
			t.Fatalf("missing corridor schema for %s", method)
		}
		for paramName := range expected {
			if _, ok := schema.Params[paramName]; !ok {
				t.Errorf("corridor schema %q is missing shared param %q", method, paramName)
			}
		}
		for paramName := range schema.Params {
			if !expected[paramName] {
				t.Errorf("corridor schema %q drifted extra param %q away from shared corridor handler params", method, paramName)
			}
		}
	}
}

func TestCorridorTaskFirstReadSideSchemasStayInAuthorityParamParity(t *testing.T) {
	expected := corridorSchemaParamTags(workspaceInstrumentationCorridorAuthorityParams{})
	methods := corridorTaskFirstReadSideMethods()
	if len(methods) == 0 {
		t.Fatal("expected at least one task-first corridor read-side method in rpc schemas")
	}
	for _, method := range methods {
		schema, ok := rpcMethodSchemas[method]
		if !ok {
			t.Fatalf("missing task-first corridor schema for %s", method)
		}
		for paramName := range expected {
			if _, ok := schema.Params[paramName]; !ok {
				t.Errorf("task-first corridor schema %q is missing authority-shared param %q", method, paramName)
			}
		}
		for paramName := range schema.Params {
			if !expected[paramName] {
				t.Errorf("task-first corridor schema %q drifted extra param %q away from shared authority params", method, paramName)
			}
		}
	}
}

func TestCorridorSharedReadSideMethodsInSchemaStayDispatched(t *testing.T) {
	source, err := os.ReadFile("handler.go")
	if err != nil {
		t.Fatalf("read handler.go: %v", err)
	}
	content := string(source)
	for _, method := range corridorSharedReadSideMethods() {
		if !strings.Contains(content, `case "`+method+`":`) {
			t.Errorf("corridor schema %q is missing a dispatch case in handler.go", method)
		}
	}
}

func TestCorridorTaskFirstReadSideMethodsInSchemaStayDispatched(t *testing.T) {
	source, err := os.ReadFile("handler.go")
	if err != nil {
		t.Fatalf("read handler.go: %v", err)
	}
	content := string(source)
	for _, method := range corridorTaskFirstReadSideMethods() {
		if !strings.Contains(content, `case "`+method+`":`) {
			t.Errorf("task-first corridor schema %q is missing a dispatch case in handler.go", method)
		}
	}
}

func TestCorridorDispatchCasesStayDocumentedInSchema(t *testing.T) {
	source, err := os.ReadFile("handler.go")
	if err != nil {
		t.Fatalf("read handler.go: %v", err)
	}
	re := regexp.MustCompile(`case "(workspace\.instrumentation\.corridor(?:\.[a-z_]+)?\.(?:report|cluster|snapshot))":`)
	matches := re.FindAllStringSubmatch(string(source), -1)
	if len(matches) == 0 {
		t.Fatal("expected corridor dispatch cases in handler.go")
	}
	seen := map[string]bool{}
	for _, match := range matches {
		method := strings.TrimSpace(match[1])
		seen[method] = true
		if _, ok := rpcMethodSchemas[method]; !ok {
			t.Errorf("corridor dispatch case %q is missing rpc.describe schema coverage", method)
		}
	}
	if !seen["workspace.instrumentation.corridor.report"] || !seen["workspace.instrumentation.corridor.fit.report"] {
		t.Fatalf("expected readiness and fit corridor dispatch families to stay visible, got %+v", seen)
	}
}

func TestCorridorTaskFirstDispatchCasesStayDocumentedInSchema(t *testing.T) {
	source, err := os.ReadFile("handler.go")
	if err != nil {
		t.Fatalf("read handler.go: %v", err)
	}
	re := regexp.MustCompile(`case "(workspace\.instrumentation\.corridor\.[a-z_]+\.task)":`)
	matches := re.FindAllStringSubmatch(string(source), -1)
	if len(matches) == 0 {
		t.Fatal("expected task-first corridor dispatch cases in handler.go")
	}
	for _, match := range matches {
		method := strings.TrimSpace(match[1])
		if _, ok := rpcMethodSchemas[method]; !ok {
			t.Errorf("task-first corridor dispatch case %q is missing rpc.describe schema coverage", method)
		}
	}
}

func TestInstrumentationLocusBundleKeepsCorridorReadSideSiblingTags(t *testing.T) {
	tags := jsonFieldTags(reflect.TypeOf(sqlite.InstrumentationLocusBundle{}))
	for _, surface := range corridorSharedReadSideSurfaces() {
		tag := "corridor"
		if surface != "" {
			tag += "_" + surface
		}
		if !tags[tag] {
			t.Errorf("instrumentation locus bundle is missing %q tag for corridor surface %q", tag, surface)
		}
	}
}

func TestInstrumentationLocusBundleKeepsTaskFirstCorridorSiblingTags(t *testing.T) {
	tags := jsonFieldTags(reflect.TypeOf(sqlite.InstrumentationLocusBundle{}))
	for _, surface := range corridorTaskFirstReadSideSurfaces() {
		tag := "corridor_" + surface
		if !tags[tag] {
			t.Errorf("instrumentation locus bundle is missing %q tag for task-first corridor surface %q", tag, surface)
		}
	}
}

func TestDashboardOpenCorridorSurfaceLoadsAdjacentCorridorReadSides(t *testing.T) {
	required := []string{
		"function openCorridorSurface(",
		"loadCorridorReadiness()",
		"loadCorridorAuthority()",
		"showCorridorReadinessClusterDetail(clusterID)",
		"loadCorridorFit()",
		"showCorridorFitClusterDetail(clusterID)",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard corridor sidecar is missing %s", needle)
		}
	}
}

func TestDashboardOpenCorridorSurfaceKeepsOptionalBasisAndOwnershipHooks(t *testing.T) {
	required := []string{
		"typeof loadCorridorBasis === 'function' ? loadCorridorBasis() : Promise.resolve(null)",
		"typeof loadCorridorOwnership === 'function' ? loadCorridorOwnership() : Promise.resolve(null)",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard corridor sidecar is missing optional future hook %s", needle)
		}
	}
}

func TestDashboardRuntimeEventDetailKeepsGenericCorridorSnapshotFallback(t *testing.T) {
	required := []string{
		"function corridorRuntimeSnapshotEventType(",
		"function corridorRuntimeSnapshotCountEntries(",
		"function corridorRuntimeSnapshotLabel(",
		"function renderGenericCorridorSnapshotDetail(",
		"Corridor Snapshot Summary",
		"cluster.corridor_",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard runtime-event detail is missing generic corridor snapshot fallback %s", needle)
		}
	}
}

func TestDashboardRuntimeSubscriptionsIncludeFutureCorridorBasisAndOwnershipSnapshots(t *testing.T) {
	required := []string{
		"cluster.corridor_basis_snapshot",
		"cluster.corridor_ownership_snapshot",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard runtime subscriptions are missing %s", needle)
		}
	}
}

func corridorSharedReadSideMethods() []string {
	methods := []string{}
	for method := range rpcMethodSchemas {
		if !isSharedCorridorReadSideMethod(method) {
			continue
		}
		methods = append(methods, method)
	}
	sort.Strings(methods)
	return methods
}

func corridorTaskFirstReadSideMethods() []string {
	methods := []string{}
	for _, surface := range corridorTaskFirstReadSideSurfaces() {
		for _, suffix := range []string{"report", "task"} {
			method := "workspace.instrumentation.corridor." + surface + "." + suffix
			if _, ok := rpcMethodSchemas[method]; ok {
				methods = append(methods, method)
			}
		}
	}
	sort.Strings(methods)
	return methods
}

func corridorSharedReadSideSurfaces() []string {
	surfaces := []string{}
	seen := map[string]bool{}
	for _, method := range corridorSharedReadSideMethods() {
		surface := corridorReadSideSurface(method)
		if seen[surface] {
			continue
		}
		seen[surface] = true
		surfaces = append(surfaces, surface)
	}
	sort.Strings(surfaces)
	return surfaces
}

func corridorTaskFirstReadSideSurfaces() []string {
	surfaces := []string{}
	seen := map[string]bool{}
	for method := range rpcMethodSchemas {
		surface, ok := corridorTaskFirstReadSideSurface(method)
		if !ok || seen[surface] {
			continue
		}
		seen[surface] = true
		surfaces = append(surfaces, surface)
	}
	sort.Strings(surfaces)
	return surfaces
}

func isSharedCorridorReadSideMethod(method string) bool {
	if !strings.HasPrefix(method, "workspace.instrumentation.corridor.") {
		return false
	}
	if strings.Contains(method, ".authority.") {
		return false
	}
	return strings.HasSuffix(method, ".report") || strings.HasSuffix(method, ".cluster") || strings.HasSuffix(method, ".snapshot")
}

func corridorReadSideSurface(method string) string {
	method = strings.TrimSpace(method)
	switch method {
	case "workspace.instrumentation.corridor.report",
		"workspace.instrumentation.corridor.cluster",
		"workspace.instrumentation.corridor.snapshot":
		return ""
	}
	trimmed := strings.TrimPrefix(method, "workspace.instrumentation.corridor.")
	parts := strings.Split(trimmed, ".")
	if len(parts) < 2 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func corridorTaskFirstReadSideSurface(method string) (string, bool) {
	method = strings.TrimSpace(method)
	if !strings.HasPrefix(method, "workspace.instrumentation.corridor.") || !strings.HasSuffix(method, ".task") {
		return "", false
	}
	trimmed := strings.TrimPrefix(method, "workspace.instrumentation.corridor.")
	parts := strings.Split(trimmed, ".")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || parts[1] != "task" {
		return "", false
	}
	return strings.TrimSpace(parts[0]), true
}

func corridorSchemaParamTags(sample any) map[string]bool {
	return jsonFieldTags(reflect.TypeOf(sample))
}

func jsonFieldTags(rt reflect.Type) map[string]bool {
	out := map[string]bool{}
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		if idx := strings.Index(tag, ","); idx >= 0 {
			tag = tag[:idx]
		}
		if tag != "" {
			out[tag] = true
		}
	}
	return out
}
