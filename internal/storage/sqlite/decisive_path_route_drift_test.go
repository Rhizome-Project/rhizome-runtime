package sqlite

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDecisivePathRouteCopiesDoNotDrift locks the byte-identity of the shared decisive-path route
// primitive across the two Go modules. agent is a SEPARATE module and cannot import
// internal/storage/sqlite, so decisivePathRoute + its constants/types/helpers are duplicated verbatim in
// agent/decisive_path_route.go and internal/storage/sqlite/decisive_path_route.go. The ONLY permitted
// difference is the leading `package` line + the file-level doc comment; everything from the first
// `// Route kinds` marker to EOF MUST be byte-identical. This is the single-source-of-truth enforcement
// (same discipline as convergence_blocking_drift_test.go): the route DECISION is written once, the two
// modules only differ in how they normalize/execute around it.
func TestDecisivePathRouteCopiesDoNotDrift(t *testing.T) {
	const marker = "// Route kinds - exactly one is chosen for every entity."
	storageBody := decisivePathSharedRegion(t, filepath.Join("decisive_path_route.go"), marker)
	agentBody := decisivePathSharedRegion(t, filepath.Join("..", "..", "..", "agent", "decisive_path_route.go"), marker)
	if storageBody != agentBody {
		t.Fatalf("decisivePathRoute copies have drifted between internal/storage/sqlite and agent.\n"+
			"They must be byte-identical from %q to EOF.\n--- storage len=%d ---\n--- agent len=%d ---",
			marker, len(storageBody), len(agentBody))
	}
	if !strings.Contains(storageBody, "unclassified_decisive_path_carrier") {
		t.Fatalf("decisive-path route primitive lost its fail-closed default reason; the shared region must contain unclassified_decisive_path_carrier")
	}
}

func decisivePathSharedRegion(t *testing.T, path, marker string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	text := string(raw)
	idx := strings.Index(text, marker)
	if idx < 0 {
		t.Fatalf("marker %q not found in %s", marker, path)
	}
	return strings.TrimRight(text[idx:], "\n")
}
