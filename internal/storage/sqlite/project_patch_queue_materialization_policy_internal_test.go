package sqlite

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
)

func TestNormalizeProjectPatchQueueMaterializationRejectsRawJSONBeforeUnmarshal(t *testing.T) {
	t.Parallel()

	_, _, _, err := normalizeProjectPatchQueueMaterialization(
		strings.Repeat("x", int(ProjectPatchQueueMaterializationMaxJSONBytes)+1),
		repoauthority.PatchMaterialization{},
		repoauthority.PatchQueueItem{},
		"agent-alpha",
		time.Now().UTC(),
	)
	if !errors.Is(err, ErrProjectPatchQueueInvalid) ||
		!strings.Contains(err.Error(), "materialization storage policy") ||
		!strings.Contains(err.Error(), "materialization_json size") {
		t.Fatalf("expected raw materialization JSON to fail storage policy before unmarshal, got %v", err)
	}
}
