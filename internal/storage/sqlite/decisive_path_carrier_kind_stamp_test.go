package sqlite

import (
	"encoding/json"
	"testing"
)

// Stage 4 birth-site stamp: the single creation chokepoint (createTaskWithGraphTx) writes the carrier kind
// into requirements JSON under "decisive_path_kind" so decisivePathRoute can classify the born entity.
func TestStampDecisivePathCarrierKindJSON(t *testing.T) {
	// Empty kind leaves an ordinary task's requirements untouched.
	if got := stampDecisivePathCarrierKindJSON(`{"a":1}`, ""); got != `{"a":1}` {
		t.Fatalf("empty kind must leave requirements unchanged, got %q", got)
	}
	if got := stampDecisivePathCarrierKindJSON("", ""); got != "" {
		t.Fatalf("empty kind + empty requirements must stay empty, got %q", got)
	}

	// Empty/absent requirements + a kind produces a fresh object carrying the kind.
	got := stampDecisivePathCarrierKindJSON("", decisivePathKindPatchQueueIntegrationCont)
	var fields map[string]any
	if err := json.Unmarshal([]byte(got), &fields); err != nil {
		t.Fatalf("stamped requirements must be valid JSON, got %q: %v", got, err)
	}
	if fields["decisive_path_kind"] != decisivePathKindPatchQueueIntegrationCont {
		t.Fatalf("decisive_path_kind not stamped, got %v", fields["decisive_path_kind"])
	}

	// Existing requirements keep their fields and gain the kind (merge, not replace).
	got = stampDecisivePathCarrierKindJSON(`{"patch_queue_task_kind":"integration","state":"ACCEPTED"}`, decisivePathKindPatchQueueIntegrationCont)
	fields = nil
	if err := json.Unmarshal([]byte(got), &fields); err != nil {
		t.Fatalf("merged requirements must be valid JSON, got %q: %v", got, err)
	}
	if fields["patch_queue_task_kind"] != "integration" || fields["state"] != "ACCEPTED" {
		t.Fatalf("merge must preserve existing fields, got %v", fields)
	}
	if fields["decisive_path_kind"] != decisivePathKindPatchQueueIntegrationCont {
		t.Fatalf("merge must add decisive_path_kind, got %v", fields)
	}

	// The kind is normalized (trim + lowercase) like the route fn reads it.
	got = stampDecisivePathCarrierKindJSON("{}", "  Patch_Queue_Integration_Continuation  ")
	fields = nil
	_ = json.Unmarshal([]byte(got), &fields)
	if fields["decisive_path_kind"] != decisivePathKindPatchQueueIntegrationCont {
		t.Fatalf("kind must be normalized to %q, got %v", decisivePathKindPatchQueueIntegrationCont, fields["decisive_path_kind"])
	}
}
