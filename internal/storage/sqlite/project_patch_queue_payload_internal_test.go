package sqlite

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProjectPatchQueueEventPayloadRedactsClaimToken(t *testing.T) {
	const sentinel = "sentinel-claim-token-must-not-enter-runtime-event"
	item := ProjectPatchQueueItemRecord{
		WorkspaceID: "ws-security",
		ProjectID:   "project-security",
		QueueID:     "patchq-security",
		ItemID:      "patchitem-security",
		ClaimToken:  sentinel,
	}

	payload := projectPatchQueueEventPayload(item, "operator-security", "patch_queue.operator_enablement_record")
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal project patch queue event payload: %v", err)
	}
	if strings.Contains(string(raw), sentinel) || strings.Contains(string(raw), `"claim_token"`) {
		t.Fatalf("runtime event payload disclosed patch queue claim token: %s", raw)
	}
	if item.ClaimToken != sentinel {
		t.Fatalf("redaction mutated internal fencing record: got %q", item.ClaimToken)
	}

	candidate, ok := payload["patch_queue_candidate"].(ProjectPatchQueueItemRecord)
	if !ok {
		t.Fatalf("patch_queue_candidate type = %T, want ProjectPatchQueueItemRecord", payload["patch_queue_candidate"])
	}
	if candidate.ClaimToken != "" {
		t.Fatalf("redacted runtime event candidate retained claim token %q", candidate.ClaimToken)
	}
}
