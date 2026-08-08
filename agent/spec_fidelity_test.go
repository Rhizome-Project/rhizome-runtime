package main

import (
	"strings"
	"testing"
)

func TestSourceDocKeysFromSourceRefsTextIgnoresMetadata(t *testing.T) {
	keys := sourceDocKeysFromSourceRefsText("```rhizome_source_refs_v1\n" +
		"project_id: project-clearpress\n" +
		"root_task_id: task-clearpress-root\n" +
		"source_doc_keys:\n" +
		"- run.clearpress.operator-spec\n" +
		"```")
	if strings.Join(keys, ",") != "run.clearpress.operator-spec" {
		t.Fatalf("expected only source_doc_keys, got %#v", keys)
	}
}
