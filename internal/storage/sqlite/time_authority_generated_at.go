package sqlite

import (
	"strings"
	"time"
)

func generatedAtFromWorkspaceTimeAuthority(authority WorkspaceTimeAuthority) string {
	if referenceAt := strings.TrimSpace(authority.ReferenceAt); referenceAt != "" {
		return referenceAt
	}
	return time.Now().UTC().Format(time.RFC3339Nano)
}
