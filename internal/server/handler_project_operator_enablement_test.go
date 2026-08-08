package server

import (
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
)

func TestOperatorPatchQueueEnableRejectsNonHumanAndUnauthorizedOperatorsBeforeStorage(t *testing.T) {
	tests := []struct {
		name       string
		env        string
		principal  string
		actorID    string
		wantCode   int
		wantErr    string
		callRecord bool
	}{
		{
			name:      "dashboard operator endpoint rejects agent principal",
			env:       "agent-alpha",
			principal: "agent",
			actorID:   "agent-alpha",
			wantCode:  errCodePermissionDenied,
			wantErr:   "human operator principal required",
		},
		{
			name:      "dashboard operator endpoint requires explicit operator ids",
			principal: "human",
			actorID:   "developer",
			wantCode:  errCodePermissionDenied,
			wantErr:   "RHIZOME_OPERATOR_IDS is required",
		},
		{
			name:      "dashboard operator endpoint rejects unauthorized human",
			env:       "alice",
			principal: "human",
			actorID:   "developer",
			wantCode:  errCodePermissionDenied,
			wantErr:   "not in RHIZOME_OPERATOR_IDS",
		},
		{
			name:       "low-level operator record rejects agent principal",
			env:        "agent-alpha",
			principal:  "agent",
			actorID:    "agent-alpha",
			wantCode:   errCodePermissionDenied,
			wantErr:    "human operator principal required",
			callRecord: true,
		},
		{
			name:       "low-level operator record requires explicit operator ids",
			principal:  "human",
			actorID:    "developer",
			wantCode:   errCodePermissionDenied,
			wantErr:    "RHIZOME_OPERATOR_IDS is required",
			callRecord: true,
		},
		{
			name:       "low-level operator record rejects unauthorized human",
			env:        "alice",
			principal:  "human",
			actorID:    "developer",
			wantCode:   errCodePermissionDenied,
			wantErr:    "not in RHIZOME_OPERATOR_IDS",
			callRecord: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RHIZOME_OPERATOR_IDS", tc.env)
			store := newServerTestStore(t)
			h := NewHandler(store)
			ctx := testAuthContext("ws-operator-enable-preflight", tc.principal, tc.actorID)

			var rpcErr *RPCError
			if tc.callRecord {
				_, rpcErr = h.projectPatchQueueOperatorEnablementRecord(ctx, mustJSONRaw(projectPatchQueueOperatorEnablementRecordParams{
					WorkspaceID:        "ws-operator-enable-preflight",
					ProjectID:          "missing-project",
					ActorID:            tc.actorID,
					QueueID:            "missing-queue",
					ItemID:             "missing-item",
					OperatorEnablement: repoauthority.PatchQueueOperatorEnablement{Enabled: true, Reason: "preflight"},
					ClaimToken:         "missing-claim",
				}))
			} else {
				_, rpcErr = h.operatorPatchQueueEnable(ctx, mustJSONRaw(operatorPatchQueueEnableParams{
					WorkspaceID: "ws-operator-enable-preflight",
					ProjectID:   "missing-project",
					ActorID:     tc.actorID,
					QueueID:     "missing-queue",
					ItemID:      "missing-item",
					ClaimToken:  "missing-claim",
					Reason:      "preflight",
				}))
			}

			if rpcErr == nil {
				t.Fatalf("expected permission denial before storage lookup")
			}
			if rpcErr.Code != tc.wantCode || !strings.Contains(rpcErr.Message, tc.wantErr) {
				t.Fatalf("expected code=%d containing %q, got %+v", tc.wantCode, tc.wantErr, rpcErr)
			}
		})
	}
}
