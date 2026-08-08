package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/tools"
)

var validIDRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-\.]+$`)

func checkJSONDepth(data []byte, maxDepth int) error {
	depth := 0
	inString := false
	escape := false
	for _, b := range data {
		if escape {
			escape = false
			continue
		}
		if inString {
			if b == '\\' {
				escape = true
			} else if b == '"' {
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > maxDepth {
				return errors.New("json payload too deep")
			}
		case '}', ']':
			depth--
		}
	}
	return nil
}

func rpcRequestMethodFromBody(data []byte) (string, error) {
	var probe struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return "", err
	}
	return strings.TrimSpace(probe.Method), nil
}

// RPCRequest is a JSON-RPC 2.0 request.
type RPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

// RPCResponse is a JSON-RPC 2.0 response.
type RPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	Result  any       `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
	ID      any       `json:"id"`
}

// RPCError is a JSON-RPC 2.0 error.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

const (
	errCodeParse                 = -32700
	errCodeInvalidRequest        = -32600
	errCodeMethodNotFound        = -32601
	errCodeInvalidParams         = -32602
	errCodeInternal              = -32603
	errCodePermissionDenied      = -32001
	errCodeDocumentConflict      = -32020
	errCodeInvalidPollCursor     = -32021
	errCodeOperatorQueueNotFound = -32022

	rpcDefaultMaxRequestBodyBytes         = int64(1 << 20)
	rpcMaterializationMaxRequestBodyBytes = sqlite.ProjectPatchQueueMaterializationMaxJSONBytes + int64(4<<20)
	rpcLargeBodyMaterializationMethod     = "project.patch_queue.materialization_record"
)

// Handler is the HTTP handler for the JSON-RPC server.
type Handler struct {
	store     *sqlite.Store
	mcpStore  *mcp.Store
	mcpClient *mcp.Client
	toolExec  *tools.Executor
	eventBus  *EventBus

	listOperatorQueueItemsOverride            func(context.Context, sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error)
	beforeRebaseRollbackFailureUpsertOverride func(context.Context, sqlite.OperatorQueueRecord)
	beforeRebaseRollbackFailureCreateOverride func(context.Context, string)
	beforeWorkspaceOpsUpsertStoreOverride     func(context.Context)
	beforeWorkspaceOpsResolveStoreOverride    func(context.Context)
	beforeWorkspaceOpsEscalateStoreOverride   func(context.Context)
	beforeActionStartSyncOverride             func(context.Context)
	beforeActionPauseSyncOverride             func(context.Context)
	beforeActionResolveQueueEffectsOverride   func(context.Context)
	beforeActionCreateQueueEffectsOverride    func(context.Context)
	beforeRSPAnomalyRollbackResolveOverride   func(context.Context, string)
}

// NewHandler creates a new Handler backed by the given store.
func NewHandler(store *sqlite.Store) *Handler {
	cfg := app.LoadConfig()
	eb := NewEventBus()

	// Register RSP-1.2 Motif Detectors Middleware
	md := NewMotifDetectors(store, eb)
	eb.RegisterMiddleware(md.Middleware)

	h := &Handler{
		store:     store,
		mcpStore:  mcp.NewStore(store.WriteDB()),
		mcpClient: mcp.NewClient(),
		toolExec:  tools.NewExecutor(cfg.WorkspaceRoot),
		eventBus:  eb,
	}
	store.SetRSPFirehoseLiveMirror(func(event sqlite.RuntimeEventRecord) {
		eventType := strings.TrimSpace(event.EventType)
		if eventType != "ANOMALY_ALERT" && eventType != "TENSION_HINT" {
			return
		}
		h.publishRuntimeEventRecord(event, "RSP anomaly alert")
	})
	return h
}

// GetEventBus returns the event bus for external subscribers (e.g., agent loop).
func (h *Handler) GetEventBus() *EventBus { return h.eventBus }

var rpcLogSem = make(chan struct{}, 50)

// ServeHTTP handles POST /rpc requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	meta := RequestMetadata{
		ClientIP:  requestClientIP(r),
		UserAgent: strings.TrimSpace(r.UserAgent()),
	}
	ctx := context.WithValue(r.Context(), requestMetadataContextKey{}, meta)
	r = r.WithContext(ctx)

	body, err := io.ReadAll(io.LimitReader(r.Body, rpcMaterializationMaxRequestBodyBytes+1))
	if err != nil {
		writeRPCError(w, nil, errCodeParse, "failed to read request body")
		return
	}
	if int64(len(body)) > rpcMaterializationMaxRequestBodyBytes {
		writeRPCError(w, nil, errCodeInvalidRequest, fmt.Sprintf("request body exceeds maximum allowed size of %d bytes", rpcMaterializationMaxRequestBodyBytes))
		return
	}

	if len(body) == 0 {
		writeRPCError(w, nil, errCodeParse, "empty request body")
		return
	}

	if !utf8.Valid(body) {
		writeRPCError(w, nil, errCodeParse, "request body contains invalid UTF-8. If your client cannot send UTF-8, write your content in English (ASCII-only)")
		return
	}

	if err := checkJSONDepth(body, 100); err != nil {
		writeRPCError(w, nil, errCodeParse, "JSON payload exceeds maximum allowed depth")
		return
	}
	if int64(len(body)) > rpcDefaultMaxRequestBodyBytes {
		method, err := rpcRequestMethodFromBody(body)
		if err != nil {
			writeRPCError(w, nil, errCodeInvalidRequest, fmt.Sprintf("request body exceeds default maximum allowed size of %d bytes and method could not be validated", rpcDefaultMaxRequestBodyBytes))
			return
		}
		if method != rpcLargeBodyMaterializationMethod {
			writeRPCError(w, nil, errCodeInvalidRequest, fmt.Sprintf("request body exceeds default maximum allowed size of %d bytes for method %q", rpcDefaultMaxRequestBodyBytes, method))
			return
		}
	}

	var req RPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeRPCError(w, nil, errCodeParse, "invalid JSON")
		return
	}

	if req.JSONRPC != "2.0" {
		writeRPCError(w, req.ID, errCodeInvalidRequest, "jsonrpc must be \"2.0\"")
		return
	}

	rpcTimeout := 30 * time.Second
	if req.Method == "tool.call" {
		rpcTimeout = tools.DefaultCallTimeout // tool.call may invoke long-running tools (e.g. NotebookLM)
	}
	ctx, cancel := context.WithTimeout(r.Context(), rpcTimeout)
	defer cancel()

	start := time.Now()
	result, rpcErr := h.dispatch(ctx, req.Method, req.Params)
	latencyMs := time.Since(start).Milliseconds()

	// Async log RPC call (skip noise: polling, list ops, bootstrap)
	skipMethods := map[string]bool{
		"rpc.logs.list": true, "agent.bootstrap": true,
		"agent.work.next": true, "agent.task.hydrate": true,
		"workspace.memory.packet.kernel.get": true, "workspace.memory.packet.shell.get": true,
		"workspace.memory.node.search": true, "workspace.memory.pack.list": true,
		"workspace.memory.promotion.list": true, "workspace.memory.metrics.list": true,
		"workspace.memory.coherence.report": true, "workspace.memory.coherence.scope": true,
		"workspace.rsp.belief.report": true, "workspace.rsp.belief.claim": true, "workspace.rsp.state.report": true,
		"workspace.rsp.forecast.report":      true,
		"workspace.rsp.capability.get":       true,
		"workspace.memory.invalidation.list": true, "workspace.memory.invalidation.cursor.get": true,
		"agent.request.list": true, "agent.request.open.list": true, "agent.message.poll": true,
		"workspace.agents.list": true, "workspace.humans.list": true, "workspace.doc.list": true,
		"workspace.artifact.list": true, "workspace.segment.list": true,
		"workspace.tasks.list": true, "workspace.messages.list": true,
		"workspace.updates.list": true, "workspace.sessions.list": true, "tool.list": true,
		"mcp.server.list": true, "action.list": true,
		"vault.list": true, "vault.audit": true,
		"project.profile.get": true, "project.gates.status": true, "project.coordination.get": true, "project.roles.list": true,
		"project.branches.list": true, "project.patch_queue.list": true, "project.governance.predicates.check": true,
		"project.governance.challenge.get": true, "project.governance.challenge.list": true, "project.governance.votes.list": true,
	}
	if !skipMethods[req.Method] {
		rpcLogSem <- struct{}{}
		go func() {
			defer func() { <-rpcLogSem }()
			status := "ok"
			errMsg := ""
			if rpcErr != nil {
				status = "error"
				errMsg = rpcErr.Message
			}
			// Extract workspace_id and actor from params for context
			var pMap map[string]any
			_ = json.Unmarshal(req.Params, &pMap)
			wsID, _ := pMap["workspace_id"].(string)
			actor, _ := pMap["from_agent_id"].(string)
			if actor == "" {
				actor, _ = pMap["actor"].(string)
			}
			if actor == "" {
				actor, _ = pMap["agent_id"].(string)
			}
			// Skip logging dashboard noise (no actor + no error)
			if actor == "" && status == "ok" {
				return
			}
			_ = h.store.RecordRPCAccess(context.Background(), sqlite.RPCAccessLogInput{
				Method:      req.Method,
				WorkspaceID: wsID,
				Actor:       actor,
				Status:      status,
				ErrorMsg:    errMsg,
				LatencyMs:   latencyMs,
				CreatedAt:   time.Now().UTC(),
			})
		}()
	}

	if rpcErr != nil {
		// Enrich error with self-help hints
		if rpcErr.Details == nil {
			hints := map[string]any{
				"hint": "Use rpc.describe to see valid params: {\"method\":\"rpc.describe\",\"params\":{\"method\":\"" + req.Method + "\"}}",
			}
			if req.Method != "tool.call" {
				hints["oracle"] = "Ask KB Oracle for help: {\"method\":\"tool.call\",\"params\":{\"tool_id\":\"kb-engine\",\"workspace_id\":\"rhizome-main\",\"arguments\":{\"action\":\"ask\",\"kb_name\":\"rhizome-oracle\",\"question\":\"How to use " + req.Method + "?\"}}}"
			}
			rpcErr.Details = hints
		}
		writeRPCResponse(w, RPCResponse{
			JSONRPC: "2.0",
			Error:   rpcErr,
			ID:      req.ID,
		})
		return
	}

	writeRPCResponse(w, RPCResponse{
		JSONRPC: "2.0",
		Result:  result,
		ID:      req.ID,
	})
}

func (h *Handler) dispatch(ctx context.Context, method string, params json.RawMessage) (any, *RPCError) {
	if rpcErr := rejectResponderOriginDisallowedRPC(ctx, method); rpcErr != nil {
		return nil, rpcErr
	}
	switch method {
	// Agent operations
	case "agent.register":
		return h.agentRegister(ctx, params)
	case "agent.heartbeat":
		return h.agentHeartbeat(ctx, params)
	case "agent.bootstrap":
		return h.agentBootstrap(ctx, params)
	case "agent.work.next":
		return h.agentWorkNext(ctx, params)
	case "agent.task.hydrate":
		return h.agentTaskHydrate(ctx, params)
	case "agent.task_frontier.decision":
		return h.agentTaskFrontierDecision(ctx, params)
	case "agent.update.post":
		return h.agentUpdatePost(ctx, params)
	case "agent.session.start":
		return h.agentSessionStart(ctx, params)
	case "agent.session.status":
		return h.agentSessionStatus(ctx, params)
	case "agent.session.blocked":
		return h.agentSessionBlocked(ctx, params)
	case "agent.session.decision_needed":
		return h.agentSessionDecisionNeeded(ctx, params)
	case "agent.session.keepalive":
		return h.agentSessionKeepalive(ctx, params)
	case "agent.session.end":
		return h.agentSessionEnd(ctx, params)
	case "agent.session.takeover":
		return h.agentSessionTakeover(ctx, params)
	case "agent.task.claim":
		return h.agentTaskClaim(ctx, params)
	case "agent.task.release":
		return h.agentTaskRelease(ctx, params)
	case "agent.task.complete":
		return h.agentTaskComplete(ctx, params)
	case "agent.task.block":
		return h.agentTaskBlock(ctx, params)
	case "agent.node.claim":
		return h.agentNodeClaim(ctx, params)
	case "agent.node.release":
		return h.agentNodeRelease(ctx, params)
	case "agent.node.complete":
		return h.agentNodeComplete(ctx, params)
	case "agent.profile.update":
		return h.agentProfileUpdate(ctx, params)
	case "agent.profile.get":
		return h.agentProfileGet(ctx, params)
	case "agent.delete":
		return h.agentDelete(ctx, params)

	// Coalition Engine
	case "coalition.offer":
		return h.coalitionOffer(ctx, params)
	case "coalition.leave":
		return h.coalitionLeave(ctx, params)
	case "coalition.status":
		return h.coalitionStatus(ctx, params)
	case "coalition.seek":
		return h.coalitionSeek(ctx, params)
	case "coalition.invite":
		return h.coalitionInvite(ctx, params)
	case "coalition.kick":
		return h.coalitionKick(ctx, params)

	// Reviewer Mesh
	case "reviewer.route":
		return h.reviewerRoute(ctx, params)
	case "reviewer.scarcity":
		return h.reviewerScarcity(ctx, params)

	// Auth / Security operations
	case "workspace.auth.agent.register":
		return h.workspaceAuthAgentRegister(ctx, params)
	case "workspace.auth.human.register":
		return h.workspaceAuthHumanRegister(ctx, params)
	case "workspace.auth.human.login":
		return h.workspaceAuthHumanLogin(ctx, params)
	case "workspace.auth.human.profile.get":
		return h.workspaceAuthHumanProfileGet(ctx, params)
	case "workspace.auth.human.profile.update":
		return h.workspaceAuthHumanProfileUpdate(ctx, params)
	case "workspace.auth.human.sessions.list":
		return h.workspaceAuthHumanSessionsList(ctx, params)
	case "workspace.auth.human.sessions.revoke":
		return h.workspaceAuthHumanSessionsRevoke(ctx, params)
	case "workspace.auth.agent.update":
		return h.workspaceAuthAgentUpdate(ctx, params)
	case "workspace.auth.agent.token.rotate":
		return h.workspaceAuthAgentTokenRotate(ctx, params)
	case "workspace.security.password.update":
		return h.workspaceSecurityPasswordUpdate(ctx, params)
	case "workspace.security.audit.list":
		return h.workspaceSecurityAuditList(ctx, params)

	// Task operations
	case "task.submit":
		return h.taskSubmit(ctx, params)
	case "task.status":
		return h.taskStatus(ctx, params)
	case "task.class.put":
		return h.taskClassPut(ctx, params)
	case "task.project_fields.put":
		return h.taskProjectFieldsPut(ctx, params)
	case "task.close":
		return h.taskClose(ctx, params)

	// Project operations
	case "project.create":
		return h.projectCreate(ctx, params)
	case "project.list":
		return h.projectList(ctx, params)
	case "project.get":
		return h.projectGet(ctx, params)
	case "project.update":
		return h.projectUpdate(ctx, params)
	case "project.delete":
		return h.projectDelete(ctx, params)
	case "project.profile.get":
		return h.projectProfileGet(ctx, params)
	case "project.profile.update":
		return h.projectProfileUpdate(ctx, params)
	case "project.phase.transition":
		return h.projectPhaseTransition(ctx, params)
	case "project.gates.status":
		return h.projectGatesStatus(ctx, params)
	case "project.coordination.get":
		return h.projectCoordinationGet(ctx, params)
	case "project.lead.claim":
		return h.projectLeadClaim(ctx, params)
	case "project.lead.renew":
		return h.projectLeadRenew(ctx, params)
	case "project.lead.release":
		return h.projectLeadRelease(ctx, params)
	case "project.lead.transfer":
		return h.projectLeadTransfer(ctx, params)
	case "project.role.assign":
		return h.projectRoleAssign(ctx, params)
	case "project.roles.list":
		return h.projectRolesList(ctx, params)
	case "project.governance.predicates.check":
		return h.projectGovernancePredicatesCheck(ctx, params)
	case "project.governance.challenge.raise":
		return h.projectGovernanceChallengeRaise(ctx, params)
	case "project.governance.challenge.defend":
		return h.projectGovernanceChallengeDefend(ctx, params)
	case "project.governance.vote.cast":
		return h.projectGovernanceVoteCast(ctx, params)
	case "project.governance.challenge.tally":
		return h.projectGovernanceChallengeTally(ctx, params)
	case "project.governance.challenge.get":
		return h.projectGovernanceChallengeGet(ctx, params)
	case "project.governance.challenge.list":
		return h.projectGovernanceChallengeList(ctx, params)
	case "project.governance.votes.list":
		return h.projectGovernanceVotesList(ctx, params)
	case "project.repository.upsert":
		return h.projectRepositoryUpsert(ctx, params)
	case "project.repositories.list":
		return h.projectRepositoriesList(ctx, params)
	case "project.checkout.register":
		return h.projectCheckoutRegister(ctx, params)
	case "project.checkouts.list":
		return h.projectCheckoutsList(ctx, params)
	case "project.branch.register":
		return h.projectBranchRegister(ctx, params)
	case "project.branches.list":
		return h.projectBranchesList(ctx, params)
	case "project.patch_queue.submit":
		return h.projectPatchQueueSubmit(ctx, params)
	case "project.patch_queue.supersede":
		return h.projectPatchQueueSupersede(ctx, params)
	case "project.patch_queue.claim":
		return h.projectPatchQueueClaim(ctx, params)
	case "project.patch_queue.operation_bind":
		return h.projectPatchQueueOperationBind(ctx, params)
	case "project.patch_queue.cas_record":
		return h.projectPatchQueueCASRecord(ctx, params)
	case "project.patch_queue.materialization_record":
		return h.projectPatchQueueMaterializationRecord(ctx, params)
	case "project.patch_queue.rollback_record":
		return h.projectPatchQueueRollbackRecord(ctx, params)
	case "project.patch_queue.integration_record":
		return h.projectPatchQueueIntegrationRecord(ctx, params)
	case "project.patch_queue.integration_repair":
		return h.projectPatchQueueIntegrationRepair(ctx, params)
	case "project.patch_queue.reviewer_advisory_record":
		return h.projectPatchQueueReviewerAdvisoryRecord(ctx, params)
	case "project.patch_queue.operator_enablement_record":
		return h.projectPatchQueueOperatorEnablementRecord(ctx, params)
	case "operator.patch_queue.enable":
		return h.operatorPatchQueueEnable(ctx, params)
	case "project.patch_queue.release":
		return h.projectPatchQueueRelease(ctx, params)
	case "project.patch_queue.decision":
		return h.projectPatchQueueDecision(ctx, params)
	case "project.patch_queue.review_task.reconcile":
		return h.projectPatchQueueReviewTaskReconcile(ctx, params)
	case "project.patch_queue.decision_continuation.consume":
		return h.projectPatchQueueDecisionContinuationConsume(ctx, params)
	case "project.patch_queue.list":
		return h.projectPatchQueueList(ctx, params)

	// Service venture operations
	case "service.direction.upsert":
		return h.serviceDirectionUpsert(ctx, params)
	case "service.direction.list":
		return h.serviceDirectionList(ctx, params)
	case "service.direction.get":
		return h.serviceDirectionGet(ctx, params)
	case "service.candidate.upsert":
		return h.serviceCandidateUpsert(ctx, params)
	case "service.candidate.list":
		return h.serviceCandidateList(ctx, params)
	case "service.candidate.get":
		return h.serviceCandidateGet(ctx, params)
	case "service.run.start":
		return h.serviceRunStart(ctx, params)
	case "service.run.update":
		return h.serviceRunUpdate(ctx, params)
	case "service.run.list":
		return h.serviceRunList(ctx, params)
	case "service.run.get":
		return h.serviceRunGet(ctx, params)
	case "service.approval.grant":
		return h.serviceApprovalGrant(ctx, params)
	case "service.resource.record":
		return h.serviceResourceRecord(ctx, params)
	case "service.spend.record":
		return h.serviceSpendRecord(ctx, params)
	case "service.revenue.record":
		return h.serviceRevenueRecord(ctx, params)
	case "service.outcome.record":
		return h.serviceOutcomeRecord(ctx, params)
	case "service.coordination.get":
		return h.serviceCoordinationGet(ctx, params)

	// Workspace operations
	case "workspace.control.epoch.tick":
		return h.workspaceControlEpochTick(ctx, params)
	case "workspace.doc.put":
		return h.workspaceDocPut(ctx, params)
	case "workspace.doc.get":
		return h.workspaceDocGet(ctx, params)
	case "workspace.doc.list":
		return h.workspaceDocList(ctx, params)
	case "workspace.artifact.write":
		return h.workspaceArtifactWrite(ctx, params)
	case "workspace.artifact.list":
		return h.workspaceArtifactList(ctx, params)
	case "workspace.segment.list":
		return h.workspaceSegmentList(ctx, params)
	case "workspace.segment.get":
		return h.workspaceSegmentGet(ctx, params)
	case "workspace.doc.archive":
		return h.workspaceDocArchive(ctx, params)
	case "workspace.doc.delete":
		return h.workspaceDocDelete(ctx, params)
	case "workspace.doc.history":
		return h.workspaceDocHistory(ctx, params)
	case "workspace.graph.snapshot":
		return h.workspaceGraphSnapshot(ctx, params)
	case "workspace.memory.write":
		return h.workspaceMemoryWrite(ctx, params)
	case "workspace.memory.node.write":
		return h.workspaceMemoryNodeWrite(ctx, params)
	case "workspace.memory.node.touch":
		return h.workspaceMemoryNodeTouch(ctx, params)
	case "workspace.memory.list":
		return h.workspaceMemoryList(ctx, params)
	case "workspace.memory.search":
		return h.workspaceMemorySearch(ctx, params)
	case "workspace.memory.packet.kernel.get":
		return h.workspaceMemoryPacketKernel(ctx, params)
	case "workspace.memory.packet.shell.get":
		return h.workspaceMemoryPacketShell(ctx, params)
	case "workspace.memory.pack.write":
		return h.workspaceMemoryPackWrite(ctx, params)
	case "workspace.memory.promotion.enqueue":
		return h.workspaceMemoryPromotionEnqueue(ctx, params)
	case "workspace.memory.promotion.resolve":
		return h.workspaceMemoryPromotionResolve(ctx, params)
	case "workspace.memory.promotion.get":
		return h.workspaceMemoryPromotionGet(ctx, params)
	case "workspace.memory.promotion.list":
		return h.workspaceMemoryPromotionList(ctx, params)
	case "workspace.memory.pack.list":
		return h.workspaceMemoryPackList(ctx, params)
	case "workspace.memory.pack.get":
		return h.workspaceMemoryPackGet(ctx, params)
	case "workspace.memory.remove":
		return h.workspaceMemoryRemove(ctx, params)
	case "workspace.memory.restore":
		return h.workspaceMemoryRestore(ctx, params)
	case "workspace.memory.graph.list":
		return h.workspaceMemoryGraphList(ctx, params)
	case "workspace.memory.graph.get":
		return h.workspaceMemoryGraphGet(ctx, params)
	case "workspace.memory.graph.atlas":
		return h.workspaceMemoryGraphAtlas(ctx, params)
	case "workspace.memory.graph.sync":
		return h.workspaceMemoryGraphSync(ctx, params)
	case "workspace.memory.graph.repair":
		return h.workspaceMemoryGraphRepair(ctx, params)
	case "workspace.memory.node.search":
		return h.workspaceMemoryNodeSearch(ctx, params)
	case "workspace.memory.residency.report":
		return h.workspaceMemoryResidencyReport(ctx, params)
	case "workspace.memory.residency.list":
		return h.workspaceMemoryResidencyList(ctx, params)
	case "workspace.memory.residency.get":
		return h.workspaceMemoryResidencyGet(ctx, params)
	case "workspace.memory.metrics.report":
		return h.workspaceMemoryMetricsReport(ctx, params)
	case "workspace.memory.metrics.list":
		return h.workspaceMemoryMetricsList(ctx, params)
	case "workspace.memory.metrics.get":
		return h.workspaceMemoryMetricsGet(ctx, params)
	case "workspace.memory.coherence.report":
		return h.workspaceMemoryCoherenceReport(ctx, params)
	case "workspace.memory.coherence.scope":
		return h.workspaceMemoryCoherenceScope(ctx, params)
	case "workspace.memory.coherence.snapshot":
		return h.workspaceMemoryCoherenceSnapshot(ctx, params)
	case "workspace.rsp.belief.report":
		return h.workspaceRSPBeliefReport(ctx, params)
	case "workspace.rsp.belief.claim":
		return h.workspaceRSPBeliefClaim(ctx, params)
	case "workspace.rsp.belief.snapshot":
		return h.workspaceRSPBeliefSnapshot(ctx, params)
	case "workspace.rsp.capability.get":
		return h.workspaceRSPCapabilityGet(ctx, params)
	case "workspace.rsp.capability.put":
		return h.workspaceRSPCapabilityPut(ctx, params)
	case "workspace.rsp.state.report":
		return h.workspaceRSPStateReport(ctx, params)
	case "workspace.rsp.state.snapshot":
		return h.workspaceRSPStateSnapshot(ctx, params)
	case "workspace.rsp.forecast.report":
		return h.workspaceRSPForecastReport(ctx, params)
	case "workspace.rsp.forecast.snapshot":
		return h.workspaceRSPForecastSnapshot(ctx, params)
	case "workspace.rsp.telemetry.dump":
		return h.workspaceRSPTelemetryDump(ctx, params)
	case "workspace.memory.invalidation.poll":
		return h.workspaceMemoryInvalidationPoll(ctx, params)
	case "workspace.memory.invalidation.ack":
		return h.workspaceMemoryInvalidationAck(ctx, params)
	case "workspace.memory.invalidation.fail":
		return h.workspaceMemoryInvalidationFail(ctx, params)
	case "workspace.memory.invalidation.requeue":
		return h.workspaceMemoryInvalidationRequeue(ctx, params)
	case "workspace.memory.invalidation.list":
		return h.workspaceMemoryInvalidationList(ctx, params)
	case "workspace.memory.invalidation.get":
		return h.workspaceMemoryInvalidationGet(ctx, params)
	case "workspace.memory.invalidation.cursor.get":
		return h.workspaceMemoryInvalidationCursorGet(ctx, params)
	case "workspace.episode.pack.list":
		return h.workspaceEpisodePackList(ctx, params)
	case "workspace.episode.pack.get":
		return h.workspaceEpisodePackGet(ctx, params)
	case "workspace.episode.pack.sync":
		return h.workspaceEpisodePackSync(ctx, params)
	case "workspace.events.list":
		return h.workspaceEventsList(ctx, params)
	case "workspace.events.replay":
		return h.workspaceEventsReplay(ctx, params)
	case "workspace.events.evaluate":
		return h.workspaceEventsEvaluate(ctx, params)
	case "workspace.authority.status":
		return h.workspaceAuthorityStatus(ctx, params)
	case "workspace.authority.ensure_local":
		return h.workspaceAuthorityEnsureLocal(ctx, params)
	case "workspace.authority.force_break":
		return h.workspaceAuthorityForceBreak(ctx, params)
	case "workspace.instrumentation.report":
		return h.workspaceInstrumentationReport(ctx, params)
	case "workspace.instrumentation.clusters":
		return h.workspaceInstrumentationClusters(ctx, params)
	case "workspace.instrumentation.snapshot":
		return h.workspaceInstrumentationSnapshot(ctx, params)
	case "workspace.instrumentation.locus.bundle":
		return h.workspaceInstrumentationLocusBundle(ctx, params)
	case "workspace.instrumentation.control.report":
		return h.workspaceInstrumentationControlReport(ctx, params)
	case "workspace.instrumentation.control.cluster":
		return h.workspaceInstrumentationControlCluster(ctx, params)
	case "workspace.instrumentation.control.snapshot":
		return h.workspaceInstrumentationControlSnapshot(ctx, params)
	case "workspace.instrumentation.corridor.report":
		return h.workspaceInstrumentationCorridorReport(ctx, params)
	case "workspace.instrumentation.corridor.cluster":
		return h.workspaceInstrumentationCorridorCluster(ctx, params)
	case "workspace.instrumentation.corridor.snapshot":
		return h.workspaceInstrumentationCorridorSnapshot(ctx, params)
	case "workspace.instrumentation.corridor.fit.report":
		return h.workspaceInstrumentationCorridorFitReport(ctx, params)
	case "workspace.instrumentation.corridor.fit.cluster":
		return h.workspaceInstrumentationCorridorFitCluster(ctx, params)
	case "workspace.instrumentation.corridor.fit.snapshot":
		return h.workspaceInstrumentationCorridorFitSnapshot(ctx, params)
	case "workspace.instrumentation.corridor.ownership.report":
		return h.workspaceInstrumentationCorridorOwnershipReport(ctx, params)
	case "workspace.instrumentation.corridor.ownership.cluster":
		return h.workspaceInstrumentationCorridorOwnershipCluster(ctx, params)
	case "workspace.instrumentation.corridor.ownership.snapshot":
		return h.workspaceInstrumentationCorridorOwnershipSnapshot(ctx, params)
	case "workspace.instrumentation.corridor.boundary.report":
		return h.workspaceInstrumentationCorridorBoundaryReport(ctx, params)
	case "workspace.instrumentation.corridor.boundary.cluster":
		return h.workspaceInstrumentationCorridorBoundaryCluster(ctx, params)
	case "workspace.instrumentation.corridor.authority.report":
		return h.workspaceInstrumentationCorridorAuthorityReport(ctx, params)
	case "workspace.instrumentation.corridor.authority.task":
		return h.workspaceInstrumentationCorridorAuthorityTask(ctx, params)
	case "workspace.instrumentation.control.state.report":
		return h.workspaceInstrumentationControlStateReport(ctx, params)
	case "workspace.instrumentation.control.state.cluster":
		return h.workspaceInstrumentationControlStateCluster(ctx, params)
	case "workspace.instrumentation.control.state.tick":
		return h.workspaceInstrumentationControlStateTick(ctx, params)
	case "workspace.instrumentation.control.state.snapshot":
		return h.workspaceInstrumentationControlStateSnapshot(ctx, params)
	case "workspace.instrumentation.unified.control.report":
		return h.workspaceInstrumentationUnifiedControlReport(ctx, params)
	case "workspace.instrumentation.unified.control.snapshot":
		return h.workspaceInstrumentationUnifiedControlSnapshot(ctx, params)
	case "workspace.tension.refresh":
		return h.workspaceTensionRefresh(ctx, params)
	case "workspace.tension.list":
		return h.workspaceTensionList(ctx, params)
	case "workspace.tension.get":
		return h.workspaceTensionGet(ctx, params)
	case "workspace.tension.frontier":
		return h.workspaceTensionFrontier(ctx, params)
	case "workspace.tension.confirm":
		return h.workspaceTensionConfirm(ctx, params)
	case "workspace.tension.discard":
		return h.workspaceTensionDiscard(ctx, params)
	case "workspace.tension.archive":
		return h.workspaceTensionArchive(ctx, params)
	case "workspace.tension.lifecycle.update":
		return h.workspaceTensionLifecycleUpdate(ctx, params)
	case "workspace.tension.resolve":
		return h.workspaceTensionResolve(ctx, params)
	case "workspace.tension.dormant":
		return h.workspaceTensionDormant(ctx, params)
	case "workspace.tension.add.dependency":
		return h.workspaceTensionAddDependency(ctx, params)
	case "workspace.tension.remove.dependency":
		return h.workspaceTensionRemoveDependency(ctx, params)
	case "workspace.tension.condense":
		return h.workspaceTensionCondense(ctx, params)
	case "workspace.tension.attachable.list":
		return h.workspaceTensionListAttachable(ctx, params)
	case "workspace.tension.agent.attach":
		return h.workspaceTensionAttachAgent(ctx, params)
	case "workspace.tension.agent.detach":
		return h.workspaceTensionDetachAgent(ctx, params)
	case "workspace.ops.upsert":
		return h.workspaceOpsUpsert(ctx, params)
	case "workspace.ops.request":
		return h.workspaceOpsRequest(ctx, params)
	case "workspace.ops.list":
		return h.workspaceOpsList(ctx, params)
	case "workspace.ops.get":
		return h.workspaceOpsGet(ctx, params)
	case "workspace.ops.resolve":
		return h.workspaceOpsResolve(ctx, params)
	case "workspace.ops.escalate":
		return h.workspaceOpsEscalate(ctx, params)
	case "workspace.claim.write":
		return h.workspaceClaimWrite(ctx, params)
	case "workspace.claim.list":
		return h.workspaceClaimList(ctx, params)
	case "workspace.claim.links.list":
		return h.workspaceClaimLinksList(ctx, params)
	case "workspace.claim.search":
		return h.workspaceClaimSearch(ctx, params)
	case "workspace.claim.archive":
		return h.workspaceClaimArchive(ctx, params)
	case "workspace.claim.review":
		return h.workspaceClaimReview(ctx, params)
	case "workspace.claim.confirm":
		return h.workspaceClaimConfirm(ctx, params)
	case "workspace.claim.dispute":
		return h.workspaceClaimDispute(ctx, params)
	case "workspace.claim.supersede":
		return h.workspaceClaimSupersede(ctx, params)
	case "workspace.claim.stale":
		return h.workspaceClaimStale(ctx, params)
	case "workspace.claim.escalate":
		return h.workspaceClaimEscalate(ctx, params)
	case "workspace.execution.run.write":
		return h.workspaceExecutionRunWrite(ctx, params)
	case "workspace.execution.run.list":
		return h.workspaceExecutionRunList(ctx, params)
	case "workspace.execution.run.get":
		return h.workspaceExecutionRunGet(ctx, params)
	case "workspace.execution.agent_runs.cancel":
		return h.workspaceExecutionAgentRunsCancel(ctx, params)
	case "workspace.execution.step.write":
		return h.workspaceExecutionStepWrite(ctx, params)
	case "workspace.policy.put":
		return h.workspacePolicyPut(ctx, params)
	case "workspace.policy.list":
		return h.workspacePolicyList(ctx, params)
	case "workspace.policy.check":
		return h.workspacePolicyCheck(ctx, params)
	case "workspace.control.command.request":
		return h.workspaceControlCommandRequest(ctx, params)
	case "workspace.search":
		return h.workspaceSearch(ctx, params)
	case "workspace.nodes.list":
		return h.workspaceNodesList(ctx, params)
	case "workspace.updates.list":
		return h.workspaceUpdatesList(ctx, params)
	case "workspace.agents.list":
		return h.workspaceAgentsList(ctx, params)
	case "workspace.humans.list":
		return h.workspaceHumansList(ctx, params)
	case "workspace.sessions.list":
		return h.workspaceSessionsList(ctx, params)
	case "workspace.agents.search":
		return h.workspaceAgentsSearch(ctx, params)
	case "workspace.tasks.list":
		return h.workspaceTasksList(ctx, params)
	case "workspace.messages.list":
		return h.workspaceMessagesList(ctx, params)
	case "workspace.compaction.candidates":
		return h.workspaceCompactionCandidates(ctx, params)
	case "workspace.compaction.snapshots":
		return h.workspaceCompactionSnapshots(ctx, params)
	case "workspace.webhook.register":
		return h.workspaceWebhookRegister(ctx, params)
	case "workspace.webhook.list":
		return h.workspaceWebhookList(ctx, params)
	case "workspace.webhook.remove":
		return h.workspaceWebhookRemove(ctx, params)

	// Tool operations
	case "tool.list":
		return h.toolList(ctx, params)
	case "tool.register":
		return h.toolRegister(ctx, params)
	case "tool.status":
		return h.toolStatus(ctx, params)
	case "tool.remove":
		return h.toolRemove(ctx, params)
	case "tool.deploy":
		return h.toolDeploy(ctx, params)
	case "tool.call":
		return h.toolCall(ctx, params)
	case "tool.undeploy":
		return h.toolUndeploy(ctx, params)

	// Human Actions (Action Required)
	case "action.create":
		return h.actionCreate(ctx, params)
	case "action.list":
		return h.actionList(ctx, params)
	case "action.start":
		return h.actionStart(ctx, params)
	case "action.pause":
		return h.actionPause(ctx, params)
	case "action.resolve":
		return h.actionResolve(ctx, params)
	case "action.chat.send":
		return h.actionChatSend(ctx, params)
	case "action.chat.list":
		return h.actionChatList(ctx, params)

	// Events
	case "event.emit":
		return h.eventEmit(ctx, params)

	// MCP operations
	case "mcp.server.register":
		return h.mcpServerRegister(ctx, params)
	case "mcp.server.list":
		return h.mcpServerList(ctx, params)
	case "mcp.server.remove":
		return h.mcpServerRemove(ctx, params)
	case "mcp.tool.discover":
		return h.mcpToolDiscover(ctx, params)
	case "mcp.tool.call":
		return h.mcpToolCall(ctx, params)
	case "mcp.tool.list":
		return h.mcpToolList(ctx, params)

	// Agent messaging
	case "agent.message.send":
		return h.agentMessageSend(ctx, params)
	case "agent.message.poll":
		return h.agentMessagePoll(ctx, params)
	case "agent.message.ack":
		return h.agentMessageAck(ctx, params)

	// Agent state (memory)
	case "agent.state.get":
		return h.agentStateGet(ctx, params)
	case "agent.state.set":
		return h.agentStateSet(ctx, params)
	case "agent.state.list":
		return h.agentStateList(ctx, params)
	case "agent.state.delete":
		return h.agentStateDelete(ctx, params)
	case "agent.heartbeat_lease.acquire":
		return h.agentHeartbeatLeaseAcquire(ctx, params)
	case "agent.heartbeat_lease.refresh":
		return h.agentHeartbeatLeaseRefresh(ctx, params)
	case "agent.heartbeat_lease.release":
		return h.agentHeartbeatLeaseRelease(ctx, params)

	// Vault (secrets / credentials)
	case "vault.create":
		return h.vaultCreate(ctx, params)
	case "vault.update":
		return h.vaultUpdate(ctx, params)
	case "vault.delete":
		return h.vaultDelete(ctx, params)
	case "vault.list":
		return h.vaultList(ctx, params)
	case "vault.get":
		return h.vaultGet(ctx, params)
	case "vault.audit":
		return h.vaultAudit(ctx, params)

	// RPC Schema
	case "rpc.describe":
		return h.rpcDescribe(ctx, params)
	case "rpc.methods.list":
		return h.rpcMethodsList(ctx, params)

	// Runtime build identity
	case "runtime.build.info":
		return h.runtimeBuildInfo(ctx, params)

	// RPC Access Log
	case "rpc.logs.list":
		return h.rpcLogsList(ctx, params)

	// Agent-to-Agent RPC
	case "agent.request":
		return h.agentRequest(ctx, params)
	case "agent.respond":
		return h.agentRespond(ctx, params)
	case "agent.request.result":
		return h.agentRequestResult(ctx, params)
	case "agent.request.open.list":
		return h.agentRequestOpenList(ctx, params)
	case "agent.request.list":
		return h.agentRequestList(ctx, params)

	// Approval operations
	case "approval.list":
		return h.approvalList(ctx, params)
	case "approval.decide":
		return h.approvalDecide(ctx, params)

	// FinOps operations
	case "finops.spend":
		return h.finopsSpend(ctx, params)
	case "finops.ledger":
		return h.finopsLedger(ctx, params)

	// Budget ledger operations
	case "budget.account.ensure":
		return h.budgetAccountEnsure(ctx, params)
	case "budget.account.get":
		return h.budgetAccountGet(ctx, params)
	case "budget.reserve":
		return h.budgetReserve(ctx, params)
	case "budget.spend":
		return h.budgetSpend(ctx, params)
	case "budget.release":
		return h.budgetRelease(ctx, params)
	case "budget.refund":
		return h.budgetRefund(ctx, params)
	case "budget.ledger.list":
		return h.budgetLedgerList(ctx, params)
	case "budget.reservations.list":
		return h.budgetReservationList(ctx, params)
	case "budget.health":
		return h.budgetHealth(ctx, params)

	// Limits operations
	case "limits.group.create":
		return h.limitsGroupCreate(ctx, params)
	case "limits.group.update":
		return h.limitsGroupUpdate(ctx, params)
	case "limits.group.get":
		return h.limitsGroupGet(ctx, params)
	case "limits.group.list":
		return h.limitsGroupList(ctx, params)
	case "agent.limits.get":
		return h.agentLimitsGet(ctx, params)
	case "limits.group.delete":
		return h.limitsGroupDelete(ctx, params)
	case "limits.report":
		return h.limitsReport(ctx, params)
	case "limits.bootstrap":
		return h.limitsBootstrap(ctx, params)
	case "limits.snapshots":
		return h.limitsSnapshots(ctx, params)

	// News
	case "news.publish":
		return h.newsPublish(ctx, params)
	case "news.poll":
		return h.newsPoll(ctx, params)
	case "news.list":
		return h.newsList(ctx, params)
	case "news.delete":
		return h.newsDelete(ctx, params)

	// Health
	case "system.health":
		return map[string]any{
			"status": "ok",
			"ts":     time.Now().UTC().Format(time.RFC3339Nano),
		}, nil

	default:
		return nil, &RPCError{
			Code:    errCodeMethodNotFound,
			Message: fmt.Sprintf("method not found: %s", method),
		}
	}
}

func rejectResponderOriginDisallowedRPC(ctx context.Context, method string) *RPCError {
	principal, ok := authPrincipalFromContext(ctx)
	if !ok || !strings.EqualFold(strings.TrimSpace(principal.RuntimeOrigin), "agent_responder") {
		return nil
	}
	method = strings.TrimSpace(method)
	if responderOriginAllowedRPC(method) {
		return nil
	}
	return &RPCError{
		Code:    errCodePermissionDenied,
		Message: "responder-origin principal cannot call RPC method " + method,
		Details: map[string]any{
			"runtime_origin": "agent_responder",
			"allowed_surface": []string{
				"agent.respond",
				"agent.request.open.list",
				"agent.request.result",
				"read-only get/list/report/status RPCs explicitly allowlisted for responder snapshots",
			},
		},
	}
}

func responderOriginAllowedRPC(method string) bool {
	switch strings.TrimSpace(method) {
	case "rpc.describe", "rpc.methods.list",
		"agent.respond",
		"agent.request.open.list", "agent.request.result",
		"agent.message.poll",
		"agent.state.get", "agent.state.list",
		"workspace.agents.list", "workspace.humans.list",
		"workspace.doc.get", "workspace.doc.list", "workspace.artifact.list", "workspace.segment.get", "workspace.segment.list",
		"workspace.tasks.list", "workspace.messages.list", "workspace.updates.list", "workspace.sessions.list",
		"workspace.memory.list", "workspace.memory.search", "workspace.memory.node.search", "workspace.memory.packet.kernel.get", "workspace.memory.packet.shell.get",
		"workspace.memory.pack.list", "workspace.memory.pack.get", "workspace.memory.promotion.get", "workspace.memory.promotion.list",
		"workspace.memory.metrics.list", "workspace.memory.metrics.get", "workspace.memory.coherence.report", "workspace.memory.coherence.scope",
		"workspace.rsp.belief.report", "workspace.rsp.state.report", "workspace.rsp.forecast.report", "workspace.rsp.capability.get",
		"project.get", "project.list", "project.profile.get", "project.gates.status", "project.coordination.get", "project.roles.list",
		"project.repositories.list", "project.checkouts.list", "project.branches.list", "project.patch_queue.list",
		"project.governance.predicates.check", "project.governance.challenge.get", "project.governance.challenge.list", "project.governance.votes.list",
		"tool.list", "mcp.server.list", "mcp.tool.list", "mcp.tool.discover":
		return true
	default:
		return false
	}
}

// ── Agent ───────────────────────────────────────────────────────────

type agentRegisterParams struct {
	WorkspaceID     string          `json:"workspace_id"`
	AgentID         string          `json:"agent_id"`
	GroupID         string          `json:"group_id,omitempty"`
	OwnerUserID     string          `json:"owner_user_id"`
	DisplayName     string          `json:"display_name"`
	Role            string          `json:"role"`
	Status          string          `json:"status"`
	ProtocolVersion string          `json:"protocol_version"`
	Capabilities    json.RawMessage `json:"capabilities"`
	Summary         string          `json:"summary"`
}

// flexParseCapabilities accepts either a comma-separated string or a JSON array of strings.
func flexParseCapabilities(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	// Try array first
	var arr []string
	if err := json.Unmarshal(raw, &arr); err == nil {
		return arr
	}
	// Try string (comma-separated)
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if strings.TrimSpace(s) == "" {
			return []string{}
		}
		return parseCSV(s)
	}
	return nil
}

func optionalRegisterStringField(rawFields map[string]json.RawMessage, key, value string) *string {
	raw, ok := rawFields[key]
	if !ok {
		return nil
	}
	if strings.TrimSpace(string(raw)) == "null" {
		return nil
	}
	trimmed := strings.TrimSpace(value)
	return &trimmed
}

func optionalRegisterCapabilitiesField(rawFields map[string]json.RawMessage, key string) *[]string {
	raw, ok := rawFields[key]
	if !ok {
		return nil
	}
	rawText := strings.TrimSpace(string(raw))
	if rawText == "null" {
		return nil
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil && strings.TrimSpace(asString) == "" {
		return nil
	}
	parsed := flexParseCapabilities(raw)
	return &parsed
}

func (h *Handler) agentRegister(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentRegisterParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawFields); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	if p.AgentID != "" && !validIDRegex.MatchString(p.AgentID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid agent_id format"}
	}

	agent, err := h.store.RegisterAgentPreservingOmitted(ctx, sqlite.AgentRegisterPatchInput{
		WorkspaceID:     p.WorkspaceID,
		AgentID:         p.AgentID,
		OwnerUserID:     optionalRegisterStringField(rawFields, "owner_user_id", p.OwnerUserID),
		DisplayName:     optionalRegisterStringField(rawFields, "display_name", p.DisplayName),
		Role:            optionalRegisterStringField(rawFields, "role", p.Role),
		Status:          optionalRegisterStringField(rawFields, "status", p.Status),
		ProtocolVersion: optionalRegisterStringField(rawFields, "protocol_version", p.ProtocolVersion),
		Capabilities:    optionalRegisterCapabilitiesField(rawFields, "capabilities"),
		Summary:         optionalRegisterStringField(rawFields, "summary", p.Summary),
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	groupID := strings.TrimSpace(p.GroupID)
	switch {
	case groupID != "":
		_ = h.store.AssignAgentLimitGroup(ctx, agent.WorkspaceID, agent.AgentID, groupID, groupID)
	default:
		// Auto-create singleton limit group for agent (best-effort)
		_ = h.store.EnsureAgentLimitGroup(ctx, agent.WorkspaceID, agent.AgentID, agent.DisplayName)
	}
	return map[string]any{"agent": agent}, nil
}

type agentHeartbeatParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Status      string `json:"status"`
	Summary     string `json:"summary"`
}

func (h *Handler) agentHeartbeat(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentHeartbeatParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	if err := h.store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		Status:      p.Status,
		Summary:     p.Summary,
	}); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"workspace_id": p.WorkspaceID,
		"agent_id":     p.AgentID,
		"status":       model.NormalizeStatus(p.Status),
		"recorded_at":  time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

type agentBootstrapParams struct {
	WorkspaceID  string `json:"workspace_id"`
	AgentID      string `json:"agent_id"`
	UpdatesLimit int    `json:"updates_limit"`
}

func (h *Handler) agentBootstrap(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentBootstrapParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if p.UpdatesLimit <= 0 {
		p.UpdatesLimit = 10
	}

	agent, err := h.store.GetAgent(ctx, p.WorkspaceID, p.AgentID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	snapshot, err := h.store.GetWorkspaceSnapshot(ctx, p.WorkspaceID, p.UpdatesLimit)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	snapshot = scopeAgentBootstrapSnapshot(p.AgentID, snapshot)
	return map[string]any{
		"generated_at": time.Now().UTC().Format(time.RFC3339Nano),
		"agent":        agent,
		"snapshot":     snapshot,
	}, nil
}

func scopeAgentBootstrapSnapshot(agentID string, snapshot sqlite.WorkspaceSnapshot) sqlite.WorkspaceSnapshot {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return snapshot
	}
	activeSession := bootstrapActiveSession(snapshot.Sessions, agentID)
	activeTaskID := ""
	activeSessionID := ""
	activeStartedAt := ""
	if activeSession != nil {
		activeTaskID = strings.TrimSpace(activeSession.TaskID)
		activeSessionID = strings.TrimSpace(activeSession.SessionID)
		activeStartedAt = strings.TrimSpace(activeSession.StartedAt)
	}
	if activeTaskID == "" {
		activeTaskID = bootstrapActiveTaskID(snapshot.Agents, agentID)
	}
	snapshot.Docs = filterBootstrapDocs(agentID, activeTaskID, snapshot.Docs)
	snapshot.RecentUpdates = filterBootstrapUpdates(agentID, activeTaskID, activeSessionID, activeStartedAt, snapshot.RecentUpdates)
	snapshot.RecentMessages = filterBootstrapMessages(agentID, activeTaskID, activeSessionID, activeStartedAt, snapshot.RecentMessages)
	snapshot.RecentMemory = filterBootstrapMemory(agentID, activeTaskID, activeSessionID, activeStartedAt, snapshot.RecentMemory)
	return snapshot
}

func bootstrapActiveSession(sessions []sqlite.AgentSessionStateRecord, agentID string) *sqlite.AgentSessionStateRecord {
	for i := range sessions {
		if strings.TrimSpace(sessions[i].AgentID) != agentID {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(sessions[i].Status), "ACTIVE") {
			return &sessions[i]
		}
	}
	return nil
}

func bootstrapActiveTaskID(agents []sqlite.AgentRecord, agentID string) string {
	for _, agent := range agents {
		if strings.TrimSpace(agent.AgentID) != agentID {
			continue
		}
		if agent.CurrentSession != nil && strings.TrimSpace(agent.CurrentSession.TaskID) != "" {
			return strings.TrimSpace(agent.CurrentSession.TaskID)
		}
		for _, task := range agent.ActiveTasks {
			switch strings.TrimSpace(task.ClaimStatus) {
			case "CLAIMED", "BLOCKED":
				if trimmed := strings.TrimSpace(task.TaskID); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

func filterBootstrapDocs(agentID, taskID string, docs []sqlite.WorkspaceDocRecord) []sqlite.WorkspaceDocRecord {
	allowedGeneric := map[string]struct{}{
		"charter":         {},
		"current_context": {},
		"decisions":       {},
		"open_questions":  {},
		"handoff":         {},
		"tooling":         {},
		"autonomy_policy": {},
	}
	out := make([]sqlite.WorkspaceDocRecord, 0, len(docs))
	for _, doc := range docs {
		docKey := strings.TrimSpace(doc.DocKey)
		if docKey == "" {
			continue
		}
		if _, ok := allowedGeneric[docKey]; ok {
			out = append(out, doc)
			continue
		}
		if isBootstrapTaskDoc(docKey, taskID) {
			out = append(out, doc)
			continue
		}
		docAgentID, suffix, ok := parseBootstrapAgentDocKey(docKey)
		if !ok || docAgentID != agentID {
			continue
		}
		docTaskID := bootstrapDocTaskID(doc.Content)
		if !bootstrapAgentDocTaskMatches(strings.TrimSpace(taskID), docTaskID) {
			continue
		}
		if strings.EqualFold(suffix, "claimed_work") && strings.Contains(strings.ToLower(doc.Content), "active_claimed_work: none") {
			continue
		}
		out = append(out, doc)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].DocKey < out[j].DocKey })
	return out
}

func isBootstrapTaskDoc(docKey, taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false
	}
	prefix := "task." + taskID
	return docKey == prefix || strings.HasPrefix(docKey, prefix+".")
}

func bootstrapAgentDocTaskMatches(activeTaskID, docTaskID string) bool {
	activeTaskID = strings.TrimSpace(activeTaskID)
	docTaskID = strings.TrimSpace(docTaskID)
	if docTaskID == "" || strings.EqualFold(docTaskID, "(none)") || strings.EqualFold(docTaskID, "none") {
		return activeTaskID == ""
	}
	if activeTaskID == "" {
		return false
	}
	return docTaskID == activeTaskID
}

func parseBootstrapAgentDocKey(docKey string) (agentID, suffix string, ok bool) {
	if !strings.HasPrefix(docKey, "agent.") {
		return "", "", false
	}
	rest := strings.TrimPrefix(docKey, "agent.")
	idx := strings.Index(rest, ".")
	if idx <= 0 || idx >= len(rest)-1 {
		return "", "", false
	}
	agentID = strings.TrimSpace(rest[:idx])
	suffix = strings.TrimSpace(rest[idx+1:])
	switch suffix {
	case "current_context", "claimed_work":
		return agentID, suffix, agentID != ""
	default:
		return "", "", false
	}
}

func bootstrapDocTaskID(content string) string {
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- "))
		switch {
		case strings.HasPrefix(trimmed, "task_id:"):
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "task_id:"))
		case strings.HasPrefix(trimmed, "task_id="):
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "task_id="))
		}
	}
	return ""
}

func filterBootstrapUpdates(agentID, taskID, sessionID, startedAt string, updates []sqlite.AgentUpdateRecord) []sqlite.AgentUpdateRecord {
	out := make([]sqlite.AgentUpdateRecord, 0, len(updates))
	for _, update := range updates {
		payloadTaskID, payloadSessionID := bootstrapPayloadTaskAndSession(update.PayloadJSON)
		if strings.TrimSpace(sessionID) != "" && payloadSessionID == strings.TrimSpace(sessionID) {
			out = append(out, update)
			continue
		}
		if strings.TrimSpace(taskID) != "" && payloadTaskID == strings.TrimSpace(taskID) && !bootstrapTimestampBefore(update.CreatedAt, startedAt) {
			out = append(out, update)
			continue
		}
		if strings.TrimSpace(taskID) == "" && strings.TrimSpace(update.AgentID) == strings.TrimSpace(agentID) {
			out = append(out, update)
		}
	}
	return out
}

func filterBootstrapMessages(agentID, taskID, sessionID, startedAt string, messages []sqlite.MessageRecord) []sqlite.MessageRecord {
	out := make([]sqlite.MessageRecord, 0, len(messages))
	for _, message := range messages {
		if bootstrapTimestampBefore(message.CreatedAt, startedAt) {
			continue
		}
		payloadTaskID, payloadSessionID := bootstrapPayloadTaskAndSession(message.MetadataJSON)
		if strings.TrimSpace(sessionID) != "" && payloadSessionID == strings.TrimSpace(sessionID) {
			out = append(out, message)
			continue
		}
		if strings.TrimSpace(taskID) != "" && payloadTaskID == strings.TrimSpace(taskID) &&
			(strings.TrimSpace(message.FromAgentID) == strings.TrimSpace(agentID) || strings.TrimSpace(message.ToAgentID) == strings.TrimSpace(agentID)) {
			out = append(out, message)
			continue
		}
		if strings.TrimSpace(message.FromAgentID) == strings.TrimSpace(agentID) || strings.TrimSpace(message.ToAgentID) == strings.TrimSpace(agentID) {
			out = append(out, message)
		}
	}
	return out
}

func filterBootstrapMemory(agentID, taskID, sessionID, startedAt string, items []sqlite.WorkspaceMemoryRecord) []sqlite.WorkspaceMemoryRecord {
	out := make([]sqlite.WorkspaceMemoryRecord, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(sessionID) != "" && strings.TrimSpace(item.SessionID) == strings.TrimSpace(sessionID) {
			out = append(out, item)
			continue
		}
		if strings.TrimSpace(taskID) != "" && strings.TrimSpace(item.TaskID) == strings.TrimSpace(taskID) {
			out = append(out, item)
			continue
		}
		if strings.TrimSpace(agentID) != "" && strings.TrimSpace(item.AgentID) == strings.TrimSpace(agentID) && !bootstrapTimestampBefore(item.CreatedAt, startedAt) {
			out = append(out, item)
		}
	}
	return out
}

func bootstrapPayloadTaskAndSession(raw string) (taskID, sessionID string) {
	if strings.TrimSpace(raw) == "" {
		return "", ""
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", ""
	}
	if value, ok := payload["task_id"].(string); ok {
		taskID = strings.TrimSpace(value)
	}
	if value, ok := payload["session_id"].(string); ok {
		sessionID = strings.TrimSpace(value)
	}
	return taskID, sessionID
}

func bootstrapTimestampBefore(candidate, floor string) bool {
	candidate = strings.TrimSpace(candidate)
	floor = strings.TrimSpace(floor)
	if candidate == "" || floor == "" {
		return false
	}
	candidateAt, okCandidate := parseBootstrapTime(candidate)
	floorAt, okFloor := parseBootstrapTime(floor)
	if !okCandidate || !okFloor {
		return false
	}
	return candidateAt.Before(floorAt)
}

func parseBootstrapTime(raw string) (time.Time, bool) {
	layouts := []string{time.RFC3339Nano, time.RFC3339}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

type agentUpdatePostParams struct {
	WorkspaceID   string `json:"workspace_id"`
	AgentID       string `json:"agent_id"`
	UpdateType    string `json:"update_type"`
	Summary       string `json:"summary"`
	PayloadJSON   string `json:"payload_json"`
	RequiresHuman bool   `json:"requires_human"`
}

func newAgentUpdateID(workspaceID, agentID, updateType, summary string) string {
	now := time.Now().UTC()
	seed := fmt.Sprintf("%s|%s|%s|%s|%d", workspaceID, agentID, updateType, summary, now.UnixNano())
	sum := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("agent_update-%d-%s", now.UnixNano(), hex.EncodeToString(sum[:4]))
}

func agentUpdateTouchesActivity(p agentUpdatePostParams) bool {
	return !agentUpdateIsValidInternalHeartbeatSummary(p)
}

func agentUpdateIsValidInternalHeartbeatSummary(p agentUpdatePostParams) bool {
	if !strings.EqualFold(strings.TrimSpace(p.UpdateType), "internal_heartbeat_summary") || p.RequiresHuman {
		return false
	}
	var payload struct {
		ContractVersion   string `json:"contract_version"`
		ObservabilityOnly bool   `json:"observability_only"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(p.PayloadJSON)), &payload); err != nil {
		return false
	}
	return payload.ContractVersion == "internal-heartbeat-summary/v1" && payload.ObservabilityOnly
}

func liveEventPayloadJSON(payload map[string]any) string {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func (h *Handler) agentUpdatePost(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentUpdatePostParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	updateID := newAgentUpdateID(p.WorkspaceID, p.AgentID, p.UpdateType, p.Summary)
	event, err := h.store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		UpdateID:              updateID,
		WorkspaceID:           p.WorkspaceID,
		AgentID:               p.AgentID,
		UpdateType:            p.UpdateType,
		Summary:               p.Summary,
		PayloadJSON:           p.PayloadJSON,
		RequiresHuman:         p.RequiresHuman,
		PromptContextEnvelope: h.agentUpdatePromptContextEnvelope(ctx, p.WorkspaceID, updateID, p.AgentID, p.UpdateType, p.Summary, p.RequiresHuman),
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.update.post")
	}
	if agentUpdateTouchesActivity(p) {
		h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
	}
	h.publishRuntimeEventRecordAs(event, "agent.update", p.Summary)
	return map[string]any{
		"workspace_id":   p.WorkspaceID,
		"agent_id":       p.AgentID,
		"update_type":    p.UpdateType,
		"requires_human": p.RequiresHuman,
		"status":         "RECORDED",
	}, nil
}

type agentTaskClaimParams struct {
	WorkspaceID          string `json:"workspace_id"`
	AgentID              string `json:"agent_id"`
	TaskID               string `json:"task_id"`
	ProjectRoleID        string `json:"project_role_id"`
	RepoID               string `json:"repo_id"`
	CheckoutID           string `json:"checkout_id"`
	BranchID             string `json:"branch_id"`
	WriteScopeJSON       string `json:"write_scope_json"`
	CoordinationMode     string `json:"coordination_mode"`
	Summary              string `json:"summary"`
	SelectedFromFrontier bool   `json:"selected_from_frontier"`
	FrontierGenerationID string `json:"frontier_generation_id"`
	SelfFitSummary       string `json:"self_fit_summary"`
}

type agentTaskFrontierDecisionParams struct {
	WorkspaceID          string `json:"workspace_id"`
	AgentID              string `json:"agent_id"`
	FrontierGenerationID string `json:"frontier_generation_id"`
	DecisionState        string `json:"decision_state"`
	SelectedTaskID       string `json:"selected_task_id"`
	Summary              string `json:"summary"`
}

func (h *Handler) agentTaskFrontierDecision(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentTaskFrontierDecisionParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	event, err := h.store.RecordAgentTaskFrontierDecision(ctx, sqlite.AgentTaskFrontierDecisionInput{
		WorkspaceID:           p.WorkspaceID,
		AgentID:               p.AgentID,
		FrontierGenerationID:  strings.TrimSpace(p.FrontierGenerationID),
		DecisionState:         strings.TrimSpace(p.DecisionState),
		SelectedTaskID:        strings.TrimSpace(p.SelectedTaskID),
		Summary:               p.Summary,
		PromptContextEnvelope: h.agentTaskPromptContextEnvelope(ctx, p.WorkspaceID, "agent.task_frontier.decision", strings.TrimSpace(p.SelectedTaskID), p.AgentID),
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.task_frontier.decision")
	}
	h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
	h.publishRuntimeEventRecord(event, p.Summary)
	return map[string]any{
		"workspace_id":             p.WorkspaceID,
		"agent_id":                 p.AgentID,
		"frontier_generation_id":   strings.TrimSpace(p.FrontierGenerationID),
		"decision_state":           strings.TrimSpace(p.DecisionState),
		"selected_task_id":         strings.TrimSpace(p.SelectedTaskID),
		"runtime_event_id":         event.EventID,
		"runtime_event_ingest_seq": event.IngestSeq,
	}, nil
}

func (h *Handler) agentTaskClaim(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentTaskClaimParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	event, err := h.store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		AgentID:               p.AgentID,
		ProjectRoleID:         strings.TrimSpace(p.ProjectRoleID),
		RepoID:                strings.TrimSpace(p.RepoID),
		CheckoutID:            strings.TrimSpace(p.CheckoutID),
		BranchID:              strings.TrimSpace(p.BranchID),
		WriteScopeJSON:        strings.TrimSpace(p.WriteScopeJSON),
		CoordinationMode:      strings.TrimSpace(p.CoordinationMode),
		Summary:               p.Summary,
		SelectedFromFrontier:  p.SelectedFromFrontier,
		FrontierGenerationID:  strings.TrimSpace(p.FrontierGenerationID),
		SelfFitSummary:        strings.TrimSpace(p.SelfFitSummary),
		PromptContextEnvelope: h.agentTaskPromptContextEnvelope(ctx, p.WorkspaceID, "agent.task.claim", p.TaskID, p.AgentID),
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.task.claim")
	}
	h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
	h.publishRuntimeEventRecord(event, p.Summary)
	return map[string]any{
		"workspace_id":           p.WorkspaceID,
		"task_id":                p.TaskID,
		"agent_id":               p.AgentID,
		"status":                 "CLAIMED",
		"selected_from_frontier": p.SelectedFromFrontier,
		"frontier_generation_id": strings.TrimSpace(p.FrontierGenerationID),
	}, nil
}

type agentTaskReleaseParams struct {
	WorkspaceID           string `json:"workspace_id"`
	AgentID               string `json:"agent_id"`
	TaskID                string `json:"task_id"`
	Reason                string `json:"reason"`
	SessionTransitionKind string `json:"session_transition_kind,omitempty"`
}

func (h *Handler) agentTaskRelease(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentTaskReleaseParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	event, err := h.store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		AgentID:               p.AgentID,
		Reason:                p.Reason,
		SessionTransitionKind: p.SessionTransitionKind,
		PromptContextEnvelope: h.agentTaskPromptContextEnvelope(ctx, p.WorkspaceID, "agent.task.release", p.TaskID, p.AgentID),
	})
	if err != nil {
		if errors.Is(err, sqlite.ErrTaskClaimStaleTransition) && h.taskClaimReleaseIsNoOpForAgent(ctx, p.WorkspaceID, p.TaskID, p.AgentID) {
			return map[string]any{
				"workspace_id": p.WorkspaceID,
				"task_id":      p.TaskID,
				"agent_id":     p.AgentID,
				"status":       "RELEASED",
			}, nil
		}
		return nil, rpcErrorFromStoreAuthority(err, "agent.task.release")
	}
	if event.EventID != "" {
		h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
		h.publishRuntimeEventRecord(event, p.Reason)
	}
	return map[string]any{
		"workspace_id": p.WorkspaceID,
		"task_id":      p.TaskID,
		"agent_id":     p.AgentID,
		"status":       "RELEASED",
	}, nil
}

// ── Agent Delete ────────────────────────────────────────────────────

type agentDeleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Actor       string `json:"actor"`
}

// taskClaimReleaseIsNoOpForAgent reports whether a release that failed with a
// stale/duplicate transition is in fact a benign no-op: this agent already owns
// the claim and it is already in a state for which "release" has nothing left to
// do. That covers RELEASED (explicit prior release) and the terminal claim
// states COMPLETED/FAILED/CANCELLED (the daemon finished the task in-process
// before the operator stop sweep re-issued a release). BLOCKED is intentionally
// excluded: it is still legally releasable, so a stale error there is real.
// Anchoring on observed state (this agent + benign status) keeps the path
// fail-closed for genuine failures (foreign owner, not-found, CAS-loss to a
// different owner): those do not match and still surface the error.
func (h *Handler) taskClaimReleaseIsNoOpForAgent(ctx context.Context, workspaceID, taskID, agentID string) bool {
	tasks, err := h.store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		return false
	}
	for _, task := range tasks {
		if task.TaskID != taskID || task.ClaimStatus == nil || task.ClaimAgentID == nil {
			continue
		}
		if *task.ClaimAgentID != agentID {
			return false
		}
		switch *task.ClaimStatus {
		case model.TaskClaimStatusReleased,
			model.TaskClaimStatusCompleted,
			model.TaskClaimStatusFailed,
			model.TaskClaimStatusCancelled:
			return true
		default:
			return false
		}
	}
	return false
}

func (h *Handler) agentDelete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentDeleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	actorID := strings.TrimSpace(p.Actor)
	if workspaceID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id is required"}
	}
	if agentID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "agent_id is required"}
	}
	if actorID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "actor is required"}
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") && agentID != strings.TrimSpace(principal.PrincipalID) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}

	event, err := h.store.DeleteAgentWithEvent(ctx, sqlite.AgentDeleteInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.agentLifecyclePromptContextEnvelope(ctx, workspaceID, "agent.delete", map[string]string{
			"agent_id": agentID,
			"actor_id": actorID,
		}),
		PromptContextSurface: "agent.delete",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.delete")
	}
	h.publishRuntimeEventRecord(event, agentID)

	log.Printf("[AGENT DELETE] agent_id=%s workspace_id=%s actor=%s", agentID, workspaceID, actorID)

	return map[string]any{
		"agent_id": agentID,
		"event":    event,
		"status":   "DELETED",
	}, nil
}

// ── Task ────────────────────────────────────────────────────────────

type taskSubmitParams struct {
	TaskID              string         `json:"task_id"`
	OwnerUserID         string         `json:"owner_user_id"`
	Priority            string         `json:"priority"`
	Title               string         `json:"title"`
	Description         string         `json:"description"`
	TaskKind            string         `json:"task_kind"`
	TaskTemplate        string         `json:"task_template"`
	TaskClass           string         `json:"task_class"`
	TaskClassSource     string         `json:"task_class_source"`
	Graph               dag.Graph      `json:"graph"`
	WorkspaceID         string         `json:"workspace_id"`
	LinkedBy            string         `json:"linked_by"`
	Tags                []string       `json:"tags"`
	ProjectID           string         `json:"project_id"`
	ProjectLane         string         `json:"project_lane"`
	RequiresProjectGate *bool          `json:"requires_project_gate"`
	DependencyTaskIDs   []string       `json:"dependency_task_ids"`
	RelatedTaskIDs      []string       `json:"related_task_ids"`
	WriteScopeHints     []string       `json:"write_scope_hints"`
	TaskRequirements    map[string]any `json:"task_requirements"`
}

func (h *Handler) taskPromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildTaskPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func (h *Handler) limitGroupPromptContextEnvelope(ctx context.Context, workspaceID, surface string, fields map[string]string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildLimitGroupPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	for key, value := range fields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(value); value != "" {
			envelope[key] = value
		}
	}
	return envelope
}

func (h *Handler) projectPromptContextEnvelope(ctx context.Context, workspaceID, surface string, fields map[string]string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildProjectPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	for key, value := range fields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(value); value != "" {
			envelope[key] = value
		}
	}
	return envelope
}

func (h *Handler) agentLifecyclePromptContextEnvelope(ctx context.Context, workspaceID, surface string, fields map[string]string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildAgentLifecyclePromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	for key, value := range fields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(value); value != "" {
			envelope[key] = value
		}
	}
	return envelope
}

func (h *Handler) agentProfilePromptContextEnvelope(ctx context.Context, workspaceID, surface string, fields map[string]string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildAgentProfilePromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	for key, value := range fields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(value); value != "" {
			envelope[key] = value
		}
	}
	return envelope
}

func (h *Handler) vaultPromptContextEnvelope(ctx context.Context, workspaceID, surface string, fields map[string]string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildVaultPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	for key, value := range fields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(value); value != "" {
			envelope[key] = value
		}
	}
	return envelope
}

func (h *Handler) newsPromptContextEnvelope(ctx context.Context, workspaceID, surface string, fields map[string]string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildNewsPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	for key, value := range fields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(value); value != "" {
			envelope[key] = value
		}
	}
	return envelope
}

func (h *Handler) agentTaskPromptContextEnvelope(ctx context.Context, workspaceID, surface, taskID, agentID string) map[string]any {
	envelope := h.taskPromptContextEnvelope(ctx, workspaceID, surface)
	envelope["actor_agent_id"] = strings.TrimSpace(agentID)
	envelope["agent_id"] = strings.TrimSpace(agentID)
	envelope["task_id"] = strings.TrimSpace(taskID)
	switch strings.TrimSpace(surface) {
	case "agent.task.claim":
		envelope["claim_status"] = model.TaskClaimStatusClaimed
	case "agent.task.release":
		envelope["claim_status"] = model.TaskClaimStatusReleased
	case "agent.task.complete":
		envelope["claim_status"] = model.TaskClaimStatusCompleted
	case "agent.task.block":
		envelope["claim_status"] = model.TaskClaimStatusBlocked
	}
	return envelope
}

func (h *Handler) attachTaskPromptContext(ctx context.Context, workspaceID, surface string, payload map[string]any) (map[string]any, *RPCError) {
	attached, err := sqlite.AttachTaskPromptContextEnvelope(payload, h.taskPromptContextEnvelope(ctx, workspaceID, surface))
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return attached, nil
}

func (h *Handler) runtimeActorFromPromptContext(ctx context.Context) (string, string, string) {
	actorType, actorID := h.promptContextPrincipal(ctx)
	agentID := ""
	if strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		agentID = strings.TrimSpace(actorID)
	}
	return actorType, actorID, agentID
}

func taskSubmitRequirementsJSON(requirements map[string]any, writeScopeHints []string) string {
	if len(requirements) == 0 && len(writeScopeHints) == 0 {
		return "{}"
	}
	payload := make(map[string]any, len(requirements)+2)
	for key, value := range requirements {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		payload[key] = value
	}
	if len(writeScopeHints) > 0 {
		payload["write_scope_hints"] = append([]string(nil), writeScopeHints...)
	}
	if _, ok := payload["schema"]; !ok {
		payload["schema"] = "task_requirements.v1"
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func (h *Handler) taskSubmit(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p taskSubmitParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	if p.TaskID != "" && !validIDRegex.MatchString(p.TaskID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid task_id format"}
	}

	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.store.GetWorkspace(ctx, workspaceID); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("workspace validation failed before task create: %s", err.Error())}
	}

	requiresProjectGate := false
	if p.RequiresProjectGate != nil {
		requiresProjectGate = *p.RequiresProjectGate
	}
	if sourceDocKeys := h.taskSubmitProjectSourceDocKeys(ctx, workspaceID, p.ProjectID); len(sourceDocKeys) > 0 {
		p.TaskRequirements = mergeTaskSubmitSourceDocKeys(p.TaskRequirements, sourceDocKeys)
	}
	taskRequirementsJSON := taskSubmitRequirementsJSON(p.TaskRequirements, p.WriteScopeHints)
	taskInput := sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               p.TaskID,
		OwnerUserID:          p.OwnerUserID,
		Priority:             p.Priority,
		Title:                p.Title,
		Description:          p.Description,
		TaskKind:             p.TaskKind,
		TaskTemplate:         p.TaskTemplate,
		TaskClass:            p.TaskClass,
		TaskClassSource:      p.TaskClassSource,
		Tags:                 p.Tags,
		ProjectID:            p.ProjectID,
		ProjectLane:          p.ProjectLane,
		RequiresProjectGate:  requiresProjectGate,
		DependencyTaskIDs:    p.DependencyTaskIDs,
		RelatedTaskIDs:       p.RelatedTaskIDs,
		TaskRequirementsJSON: taskRequirementsJSON,
		WriteScopeHints:      p.WriteScopeHints,
	}
	attachActor := strings.TrimSpace(p.LinkedBy)
	if attachActor == "" {
		attachActor = strings.TrimSpace(p.OwnerUserID)
	}
	var createdEvent *sqlite.RuntimeEventRecord
	summary := fmt.Sprintf("Task created: %s", firstNonEmpty(strings.TrimSpace(p.Title), strings.TrimSpace(p.TaskID)))
	payload, rpcErr := h.attachTaskPromptContext(ctx, workspaceID, "task.submit", map[string]any{
		"workspace_id":           workspaceID,
		"task_id":                p.TaskID,
		"title":                  p.Title,
		"description":            p.Description,
		"priority":               p.Priority,
		"task_kind":              p.TaskKind,
		"task_template":          p.TaskTemplate,
		"task_class":             p.TaskClass,
		"task_class_source":      p.TaskClassSource,
		"owner_user_id":          p.OwnerUserID,
		"linked_by":              attachActor,
		"project_id":             p.ProjectID,
		"project_lane":           p.ProjectLane,
		"requires_project_gate":  requiresProjectGate,
		"dependency_task_ids":    p.DependencyTaskIDs,
		"related_task_ids":       p.RelatedTaskIDs,
		"write_scope_hints":      p.WriteScopeHints,
		"task_requirements":      p.TaskRequirements,
		"task_requirements_json": taskRequirementsJSON,
		"tags":                   p.Tags,
		"status":                 "PENDING",
		"summary":                summary,
	})
	if rpcErr != nil {
		return nil, rpcErr
	}
	actorType, actorID, agentID := h.runtimeActorFromPromptContext(ctx)
	event, err := h.store.CreateTaskWithGraphAndWorkspaceEvent(ctx, taskInput, p.Graph, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      p.TaskID,
		LinkedBy:    attachActor,
	}, sqlite.RuntimeEventInput{
		DedupKey:    "task:" + strings.TrimSpace(p.TaskID) + ":created",
		WorkspaceID: workspaceID,
		EventType:   "task.created",
		EntityType:  "task",
		EntityID:    strings.TrimSpace(p.TaskID),
		ActorType:   actorType,
		ActorID:     actorID,
		AgentID:     agentID,
		TaskID:      strings.TrimSpace(p.TaskID),
		PayloadJSON: string(mustJSON(payload)),
	})
	if err != nil {
		log.Printf("[task.submit] task create runtime transaction failed workspace=%s task=%s err=%v", workspaceID, p.TaskID, err)
		if errors.Is(err, sqlite.ErrTaskProjectNotFound) || errors.Is(err, sqlite.ErrWorkspaceTaskAbsent) || errors.Is(err, sqlite.ErrTaskWorkspaceAmbiguous) || errors.Is(err, sqlite.ErrTaskNotFound) || errors.Is(err, sqlite.ErrTaskSubmitPatchQueueGate) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	createdEvent = &event

	status, err := h.store.GetTaskStatus(ctx, workspaceID, p.TaskID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("task status readback failed after create: %s", err.Error())}
	}
	result := map[string]any{
		"task_id":                status.TaskID,
		"workspace_id":           workspaceID,
		"status":                 status.Status,
		"task":                   status,
		"project_id":             status.ProjectID,
		"task_kind":              status.TaskKind,
		"project_lane":           status.ProjectLane,
		"requires_project_gate":  status.RequiresProjectGate,
		"task_requirements_json": status.TaskRequirementsJSON,
		"write_scope_hints":      status.WriteScopeHints,
	}

	// Auto-routing: find agents with matching tags and send suggestion.
	if len(p.Tags) > 0 {
		matched, _ := h.store.SearchAgentsByTags(ctx, workspaceID, p.Tags)
		if len(matched) > 0 {
			var suggested []string
			for _, m := range matched {
				suggested = append(suggested, m.AgentID)
				// Send suggestion message to each matching agent.
				_, _ = h.store.SendMessage(ctx, sqlite.MessageSendInput{
					WorkspaceID: workspaceID,
					FromAgentID: "system",
					ToAgentID:   m.AgentID,
					Channel:     "task-suggestion",
					ContentType: "application/json",
					Content: string(mustJSON(map[string]any{
						"type":         "task_suggestion",
						"task_id":      p.TaskID,
						"title":        p.Title,
						"tags":         p.Tags,
						"matched_tags": m.MatchedTags,
					})),
				})
			}
			result["suggested_agents"] = suggested
		}
	}

	if createdEvent != nil {
		h.publishRuntimeEventRecord(*createdEvent, fmt.Sprintf("Task created: %s", firstNonEmpty(strings.TrimSpace(p.Title), strings.TrimSpace(p.TaskID))))
	}

	return result, nil
}

func (h *Handler) taskSubmitProjectSourceDocKeys(ctx context.Context, workspaceID, projectID string) []string {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if h == nil || h.store == nil || workspaceID == "" || projectID == "" {
		return nil
	}
	docKey := "project." + taskSubmitProjectDocKeySegment(projectID) + ".source_refs"
	doc, err := h.store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		return nil
	}
	return sqlite.SourceDocKeysFromSourceRefsText(doc.Content)
}

func mergeTaskSubmitSourceDocKeys(requirements map[string]any, sourceDocKeys []string) map[string]any {
	sourceDocKeys = uniqueServerStrings(sourceDocKeys)
	if len(sourceDocKeys) == 0 {
		return requirements
	}
	if requirements == nil {
		requirements = map[string]any{}
	}
	existing := taskSubmitRequirementsStringSlice(requirements, "source_doc_keys", "operator_spec_doc_key", "operator_spec_doc")
	requirements["source_doc_keys"] = uniqueServerStrings(append(existing, sourceDocKeys...))
	requirements["spec_fidelity_contract"] = "source_artifact_fidelity.v1"
	if _, ok := requirements["schema"]; !ok {
		requirements["schema"] = "task_requirements.v1"
	}
	return requirements
}

func taskSubmitRequirementsStringSlice(requirements map[string]any, keys ...string) []string {
	if len(requirements) == 0 {
		return nil
	}
	wanted := map[string]struct{}{}
	for _, key := range keys {
		wanted[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	var out []string
	for key, value := range requirements {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(key))]; !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			out = append(out, typed)
		case []string:
			out = append(out, typed...)
		case []any:
			for _, item := range typed {
				out = append(out, fmt.Sprint(item))
			}
		}
	}
	return out
}

func uniqueServerStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func taskSubmitProjectDocKeySegment(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

type taskStatusParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id"`
}

func (h *Handler) taskStatus(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p taskStatusParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}

	status, err := h.store.GetTaskStatus(ctx, p.WorkspaceID, p.TaskID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return status, nil
}

type taskClassPutParams struct {
	WorkspaceID     string `json:"workspace_id"`
	TaskID          string `json:"task_id"`
	TaskClass       string `json:"task_class"`
	TaskClassSource string `json:"task_class_source"`
	ActorID         string `json:"actor_id"`
}

func (h *Handler) taskClassPut(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p taskClassPutParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.TaskID) == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "task_id is required"}
	}
	if !validIDRegex.MatchString(p.TaskID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid task_id format"}
	}

	status, err := h.store.PutTaskClassEvidence(ctx, sqlite.TaskClassEvidencePutInput{
		WorkspaceID:     p.WorkspaceID,
		TaskID:          p.TaskID,
		TaskClass:       p.TaskClass,
		TaskClassSource: p.TaskClassSource,
		ActorID:         p.ActorID,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"status": "UPDATED",
		"task":   status,
	}, nil
}

type taskProjectFieldsPutParams struct {
	WorkspaceID         string  `json:"workspace_id"`
	TaskID              string  `json:"task_id"`
	ProjectID           *string `json:"project_id"`
	TaskKind            *string `json:"task_kind"`
	ProjectLane         *string `json:"project_lane"`
	RequiresProjectGate *bool   `json:"requires_project_gate"`
	ActorID             string  `json:"actor_id"`
}

func (h *Handler) taskProjectFieldsPut(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p taskProjectFieldsPutParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.TaskID) == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "task_id is required"}
	}
	if !validIDRegex.MatchString(p.TaskID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid task_id format"}
	}
	if p.ProjectID == nil && p.TaskKind == nil && p.ProjectLane == nil && p.RequiresProjectGate == nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "at least one project field is required"}
	}

	actorType, actorID, agentID := h.runtimeActorFromPromptContext(ctx)
	effectiveActorID := strings.TrimSpace(p.ActorID)
	if principal, ok, rpcErr := requireWorkspacePrincipalIfPresent(ctx, strings.TrimSpace(p.WorkspaceID)); rpcErr != nil {
		return nil, rpcErr
	} else if ok {
		if effectiveActorID != "" && effectiveActorID != strings.TrimSpace(principal.PrincipalID) {
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match actor_id"}
		}
		actorType = principal.PrincipalType
		actorID = principal.PrincipalID
		effectiveActorID = strings.TrimSpace(principal.PrincipalID)
		if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
			agentID = strings.TrimSpace(principal.PrincipalID)
		}
	}
	summary := fmt.Sprintf("Task project fields updated: %s", strings.TrimSpace(p.TaskID))
	eventPayload, rpcErr := h.attachTaskPromptContext(ctx, strings.TrimSpace(p.WorkspaceID), "task.project_fields.put", map[string]any{
		"workspace_id":                    strings.TrimSpace(p.WorkspaceID),
		"task_id":                         strings.TrimSpace(p.TaskID),
		"actor_id":                        effectiveActorID,
		"requested_project_id":            optionalStringPointerValue(p.ProjectID),
		"requested_task_kind":             optionalStringPointerValue(p.TaskKind),
		"requested_project_lane":          optionalStringPointerValue(p.ProjectLane),
		"requested_requires_project_gate": p.RequiresProjectGate,
		"summary":                         summary,
	})
	if rpcErr != nil {
		return nil, rpcErr
	}
	status, event, err := h.store.UpdateTaskProjectFieldsWithRuntimeEvent(ctx, sqlite.TaskProjectFieldsUpdateInput{
		WorkspaceID:         p.WorkspaceID,
		TaskID:              p.TaskID,
		ProjectID:           p.ProjectID,
		TaskKind:            p.TaskKind,
		ProjectLane:         p.ProjectLane,
		RequiresProjectGate: p.RequiresProjectGate,
		ActorID:             effectiveActorID,
	}, sqlite.RuntimeEventInput{
		DedupKey:    "task:" + strings.TrimSpace(p.TaskID) + ":project-fields:" + time.Now().UTC().Format(time.RFC3339Nano),
		WorkspaceID: strings.TrimSpace(p.WorkspaceID),
		EventType:   "task.project_fields.updated",
		EntityType:  "task",
		EntityID:    strings.TrimSpace(p.TaskID),
		ActorType:   actorType,
		ActorID:     firstNonEmpty(effectiveActorID, actorID),
		AgentID:     agentID,
		TaskID:      strings.TrimSpace(p.TaskID),
		PayloadJSON: string(mustJSON(eventPayload)),
	})
	if err != nil {
		if errors.Is(err, sqlite.ErrWorkspaceTaskAbsent) || errors.Is(err, sqlite.ErrTaskProjectNotFound) || errors.Is(err, sqlite.ErrTaskNotFound) || errors.Is(err, sqlite.ErrTaskWorkspaceAmbiguous) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	h.publishRuntimeEventRecord(event, summary)
	return map[string]any{
		"status": "UPDATED",
		"task":   status,
		"event":  event,
	}, nil
}

func optionalStringPointerValue(value *string) any {
	if value == nil {
		return nil
	}
	return strings.TrimSpace(*value)
}

type taskCloseParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id"`
	ActorID     string `json:"actor_id"`
	Resolution  string `json:"resolution"`
	Reason      string `json:"reason"`
}

func (h *Handler) taskClose(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p taskCloseParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	resolution := strings.TrimSpace(p.Resolution)
	if resolution == "" {
		resolution = model.TaskStatusResolved
	}
	actorIDParam := strings.TrimSpace(p.ActorID)
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, strings.TrimSpace(p.WorkspaceID), actorIDParam, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	actorIDParam = strings.TrimSpace(principal.PrincipalID)
	if actorIDParam == "" {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "authenticated principal id is required"}
	}

	summary := fmt.Sprintf("Task closed: %s", firstNonEmpty(strings.TrimSpace(p.TaskID), "task"))
	payload, rpcErr := h.attachTaskPromptContext(ctx, strings.TrimSpace(p.WorkspaceID), "task.close", map[string]any{
		"workspace_id": p.WorkspaceID,
		"task_id":      p.TaskID,
		"actor_id":     actorIDParam,
		"resolution":   resolution,
		"reason":       p.Reason,
		"status":       resolution,
		"summary":      summary,
	})
	if rpcErr != nil {
		return nil, rpcErr
	}
	actorType, actorID, agentID := h.runtimeActorFromPromptContext(ctx)
	event, changed, err := h.store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
		WorkspaceID: p.WorkspaceID,
		TaskID:      p.TaskID,
		ActorID:     actorIDParam,
		Resolution:  resolution,
		Reason:      p.Reason,
	}, sqlite.RuntimeEventInput{
		DedupKey:    "task:" + strings.TrimSpace(p.TaskID) + ":closed:" + resolution,
		WorkspaceID: strings.TrimSpace(p.WorkspaceID),
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    strings.TrimSpace(p.TaskID),
		ActorType:   actorType,
		ActorID:     actorID,
		AgentID:     agentID,
		TaskID:      strings.TrimSpace(p.TaskID),
		PayloadJSON: string(mustJSON(payload)),
	})
	if err != nil {
		log.Printf("[task.close] task close runtime transaction failed workspace=%s task=%s err=%v", p.WorkspaceID, p.TaskID, err)
		return nil, rpcErrorFromStoreAuthority(err, "task.close")
	}
	if changed {
		h.publishRuntimeEventRecord(event, summary)
	}
	return map[string]any{
		"task_id": p.TaskID,
		"status":  "CLOSED",
	}, nil
}

type taskCompleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id"`
	AgentID     string `json:"agent_id"`
	Summary     string `json:"summary"`
}

func (h *Handler) agentTaskComplete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p taskCompleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	event, err := h.store.CompleteTaskWithEvent(ctx, sqlite.TaskCompleteInput{
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		AgentID:               p.AgentID,
		Summary:               p.Summary,
		PromptContextEnvelope: h.agentTaskPromptContextEnvelope(ctx, p.WorkspaceID, "agent.task.complete", p.TaskID, p.AgentID),
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.task.complete")
	}
	h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
	h.publishRuntimeEventRecord(event, p.Summary)
	return map[string]any{
		"task_id": p.TaskID,
		"status":  "COMPLETED",
	}, nil
}

type taskBlockParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id"`
	AgentID     string `json:"agent_id"`
	Reason      string `json:"reason"`
}

func (h *Handler) agentTaskBlock(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p taskBlockParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	event, err := h.store.BlockTaskWithEvent(ctx, sqlite.TaskBlockInput{
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		AgentID:               p.AgentID,
		Reason:                p.Reason,
		PromptContextEnvelope: h.agentTaskPromptContextEnvelope(ctx, p.WorkspaceID, "agent.task.block", p.TaskID, p.AgentID),
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.task.block")
	}
	h.touchAgentActivity(ctx, p.WorkspaceID, p.AgentID)
	h.publishRuntimeEventRecord(event, p.Reason)
	return map[string]any{
		"task_id": p.TaskID,
		"status":  "BLOCKED",
		"reason":  p.Reason,
	}, nil
}

// ── Workspace Docs───────────────────────────────────────────────────────

type workspaceDocPutParams struct {
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	UpdatedBy   string `json:"updated_by"`
	ExpectedSHA string `json:"expected_sha"` // optional: optimistic locking
}

func (h *Handler) workspaceDocPut(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceDocPutParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	docKey := strings.TrimSpace(p.DocKey)
	title := strings.TrimSpace(p.Title)
	updatedBy := strings.TrimSpace(p.UpdatedBy)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(docKey, "doc_key"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(updatedBy, "updated_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, updatedBy, "updated_by"); rpcErr != nil {
		return nil, rpcErr
	}

	event, invalidationEvents, err := h.store.UpsertWorkspaceDocWithEffects(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       title,
		Content:     p.Content,
		UpdatedBy:   updatedBy,
		ExpectedSHA: p.ExpectedSHA,
		PromptContextEnvelope: h.workspaceDocPromptContextEnvelope(ctx, workspaceID, "workspace.doc.put", map[string]string{
			"doc_key":    docKey,
			"title":      title,
			"updated_by": updatedBy,
		}),
	})
	if err != nil {
		if errors.Is(err, sqlite.ErrDocConflict) {
			return nil, &RPCError{Code: errCodeDocumentConflict, Message: "document conflict: content was modified by another agent, re-read doc to get current sha"}
		}
		if isWorkspaceDocValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		if rpcErr := rpcErrorFromStoreAuthority(err, "workspace.doc.put"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	// Compute sha of the new content for the caller.
	newSHA := sha256hex(p.Content)
	actions := []runtimeEventPublishAction{
		{
			Event: event,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, title, docKey)
			},
		},
	}
	for _, invalidationEvent := range invalidationEvents {
		eventRecord := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventRecord,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, title, docKey)
			},
		})
	}
	h.publishRuntimeEventActionsChronological(actions...)

	// Fire webhooks for doc.put event.
	h.fireWebhooksAsync(workspaceID, "doc.put", map[string]any{
		"doc_key":    docKey,
		"updated_by": updatedBy,
		"sha":        newSHA,
	})
	h.touchAgentActivity(ctx, workspaceID, updatedBy)

	return map[string]any{
		"workspace_id": workspaceID,
		"doc_key":      docKey,
		"sha":          newSHA,
		"status":       "SAVED",
	}, nil
}

type workspaceDocGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key"`
}

func (h *Handler) workspaceDocGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceDocGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	docKey := strings.TrimSpace(p.DocKey)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(docKey, "doc_key"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	doc, err := h.store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		if isWorkspaceDocValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return doc, nil
}

type workspaceSearchParams struct {
	WorkspaceID string `json:"workspace_id"`
	Query       string `json:"query"`
	EntityType  string `json:"entity_type"`
	Limit       int    `json:"limit"`
}

func (h *Handler) workspaceSearch(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceSearchParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	results, err := h.store.SearchWorkspace(ctx, sqlite.WorkspaceSearchFilter{
		WorkspaceID: workspaceID,
		Query:       p.Query,
		EntityType:  p.EntityType,
		Limit:       p.Limit,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return results, nil
}

type workspaceUpdatesListParams struct {
	WorkspaceID    string `json:"workspace_id"`
	AfterCreatedAt string `json:"after_created_at"`
	AfterUpdateID  string `json:"after_update_id"`
	Limit          int    `json:"limit"`
}

func (h *Handler) workspaceUpdatesList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceUpdatesListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	updates, err := h.store.ListAgentUpdatesAfter(ctx, workspaceID, p.AfterCreatedAt, p.AfterUpdateID, p.Limit)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"updates": updates,
		"count":   len(updates),
	}, nil
}

// ── Agent Profiles ────────────────────────────────────────────────────

type agentProfileUpdateParams struct {
	WorkspaceID    string         `json:"workspace_id"`
	AgentID        string         `json:"agent_id"`
	ActorID        string         `json:"actor_id"`
	Bio            string         `json:"bio"`
	Specialization string         `json:"specialization"`
	OwnerName      string         `json:"owner_name"`
	OwnerContact   string         `json:"owner_contact"`
	AvatarURL      string         `json:"avatar_url"`
	Links          []string       `json:"links"`
	Tags           []string       `json:"tags"`
	ToolsAccess    []string       `json:"tools_access"`
	Metadata       map[string]any `json:"metadata"`
}

func (h *Handler) agentProfileUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentProfileUpdateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") && agentID != strings.TrimSpace(principal.PrincipalID) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}

	profile, event, err := h.store.UpsertAgentProfileWithEvent(ctx, sqlite.AgentProfileInput{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ActorID:        actorID,
		ActorType:      principal.PrincipalType,
		Bio:            p.Bio,
		Specialization: p.Specialization,
		OwnerName:      p.OwnerName,
		OwnerContact:   p.OwnerContact,
		AvatarURL:      p.AvatarURL,
		Links:          p.Links,
		Tags:           p.Tags,
		ToolsAccess:    p.ToolsAccess,
		Metadata:       p.Metadata,
		PromptContextEnvelope: h.agentProfilePromptContextEnvelope(ctx, workspaceID, "agent.profile.update", map[string]string{
			"agent_id": agentID,
			"actor_id": actorID,
		}),
		PromptContextSurface: "agent.profile.update",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "agent.profile.update")
	}
	h.publishRuntimeEventRecord(event, agentID)
	return map[string]any{
		"agent_id": agentID,
		"profile":  profile,
		"event":    event,
		"status":   "UPDATED",
	}, nil
}

type agentProfileGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
}

func (h *Handler) agentProfileGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentProfileGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	agentID := strings.TrimSpace(p.AgentID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(agentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	profile, err := h.store.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return profile, nil
}

// ── Agent Discovery + Doc List + Doc History + Webhooks ────────────

type workspaceAgentsListParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) workspaceAgentsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceAgentsListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	agents, err := h.store.ListWorkspaceAgents(ctx, workspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"agents": agents,
		"count":  len(agents),
	}, nil
}

type workspaceHumansListParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) workspaceHumansList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceHumansListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	humans, err := h.store.ListHumanProfiles(ctx, workspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"humans": humans,
		"count":  len(humans),
	}, nil
}

type workspaceAgentsSearchParams struct {
	WorkspaceID string          `json:"workspace_id"`
	Tags        json.RawMessage `json:"tags"`
}

func (h *Handler) workspaceAgentsSearch(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceAgentsSearchParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	results, err := h.store.SearchAgentsByTags(ctx, workspaceID, flexParseCapabilities(p.Tags))
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"agents": results,
		"count":  len(results),
	}, nil
}

type workspaceTasksListParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
}

func (h *Handler) workspaceTasksList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceTasksListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	tasks, err := h.store.ListWorkspaceTasks(ctx, workspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	// Filter by project_id if specified
	projectID := strings.TrimSpace(p.ProjectID)
	if projectID != "" {
		filtered := make([]sqlite.WorkspaceTaskRecord, 0)
		for _, t := range tasks {
			if t.ProjectID == projectID {
				filtered = append(filtered, t)
			}
		}
		tasks = filtered
	}

	return map[string]any{
		"tasks": tasks,
		"count": len(tasks),
	}, nil
}

// ── Project ─────────────────────────────────────────────────────────

type projectCreateParams struct {
	ProjectID   string `json:"project_id"`
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	CreatedBy   string `json:"created_by"`
}

func (h *Handler) projectCreate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectCreateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	projectID := strings.TrimSpace(p.ProjectID)
	title := strings.TrimSpace(p.Title)
	createdBy := strings.TrimSpace(p.CreatedBy)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(projectID, "project_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(createdBy, "created_by"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, createdBy, "created_by")
	if rpcErr != nil {
		return nil, rpcErr
	}

	project, event, err := h.store.CreateProjectWithEvent(ctx, sqlite.ProjectCreateInput{
		ProjectID:   projectID,
		WorkspaceID: workspaceID,
		Title:       title,
		Description: p.Description,
		CreatedBy:   createdBy,
		ActorID:     createdBy,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.create", map[string]string{
			"project_id": projectID,
			"actor_id":   createdBy,
			"created_by": createdBy,
		}),
		PromptContextSurface: "project.create",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.create")
	}
	h.publishRuntimeEventRecord(event, title, projectID)
	return map[string]any{"project": project}, nil
}

type projectListParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) projectList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	projects, err := h.store.ListProjects(ctx, workspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"projects": projects,
		"count":    len(projects),
	}, nil
}

type projectGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
}

func (h *Handler) projectGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	projectID := strings.TrimSpace(p.ProjectID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(projectID, "project_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	proj, err := h.store.GetProject(ctx, workspaceID, projectID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return proj, nil
}

type projectUpdateParams struct {
	WorkspaceID string  `json:"workspace_id"`
	ProjectID   string  `json:"project_id"`
	Title       string  `json:"title"`
	Description *string `json:"description"`
	Status      string  `json:"status"`
	ActorID     string  `json:"actor_id"`
}

func (h *Handler) projectUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectUpdateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	projectID := strings.TrimSpace(p.ProjectID)
	title := strings.TrimSpace(p.Title)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(projectID, "project_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}

	proj, event, err := h.store.UpdateProjectWithEvent(ctx, sqlite.ProjectUpdateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       title,
		Description: p.Description,
		Status:      p.Status,
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.update", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.update",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.update")
	}
	h.publishRuntimeEventRecord(event, projectID)
	return proj, nil
}

type projectDeleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	ProjectID   string `json:"project_id"`
	ActorID     string `json:"actor_id"`
}

func (h *Handler) projectDelete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p projectDeleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	projectID := strings.TrimSpace(p.ProjectID)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(projectID, "project_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}

	event, err := h.store.DeleteProjectWithEvent(ctx, sqlite.ProjectDeleteInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.projectPromptContextEnvelope(ctx, workspaceID, "project.delete", map[string]string{
			"project_id": projectID,
			"actor_id":   actorID,
		}),
		PromptContextSurface: "project.delete",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "project.delete")
	}
	h.publishRuntimeEventRecord(event, projectID)
	return map[string]any{
		"project_id": projectID,
		"status":     "DELETED",
	}, nil
}

type workspaceMessagesListParams struct {
	WorkspaceID string `json:"workspace_id"`
	Channel     string `json:"channel"`
	Limit       int    `json:"limit"`
}

func (h *Handler) workspaceMessagesList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceMessagesListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if p.Limit <= 0 {
		p.Limit = 100
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	msgs, err := h.store.ListWorkspaceMessages(ctx, workspaceID, p.Channel, p.Limit)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	for i := range msgs {
		if msgs[i].Channel == "security" && msgs[i].ContentType == "application/vnd.rhizome.auth-token" {
			msgs[i].Content = "[redacted security notice]"
			msgs[i].MetadataJSON = "{}"
		}
	}
	// Collect unique channels for filter UI
	channelSet := map[string]bool{}
	for _, m := range msgs {
		if m.Channel != "" {
			channelSet[m.Channel] = true
		}
	}
	channels := make([]string, 0, len(channelSet))
	for c := range channelSet {
		channels = append(channels, c)
	}
	return map[string]any{
		"messages": msgs,
		"count":    len(msgs),
		"channels": channels,
	}, nil
}

type workspaceDocListParams struct {
	WorkspaceID     string `json:"workspace_id"`
	IncludeArchived bool   `json:"include_archived"`
}

func (h *Handler) workspaceDocList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceDocListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	docs, err := h.store.ListWorkspaceDocs(ctx, workspaceID, p.IncludeArchived)
	if err != nil {
		if isWorkspaceDocValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"docs":  docs,
		"count": len(docs),
	}, nil
}

func (h *Handler) touchAgentActivity(ctx context.Context, workspaceID, agentID string) {
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(agentID) == "" {
		return
	}
	if err := h.store.TouchAgentActivity(ctx, workspaceID, agentID); err != nil {
		log.Printf("[agent-activity] touch failed workspace=%s agent=%s err=%v", workspaceID, agentID, err)
	}
}

func runtimeEventSummary(payloadJSON string, fallbacks ...string) string {
	if strings.TrimSpace(payloadJSON) != "" {
		var payload map[string]any
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err == nil {
			for _, key := range []string{"summary", "title", "tension_id", "doc_key", "artifact_ref", "artifact_id", "entity_id"} {
				if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
					return strings.TrimSpace(value)
				}
			}
		}
	}
	for _, fallback := range fallbacks {
		if strings.TrimSpace(fallback) != "" {
			return strings.TrimSpace(fallback)
		}
	}
	return ""
}

func (h *Handler) publishRuntimeEventRecord(event sqlite.RuntimeEventRecord, fallbacks ...string) {
	h.publishRuntimeEventRecordAs(event, "", fallbacks...)
}

func (h *Handler) publishRuntimeEventRecordAs(event sqlite.RuntimeEventRecord, liveType string, fallbacks ...string) {
	summary := runtimeEventSummary(event.PayloadJSON, fallbacks...)
	if summary == "" {
		summary = strings.TrimSpace(event.EventType)
	}
	canonicalEventType := strings.TrimSpace(event.EventType)
	if strings.TrimSpace(liveType) == "" {
		liveType = canonicalEventType
	}
	h.eventBus.Publish(EventMessage{
		Type:               strings.TrimSpace(liveType),
		CanonicalEventType: canonicalEventType,
		WorkspaceID:        strings.TrimSpace(event.WorkspaceID),
		AgentID:            strings.TrimSpace(event.AgentID),
		EventID:            strings.TrimSpace(event.EventID),
		IngestSeq:          event.IngestSeq,
		DedupKey:           strings.TrimSpace(event.DedupKey),
		EntityType:         strings.TrimSpace(event.EntityType),
		EntityID:           strings.TrimSpace(event.EntityID),
		RootCauseID:        strings.TrimSpace(event.RootCauseID),
		ProvenanceGroupID:  strings.TrimSpace(event.ProvenanceGroupID),
		ParentRefsJSON:     strings.TrimSpace(event.ParentRefsJSON),
		Summary:            summary,
		Timestamp:          firstNonEmpty(strings.TrimSpace(event.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano)),
		PayloadJSON:        strings.TrimSpace(event.PayloadJSON),
	})
}

func (h *Handler) publishRuntimeEventRecordEnvelopeAs(event sqlite.RuntimeEventRecord, liveType, payloadJSON string, fallbacks ...string) {
	if strings.TrimSpace(payloadJSON) != "" {
		event.PayloadJSON = strings.TrimSpace(payloadJSON)
	}
	h.publishRuntimeEventRecordAs(event, liveType, fallbacks...)
}

func (h *Handler) publishRuntimeEventRecordAlias(event sqlite.RuntimeEventRecord, liveType string, liveAgentID *string, payloadJSON string, fallbacks ...string) {
	if liveAgentID != nil {
		event.AgentID = strings.TrimSpace(*liveAgentID)
	}
	if strings.TrimSpace(payloadJSON) != "" {
		event.PayloadJSON = strings.TrimSpace(payloadJSON)
	}
	h.publishRuntimeEventRecordAs(event, liveType, fallbacks...)
}

type runtimeEventPublishAction struct {
	Event   sqlite.RuntimeEventRecord
	Publish func(sqlite.RuntimeEventRecord)
}

func runtimeEventChronologicalLess(left, right sqlite.RuntimeEventRecord) bool {
	leftSeq := left.IngestSeq
	rightSeq := right.IngestSeq
	switch {
	case leftSeq > 0 && rightSeq > 0 && leftSeq != rightSeq:
		return leftSeq < rightSeq
	}

	leftAt, leftOK := parseRuntimeEventCreatedAt(left.CreatedAt)
	rightAt, rightOK := parseRuntimeEventCreatedAt(right.CreatedAt)
	switch {
	case leftOK && rightOK && !leftAt.Equal(rightAt):
		return leftAt.Before(rightAt)
	case leftOK != rightOK:
		return leftOK
	}

	leftEventID := strings.TrimSpace(left.EventID)
	rightEventID := strings.TrimSpace(right.EventID)
	if leftEventID != rightEventID {
		return leftEventID < rightEventID
	}
	leftType := strings.TrimSpace(left.EventType)
	rightType := strings.TrimSpace(right.EventType)
	if leftType != rightType {
		return leftType < rightType
	}
	return strings.TrimSpace(left.EntityID) < strings.TrimSpace(right.EntityID)
}

func parseRuntimeEventCreatedAt(raw string) (time.Time, bool) {
	ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

func (h *Handler) publishRuntimeEventBatchChronological(events []sqlite.RuntimeEventRecord, publish func(sqlite.RuntimeEventRecord)) {
	if publish == nil || len(events) == 0 {
		return
	}
	ordered := append([]sqlite.RuntimeEventRecord(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return runtimeEventChronologicalLess(ordered[i], ordered[j])
	})
	for _, event := range ordered {
		if strings.TrimSpace(event.EventID) == "" {
			continue
		}
		publish(event)
	}
}

func (h *Handler) publishRuntimeEventActionsChronological(actions ...runtimeEventPublishAction) {
	if len(actions) == 0 {
		return
	}
	ordered := make([]runtimeEventPublishAction, 0, len(actions))
	for _, action := range actions {
		if action.Publish == nil || strings.TrimSpace(action.Event.EventID) == "" {
			continue
		}
		ordered = append(ordered, action)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		return runtimeEventChronologicalLess(ordered[i].Event, ordered[j].Event)
	})
	for _, action := range ordered {
		action.Publish(action.Event)
	}
}

func sha256hex(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:])
}

type workspaceDocArchiveParams struct {
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key"`
	ArchivedBy  string `json:"archived_by"`
}

func (h *Handler) workspaceDocArchive(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceDocArchiveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	docKey := strings.TrimSpace(p.DocKey)
	archivedBy := strings.TrimSpace(p.ArchivedBy)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(docKey, "doc_key"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(archivedBy, "archived_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, archivedBy, "archived_by"); rpcErr != nil {
		return nil, rpcErr
	}
	event, invalidationEvents, err := h.store.ArchiveWorkspaceDocWithEffectsAndPromptContext(ctx, workspaceID, docKey, archivedBy,
		h.workspaceDocPromptContextEnvelope(ctx, workspaceID, "workspace.doc.archive", map[string]string{
			"doc_key":     docKey,
			"archived_by": archivedBy,
		}))
	if err != nil {
		if isWorkspaceDocValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		if rpcErr := rpcErrorFromStoreAuthority(err, "workspace.doc.archive"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	actions := []runtimeEventPublishAction{
		{
			Event: event,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, docKey)
			},
		},
	}
	for _, invalidationEvent := range invalidationEvents {
		eventRecord := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventRecord,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, docKey)
			},
		})
	}
	h.publishRuntimeEventActionsChronological(actions...)
	h.touchAgentActivity(ctx, workspaceID, archivedBy)
	return map[string]any{
		"workspace_id": workspaceID,
		"doc_key":      docKey,
		"status":       "ARCHIVED",
	}, nil
}

type workspaceDocDeleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key"`
	DeletedBy   string `json:"deleted_by"`
}

func (h *Handler) workspaceDocDelete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceDocDeleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	docKey := strings.TrimSpace(p.DocKey)
	deletedBy := strings.TrimSpace(p.DeletedBy)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(docKey, "doc_key"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(deletedBy, "deleted_by"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, deletedBy, "deleted_by"); rpcErr != nil {
		return nil, rpcErr
	}
	event, invalidationEvents, err := h.store.DeleteWorkspaceDocWithEffectsAndPromptContext(ctx, workspaceID, docKey, deletedBy,
		h.workspaceDocPromptContextEnvelope(ctx, workspaceID, "workspace.doc.delete", map[string]string{
			"doc_key":    docKey,
			"deleted_by": deletedBy,
		}))
	if err != nil {
		if isWorkspaceDocValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		if rpcErr := rpcErrorFromStoreAuthority(err, "workspace.doc.delete"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	actions := []runtimeEventPublishAction{
		{
			Event: event,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, docKey)
			},
		},
	}
	for _, invalidationEvent := range invalidationEvents {
		eventRecord := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventRecord,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(runtimeEvent, docKey)
			},
		})
	}
	h.publishRuntimeEventActionsChronological(actions...)
	h.touchAgentActivity(ctx, workspaceID, deletedBy)
	return map[string]any{
		"workspace_id": workspaceID,
		"doc_key":      docKey,
		"status":       "DELETED",
	}, nil
}

type workspaceDocHistoryParams struct {
	WorkspaceID string `json:"workspace_id"`
	DocKey      string `json:"doc_key"`
	Limit       int    `json:"limit"`
}

func (h *Handler) workspaceDocHistory(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceDocHistoryParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	docKey := strings.TrimSpace(p.DocKey)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(docKey, "doc_key"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	revisions, err := h.store.ListWorkspaceDocRevisions(ctx, workspaceID, docKey, p.Limit)
	if err != nil {
		if isWorkspaceDocValidationError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"revisions": revisions,
		"count":     len(revisions),
	}, nil
}

func isWorkspaceDocValidationError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return false
	}
	return errors.Is(err, sqlite.ErrWorkspaceNotFound) ||
		strings.Contains(msg, " is required") ||
		strings.Contains(msg, "workspace doc not found") ||
		strings.Contains(msg, "already archived")
}

// ── Webhooks ────────────────────────────────────────────────────────

func (h *Handler) workspaceWebhookRegister(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p sqlite.WebhookInput
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	p.WorkspaceID = workspaceID
	id, err := h.store.RegisterWebhook(ctx, p)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"webhook_id": id,
		"status":     "REGISTERED",
	}, nil
}

type workspaceWebhookListParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) workspaceWebhookList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceWebhookListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return nil, rpcErr
	}
	hooks, err := h.store.ListWebhooks(ctx, workspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"webhooks": hooks,
		"count":    len(hooks),
	}, nil
}

type workspaceWebhookRemoveParams struct {
	WebhookID string `json:"webhook_id"`
}

func (h *Handler) workspaceWebhookRemove(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p workspaceWebhookRemoveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	webhookID := strings.TrimSpace(p.WebhookID)
	if rpcErr := requireTrimmedParam(webhookID, "webhook_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, ok := authPrincipalFromContext(ctx)
	if !ok {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
	}
	hooks, err := h.store.ListWebhooks(ctx, principal.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	found := false
	for _, hook := range hooks {
		if hook.WebhookID == webhookID {
			found = true
			break
		}
	}
	if !found {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	if err := h.store.RemoveWebhook(ctx, webhookID); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"webhook_id": webhookID,
		"status":     "REMOVED",
	}, nil
}

// fireWebhooksAsync sends event payload to all matching webhooks in background.
func (h *Handler) fireWebhooksAsync(workspaceID, eventType string, payload map[string]any) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		hooks, err := h.store.GetActiveWebhooks(ctx, workspaceID, eventType)
		if err != nil || len(hooks) == 0 {
			return
		}
		body, _ := json.Marshal(map[string]any{
			"event":        eventType,
			"workspace_id": workspaceID,
			"payload":      payload,
			"timestamp":    time.Now().UTC().Format(time.RFC3339Nano),
		})

		// Setup a secure HTTP client that forbids proxying to inner network
		dialer := &net.Dialer{
			Timeout: 5 * time.Second,
		}
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: func(c context.Context, network, addr string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				ips, err := net.LookupIP(host)
				if err != nil {
					return nil, err
				}
				if len(ips) == 0 {
					return nil, fmt.Errorf("SSRF prevention: no IPs resolved for host %s", host)
				}
				for _, ip := range ips {
					if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
						return nil, fmt.Errorf("SSRF prevention: invalid target IP %s", ip.String())
					}
				}
				return dialer.DialContext(c, network, net.JoinHostPort(ips[0].String(), port))
			},
			DisableKeepAlives: true,
		}
		client := &http.Client{
			Timeout:   5 * time.Second,
			Transport: transport,
		}

		sem := make(chan struct{}, 10)
		var wg sync.WaitGroup
		for _, hook := range hooks {
			sem <- struct{}{}
			wg.Add(1)
			go func(h sqlite.WebhookRecord) {
				defer wg.Done()
				defer func() { <-sem }()
				hookCtx, hookCancel := context.WithTimeout(ctx, 5*time.Second)
				defer hookCancel()
				req, err := http.NewRequestWithContext(hookCtx, "POST", h.URL, bytes.NewReader(body))
				if err != nil {
					return
				}
				// Verify scheme
				if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
					log.Printf("webhook %s failed: unsupported scheme %s", h.WebhookID, req.URL.Scheme)
					return
				}

				req.Header.Set("Content-Type", "application/json")
				if h.Secret != "" {
					sig := sha256hex(h.Secret + string(body))
					req.Header.Set("X-Rhizome-Signature", sig)
				}
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("webhook %s failed: %v", h.WebhookID, err)
					return
				}
				resp.Body.Close()
			}(hook)
		}
		wg.Wait()
	}()
}

// ── Tool ────────────────────────────────────────────────────────────

type toolListParams struct {
	WorkspaceID string `json:"workspace_id"`
	Status      string `json:"status"`
}

func (h *Handler) toolList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p toolListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	tools, err := h.store.ListWorkspaceTools(ctx, sqlite.WorkspaceToolFilter{
		WorkspaceID: p.WorkspaceID,
		Status:      p.Status,
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"tools": tools,
		"count": len(tools),
	}, nil
}

type toolRegisterParams struct {
	WorkspaceID  string          `json:"workspace_id"`
	ToolID       string          `json:"tool_id"`
	DisplayName  string          `json:"display_name"`
	Description  string          `json:"description"`
	OwnerUserID  string          `json:"owner_user_id"`
	OwnerAgentID string          `json:"owner_agent_id"`
	Kind         string          `json:"kind"`
	Status       string          `json:"status"`
	Version      string          `json:"version"`
	AccessLevel  string          `json:"access_level"`
	Endpoint     string          `json:"endpoint"`
	Capabilities []string        `json:"capabilities"`
	ManifestJSON string          `json:"manifest_json"`
	InputSchema  json.RawMessage `json:"input_schema,omitempty"`
}

func (h *Handler) toolRegister(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p toolRegisterParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}

	// Merge input_schema into manifest_json if provided
	manifestJSON := p.ManifestJSON
	if len(p.InputSchema) > 0 {
		var manifest map[string]any
		if manifestJSON != "" {
			_ = json.Unmarshal([]byte(manifestJSON), &manifest)
		}
		if manifest == nil {
			manifest = make(map[string]any)
		}
		var schema any
		_ = json.Unmarshal(p.InputSchema, &schema)
		manifest["input_schema"] = schema
		mj, _ := json.Marshal(manifest)
		manifestJSON = string(mj)
	}

	if err := h.store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:  p.WorkspaceID,
		ToolID:       p.ToolID,
		DisplayName:  p.DisplayName,
		Description:  p.Description,
		OwnerUserID:  p.OwnerUserID,
		OwnerAgentID: p.OwnerAgentID,
		Kind:         p.Kind,
		Status:       p.Status,
		Version:      p.Version,
		AccessLevel:  p.AccessLevel,
		Endpoint:     p.Endpoint,
		Capabilities: p.Capabilities,
		ManifestJSON: manifestJSON,
		PromptContextEnvelope: sqlite.BuildToolRegistryPromptContextEnvelope(
			"tool.register",
			"server_rpc",
			p.WorkspaceID,
			principal.PrincipalType,
			principal.PrincipalID,
		),
		PromptContextSurface:       "tool.register",
		PromptContextPrincipalType: principal.PrincipalType,
		PromptContextPrincipalID:   principal.PrincipalID,
	}); err != nil {
		if rpcErr := authorityRejectRPCError(err, "tool.register"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"tool_id": p.ToolID,
		"status":  "REGISTERED",
	}, nil
}

type toolStatusParams struct {
	WorkspaceID string `json:"workspace_id"`
	ToolID      string `json:"tool_id"`
}

func (h *Handler) toolStatus(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p toolStatusParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	if _, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID); rpcErr != nil {
		return nil, rpcErr
	}

	tool, err := h.store.GetWorkspaceTool(ctx, p.WorkspaceID, p.ToolID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return tool, nil
}

// ── Approval ────────────────────────────────────────────────────────

type approvalListParams struct {
	Status string `json:"status"`
}

func (h *Handler) approvalList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p approvalListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	approvals, err := h.store.ListApprovalRequests(ctx, p.Status)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"approvals": approvals,
		"count":     len(approvals),
	}, nil
}

type approvalDecideParams struct {
	ApprovalID   string `json:"approval_id"`
	NewStatus    string `json:"new_status"`
	DecidedBy    string `json:"decided_by"`
	DecisionNote string `json:"decision_note"`
}

func (h *Handler) approvalDecide(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p approvalDecideParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	if err := h.store.DecideApproval(ctx, sqlite.ApprovalDecisionInput{
		ApprovalID:   p.ApprovalID,
		NewStatus:    p.NewStatus,
		DecidedBy:    p.DecidedBy,
		DecisionNote: p.DecisionNote,
	}); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"approval_id": p.ApprovalID,
		"status":      p.NewStatus,
	}, nil
}

// ── FinOps ──────────────────────────────────────────────────────────

type finopsSpendParams struct {
	TxID        string  `json:"tx_id"`
	OwnerUserID string  `json:"owner_user_id"`
	TaskID      string  `json:"task_id"`
	NodeID      string  `json:"node_id"`
	ServiceID   string  `json:"service_id"`
	AmountUSD   float64 `json:"amount_usd"`
}

func (h *Handler) finopsSpend(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p finopsSpendParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	if err := h.store.RecordSpendTransaction(ctx, sqlite.SpendTransactionInput{
		TxID:        p.TxID,
		OwnerUserID: p.OwnerUserID,
		TaskID:      p.TaskID,
		NodeID:      p.NodeID,
		ServiceID:   p.ServiceID,
		AmountUSD:   p.AmountUSD,
	}); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"tx_id":  p.TxID,
		"status": "RECORDED",
	}, nil
}

type finopsLedgerParams struct {
	TaskID string `json:"task_id"`
}

func (h *Handler) finopsLedger(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p finopsLedgerParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	txns, err := h.store.ListSpendTransactionsByTask(ctx, p.TaskID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"transactions": txns,
		"count":        len(txns),
	}, nil
}

// ── Helpers ─────────────────────────────────────────────────────────

func writeRPCError(w http.ResponseWriter, id any, code int, message string) {
	writeRPCResponse(w, RPCResponse{
		JSONRPC: "2.0",
		Error:   &RPCError{Code: code, Message: message},
		ID:      id,
	})
}

func writeRPCResponse(w http.ResponseWriter, resp RPCResponse) {
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	if resp.Error != nil {
		status = http.StatusOK // JSON-RPC always returns 200
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

// ── Limits ──────────────────────────────────────────────────────────

type limitsGroupCreateParams struct {
	GroupID          string `json:"group_id"`
	WorkspaceID      string `json:"workspace_id"`
	Title            string `json:"title"`
	OwnerName        string `json:"owner_name"`
	SubscriptionTier string `json:"subscription_tier"`
	DailyLimit       int    `json:"daily_limit"`
	WeeklyLimit      int    `json:"weekly_limit"`
	ActorID          string `json:"actor_id"`
}

func (h *Handler) limitsGroupCreate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p limitsGroupCreateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	groupID := strings.TrimSpace(p.GroupID)
	title := strings.TrimSpace(p.Title)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(groupID, "group_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if !validIDRegex.MatchString(groupID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "invalid group_id format"}
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	group, event, err := h.store.CreateLimitGroupWithEvent(ctx, sqlite.LimitGroupCreateInput{
		GroupID:          groupID,
		WorkspaceID:      workspaceID,
		Title:            title,
		OwnerName:        p.OwnerName,
		SubscriptionTier: p.SubscriptionTier,
		DailyLimit:       p.DailyLimit,
		WeeklyLimit:      p.WeeklyLimit,
		ActorID:          actorID,
		ActorType:        principal.PrincipalType,
		PromptContextEnvelope: h.limitGroupPromptContextEnvelope(ctx, workspaceID, "limits.group.create", map[string]string{
			"group_id": groupID,
			"actor_id": actorID,
		}),
		PromptContextSurface: "limits.group.create",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "limits.group.create")
	}
	h.publishRuntimeEventRecord(event, title, groupID)
	return map[string]any{"group": group}, nil
}

type limitsGroupUpdateParams struct {
	WorkspaceID      string   `json:"workspace_id"`
	GroupID          string   `json:"group_id"`
	Title            string   `json:"title"`
	OwnerName        string   `json:"owner_name"`
	SubscriptionTier string   `json:"subscription_tier"`
	DailyLimit       *int     `json:"daily_limit"`
	WeeklyLimit      *int     `json:"weekly_limit"`
	AgentIDs         []string `json:"agent_ids"`
	ActorID          string   `json:"actor_id"`
}

func (h *Handler) limitsGroupUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p limitsGroupUpdateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	groupID := strings.TrimSpace(p.GroupID)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(groupID, "group_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	group, event, err := h.store.UpdateLimitGroupWithEvent(ctx, sqlite.LimitGroupUpdateInput{
		WorkspaceID:      workspaceID,
		GroupID:          groupID,
		Title:            p.Title,
		OwnerName:        p.OwnerName,
		SubscriptionTier: p.SubscriptionTier,
		DailyLimit:       p.DailyLimit,
		WeeklyLimit:      p.WeeklyLimit,
		AgentIDs:         p.AgentIDs,
		ActorID:          actorID,
		ActorType:        principal.PrincipalType,
		PromptContextEnvelope: h.limitGroupPromptContextEnvelope(ctx, workspaceID, "limits.group.update", map[string]string{
			"group_id": groupID,
			"actor_id": actorID,
		}),
		PromptContextSurface: "limits.group.update",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "limits.group.update")
	}
	h.publishRuntimeEventRecord(event, groupID)
	return group, nil
}

type limitsGroupGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	GroupID     string `json:"group_id"`
}

func (h *Handler) limitsGroupGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p limitsGroupGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	group, err := h.store.GetLimitGroup(ctx, p.WorkspaceID, p.GroupID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return group, nil
}

type agentLimitsGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
}

func (h *Handler) agentLimitsGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p agentLimitsGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireAgentPrincipal(ctx, p.WorkspaceID, p.AgentID, "agent_id"); rpcErr != nil {
		return nil, rpcErr
	}

	// auto-bootstrap agent into a group if not already in one
	if err := h.store.EnsureAgentLimitGroup(ctx, p.WorkspaceID, p.AgentID, p.AgentID); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: fmt.Sprintf("ensure group: %v", err)}
	}

	group, err := h.store.GetAgentLimitGroup(ctx, p.WorkspaceID, p.AgentID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if group == nil {
		return nil, &RPCError{Code: errCodeInternal, Message: "agent has no limit group"}
	}
	return group, nil
}

type limitsGroupListParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) limitsGroupList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p limitsGroupListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	groups, err := h.store.ListLimitGroups(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if groups == nil {
		groups = []sqlite.LimitGroupRecord{}
	}
	return map[string]any{
		"groups": groups,
		"count":  len(groups),
	}, nil
}

type limitsGroupDeleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	GroupID     string `json:"group_id"`
	ActorID     string `json:"actor_id"`
}

func (h *Handler) limitsGroupDelete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p limitsGroupDeleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	groupID := strings.TrimSpace(p.GroupID)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(groupID, "group_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	event, err := h.store.DeleteLimitGroupWithEvent(ctx, sqlite.LimitGroupDeleteInput{
		WorkspaceID: workspaceID,
		GroupID:     groupID,
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.limitGroupPromptContextEnvelope(ctx, workspaceID, "limits.group.delete", map[string]string{
			"group_id": groupID,
			"actor_id": actorID,
		}),
		PromptContextSurface: "limits.group.delete",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "limits.group.delete")
	}
	h.publishRuntimeEventRecord(event, groupID)
	return map[string]any{
		"group_id": groupID,
		"status":   "DELETED",
	}, nil
}

type limitsReportParams struct {
	GroupID         string `json:"group_id"`
	AgentID         string `json:"agent_id"`
	DailyRemaining  int    `json:"daily_remaining"`
	WeeklyRemaining int    `json:"weekly_remaining"`
}

func (h *Handler) limitsReport(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p limitsReportParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	principal, ok := authPrincipalFromContext(ctx)
	if !ok {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
	}
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "agent principal required"}
	}
	if strings.TrimSpace(p.AgentID) == "" || strings.TrimSpace(p.AgentID) != strings.TrimSpace(principal.PrincipalID) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}
	if _, err := h.store.GetLimitGroup(ctx, principal.WorkspaceID, p.GroupID); err != nil {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "budget scope mismatch: token identity does not match group_id"}
	}
	if err := h.store.ReportLimits(ctx, sqlite.LimitReportInput{
		GroupID:         p.GroupID,
		AgentID:         p.AgentID,
		DailyRemaining:  p.DailyRemaining,
		WeeklyRemaining: p.WeeklyRemaining,
	}); err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"group_id":         p.GroupID,
		"agent_id":         p.AgentID,
		"daily_remaining":  p.DailyRemaining,
		"weekly_remaining": p.WeeklyRemaining,
		"status":           "REPORTED",
	}, nil
}

type limitsBootstrapParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) limitsBootstrap(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p limitsBootstrapParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	created, err := h.store.BootstrapLimitGroups(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return map[string]any{
		"created": created,
		"status":  "BOOTSTRAPPED",
	}, nil
}

type limitsSnapshotsParams struct {
	GroupID string `json:"group_id"`
	Days    int    `json:"days"`
}

func (h *Handler) limitsSnapshots(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p limitsSnapshotsParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	snaps, err := h.store.ListLimitSnapshotsByRange(ctx, p.GroupID, p.Days)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if snaps == nil {
		snaps = []sqlite.LimitSnapshotRecord{}
	}
	return map[string]any{
		"snapshots": snaps,
		"count":     len(snaps),
	}, nil
}

// ── News ─────────────────────────────────────────────────────────────

type newsPublishParams struct {
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	AuthorID    string `json:"author_id"`
	AuthorType  string `json:"author_type"`
}

func (h *Handler) newsPublish(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p newsPublishParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	title := strings.TrimSpace(p.Title)
	authorID := strings.TrimSpace(p.AuthorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(authorID, "author_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, authorID, "author_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if authorType := strings.TrimSpace(p.AuthorType); authorType != "" && !strings.EqualFold(authorType, strings.TrimSpace(principal.PrincipalType)) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "author_type mismatch: token identity does not match author_type"}
	}
	news, event, err := h.store.CreateNewsWithEvent(ctx, sqlite.NewsCreateInput{
		WorkspaceID: workspaceID,
		Title:       title,
		Content:     p.Content,
		AuthorID:    authorID,
		AuthorType:  principal.PrincipalType,
		ActorID:     authorID,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.newsPromptContextEnvelope(ctx, workspaceID, "news.publish", map[string]string{
			"actor_id":  authorID,
			"author_id": authorID,
		}),
		PromptContextSurface: "news.publish",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "news.publish")
	}
	h.publishRuntimeEventRecord(event, news.Title, news.NewsID)
	h.touchAgentActivity(ctx, news.WorkspaceID, news.AuthorID)

	// Broadcast notification to all agents
	notif := fmt.Sprintf("📰 %s published a news update: \"%s\"", news.AuthorID, news.Title)
	metadataRaw, _ := json.Marshal(map[string]any{
		"news_id":     news.NewsID,
		"title":       news.Title,
		"author_id":   news.AuthorID,
		"author_type": news.AuthorType,
		"created_at":  news.CreatedAt,
	})
	_, _ = h.store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID:  news.WorkspaceID,
		FromAgentID:  "system",
		ToAgentID:    "", // broadcast
		Channel:      "news",
		ContentType:  "text/plain",
		Content:      notif,
		MetadataJSON: string(metadataRaw),
	})

	return news, nil
}

type newsPollParams struct {
	WorkspaceID    string `json:"workspace_id"`
	AfterCreatedAt string `json:"after_created_at"`
	AfterNewsID    string `json:"after_news_id"`
	Limit          int    `json:"limit"`
	LookbackHours  int    `json:"lookback_hours"`
}

func newsPollResponse(items []sqlite.NewsRecord, fallbackCreatedAt, fallbackNewsID string) map[string]any {
	nextCreatedAt := strings.TrimSpace(fallbackCreatedAt)
	nextNewsID := strings.TrimSpace(fallbackNewsID)
	if len(items) > 0 {
		last := items[len(items)-1]
		nextCreatedAt = strings.TrimSpace(last.CreatedAt)
		nextNewsID = strings.TrimSpace(last.NewsID)
	}
	return map[string]any{
		"items":                  items,
		"count":                  len(items),
		"next_cursor_created_at": nextCreatedAt,
		"next_cursor_news_id":    nextNewsID,
	}
}

func (h *Handler) newsPoll(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p newsPollParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := requireTrimmedParam(p.WorkspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	p.AfterCreatedAt = strings.TrimSpace(p.AfterCreatedAt)
	p.AfterNewsID = strings.TrimSpace(p.AfterNewsID)
	if p.AfterCreatedAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, p.AfterCreatedAt); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "after_created_at must be RFC3339Nano"}
		}
	}
	if p.AfterCreatedAt == "" && p.AfterNewsID != "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "after_news_id requires after_created_at"}
	}
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if p.LookbackHours <= 0 {
		p.LookbackHours = 24
	}

	items, err := h.store.ListNewsAfter(ctx, p.WorkspaceID, p.AfterCreatedAt, p.AfterNewsID, p.Limit, p.LookbackHours)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if items == nil {
		items = []sqlite.NewsRecord{}
	}
	return newsPollResponse(items, p.AfterCreatedAt, p.AfterNewsID), nil
}

type newsListParams struct {
	WorkspaceID string `json:"workspace_id"`
	Limit       int    `json:"limit"`
}

func (h *Handler) newsList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p newsListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	items, err := h.store.ListNews(ctx, p.WorkspaceID, p.Limit)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if items == nil {
		items = []sqlite.NewsRecord{}
	}
	return map[string]any{"items": items, "count": len(items)}, nil
}

type newsDeleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	NewsID      string `json:"news_id"`
	ActorID     string `json:"actor_id"`
}

func (h *Handler) newsDelete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p newsDeleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	newsID := strings.TrimSpace(p.NewsID)
	actorID := strings.TrimSpace(p.ActorID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(newsID, "news_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
	if rpcErr != nil {
		return nil, rpcErr
	}
	event, err := h.store.DeleteNewsWithEvent(ctx, sqlite.NewsDeleteInput{
		WorkspaceID: workspaceID,
		NewsID:      newsID,
		ActorID:     actorID,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.newsPromptContextEnvelope(ctx, workspaceID, "news.delete", map[string]string{
			"news_id":  newsID,
			"actor_id": actorID,
		}),
		PromptContextSurface: "news.delete",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "news.delete")
	}
	h.publishRuntimeEventRecord(event, newsID)
	h.touchAgentActivity(ctx, workspaceID, actorID)
	return map[string]any{"deleted": true, "event": event, "news_id": newsID}, nil
}
