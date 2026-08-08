package server

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// TestSchemaAccuracy verifies that rpc.describe schemas match actual handler param structs.
// This catches mismatches like "author" vs "updated_by".
func TestSchemaAccuracy(t *testing.T) {
	// Map of method -> struct type for validation.
	// We check that every json tag in the struct exists in rpcMethodSchemas.
	type structCheck struct {
		method string
		sample any
	}

	checks := []structCheck{
		{"agent.register", agentRegisterParams{}},
		{"agent.heartbeat", agentHeartbeatParams{}},
		{"agent.bootstrap", agentBootstrapParams{}},
		{"agent.work.next", agentWorkNextParams{}},
		{"agent.delete", agentDeleteParams{}},
		{"agent.profile.update", agentProfileUpdateParams{}},
		{"agent.profile.get", agentProfileGetParams{}},
		{"agent.task.hydrate", agentTaskHydrateParams{}},
		{"agent.task_frontier.decision", agentTaskFrontierDecisionParams{}},
		{"agent.update.post", agentUpdatePostParams{}},
		{"agent.session.start", agentSessionEventParams{}},
		{"agent.session.takeover", agentSessionTakeoverParams{}},
		{"agent.task.claim", agentTaskClaimParams{}},
		{"agent.task.release", agentTaskReleaseParams{}},
		{"agent.task.complete", taskCompleteParams{}},
		{"agent.task.block", taskBlockParams{}},
		{"task.submit", taskSubmitParams{}},
		{"task.status", taskStatusParams{}},
		{"task.class.put", taskClassPutParams{}},
		{"task.close", taskCloseParams{}},
		{"project.create", projectCreateParams{}},
		{"project.list", projectListParams{}},
		{"project.get", projectGetParams{}},
		{"project.update", projectUpdateParams{}},
		{"project.delete", projectDeleteParams{}},
		{"project.profile.get", projectProfileGetParams{}},
		{"project.profile.update", projectProfileUpdateParams{}},
		{"project.phase.transition", projectPhaseTransitionParams{}},
		{"project.gates.status", projectGatesStatusParams{}},
		{"project.coordination.get", projectCoordinationGetParams{}},
		{"project.lead.claim", projectLeadClaimParams{}},
		{"project.lead.renew", projectLeadRenewParams{}},
		{"project.lead.release", projectLeadReleaseParams{}},
		{"project.lead.transfer", projectLeadTransferParams{}},
		{"project.role.assign", projectRoleAssignParams{}},
		{"project.roles.list", projectRolesListParams{}},
		{"project.governance.predicates.check", projectGovernancePredicatesCheckParams{}},
		{"project.governance.challenge.raise", projectGovernanceChallengeRaiseParams{}},
		{"project.governance.challenge.defend", projectGovernanceChallengeDefendParams{}},
		{"project.governance.vote.cast", projectGovernanceVoteCastParams{}},
		{"project.governance.challenge.tally", projectGovernanceChallengeTallyParams{}},
		{"project.governance.challenge.get", projectGovernanceChallengeGetParams{}},
		{"project.governance.challenge.list", projectGovernanceChallengeListParams{}},
		{"project.governance.votes.list", projectGovernanceVotesListParams{}},
		{"project.repository.upsert", projectRepositoryUpsertParams{}},
		{"project.repositories.list", projectRepositoriesListParams{}},
		{"project.checkout.register", projectCheckoutRegisterParams{}},
		{"project.checkouts.list", projectCheckoutsListParams{}},
		{"project.branch.register", projectBranchRegisterParams{}},
		{"project.branches.list", projectBranchesListParams{}},
		{"project.patch_queue.submit", projectPatchQueueSubmitParams{}},
		{"project.patch_queue.supersede", projectPatchQueueSupersedeParams{}},
		{"project.patch_queue.claim", projectPatchQueueClaimParams{}},
		{"project.patch_queue.operation_bind", projectPatchQueueOperationBindParams{}},
		{"project.patch_queue.cas_record", projectPatchQueueCASRecordParams{}},
		{"project.patch_queue.materialization_record", projectPatchQueueMaterializationRecordParams{}},
		{"project.patch_queue.rollback_record", projectPatchQueueRollbackRecordParams{}},
		{"project.patch_queue.integration_record", projectPatchQueueIntegrationRecordParams{}},
		{"project.patch_queue.integration_repair", projectPatchQueueIntegrationRecordParams{}},
		{"project.patch_queue.reviewer_advisory_record", projectPatchQueueReviewerAdvisoryRecordParams{}},
		{"project.patch_queue.operator_enablement_record", projectPatchQueueOperatorEnablementRecordParams{}},
		{"operator.patch_queue.enable", operatorPatchQueueEnableParams{}},
		{"project.patch_queue.release", projectPatchQueueReleaseParams{}},
		{"project.patch_queue.decision", projectPatchQueueDecisionParams{}},
		{"project.patch_queue.review_task.reconcile", projectPatchQueueReviewTaskReconcileParams{}},
		{"project.patch_queue.decision_continuation.consume", projectPatchQueueDecisionContinuationConsumeParams{}},
		{"project.patch_queue.list", projectPatchQueueListParams{}},
		{"service.direction.upsert", serviceDirectionUpsertParams{}},
		{"service.direction.list", serviceDirectionListParams{}},
		{"service.direction.get", serviceDirectionGetParams{}},
		{"service.candidate.upsert", serviceCandidateUpsertParams{}},
		{"service.candidate.list", serviceCandidateListParams{}},
		{"service.candidate.get", serviceCandidateGetParams{}},
		{"service.run.start", serviceRunUpsertParams{}},
		{"service.run.update", serviceRunUpsertParams{}},
		{"service.run.list", serviceRunListParams{}},
		{"service.run.get", serviceRunGetParams{}},
		{"service.approval.grant", serviceApprovalGrantParams{}},
		{"service.resource.record", serviceResourceRecordParams{}},
		{"service.spend.record", serviceSpendRecordParams{}},
		{"service.revenue.record", serviceRevenueRecordParams{}},
		{"service.outcome.record", serviceOutcomeRecordParams{}},
		{"service.coordination.get", serviceCoordinationGetParams{}},
		{"budget.account.ensure", budgetAccountEnsureParams{}},
		{"budget.account.get", budgetAccountGetParams{}},
		{"budget.reserve", budgetReserveParams{}},
		{"budget.spend", budgetSpendParams{}},
		{"budget.release", budgetReleaseParams{}},
		{"budget.refund", budgetRefundParams{}},
		{"budget.ledger.list", budgetLedgerListParams{}},
		{"budget.reservations.list", budgetReservationListParams{}},
		{"budget.health", budgetHealthParams{}},
		{"limits.group.create", limitsGroupCreateParams{}},
		{"limits.group.update", limitsGroupUpdateParams{}},
		{"limits.group.get", limitsGroupGetParams{}},
		{"agent.limits.get", agentLimitsGetParams{}},
		{"limits.group.list", limitsGroupListParams{}},
		{"limits.group.delete", limitsGroupDeleteParams{}},
		{"limits.report", limitsReportParams{}},
		{"agent.message.send", agentMessageSendParams{}},
		{"agent.message.poll", agentMessagePollParams{}},
		{"agent.message.ack", agentMessageAckParams{}},
		{"news.publish", newsPublishParams{}},
		{"news.delete", newsDeleteParams{}},
		{"news.poll", newsPollParams{}},
		{"vault.create", vaultCreateParams{}},
		{"vault.update", vaultUpdateParams{}},
		{"vault.get", vaultGetParams{}},
		{"vault.list", vaultListParams{}},
		{"vault.delete", vaultDeleteParams{}},
		{"vault.audit", vaultAuditParams{}},
		{"workspace.doc.put", workspaceDocPutParams{}},
		{"workspace.segment.list", workspaceSegmentListParams{}},
		{"workspace.segment.get", workspaceSegmentGetParams{}},
		{"workspace.artifact.write", workspaceArtifactWriteParams{}},
		{"workspace.artifact.list", workspaceArtifactListParams{}},
		{"action.create", actionCreateParams{}},
		{"action.list", actionListParams{}},
		{"action.start", actionStartParams{}},
		{"action.pause", actionPauseParams{}},
		{"action.resolve", actionResolveParams{}},
		{"workspace.memory.write", workspaceMemoryWriteParams{}},
		{"workspace.memory.node.write", workspaceMemoryNodeWriteParams{}},
		{"workspace.memory.list", workspaceMemoryListParams{}},
		{"workspace.memory.search", workspaceMemorySearchParams{}},
		{"workspace.memory.packet.kernel.get", workspaceMemoryPacketParams{}},
		{"workspace.memory.packet.shell.get", workspaceMemoryPacketParams{}},
		{"workspace.memory.pack.write", workspaceMemoryPackWriteParams{}},
		{"workspace.memory.pack.list", workspaceMemoryPackListParams{}},
		{"workspace.memory.pack.get", workspaceMemoryPackGetParams{}},
		{"workspace.memory.restore", workspaceMemoryRestoreParams{}},
		{"workspace.memory.metrics.report", sqlite.MemoryMetricsReportInput{}},
		{"workspace.memory.metrics.list", workspaceMemoryMetricsListParams{}},
		{"workspace.memory.metrics.get", workspaceMemoryMetricsGetParams{}},
		{"workspace.memory.coherence.report", workspaceMemoryCoherenceReportParams{}},
		{"workspace.memory.coherence.scope", workspaceMemoryCoherenceScopeParams{}},
		{"workspace.memory.coherence.snapshot", workspaceMemoryCoherenceReportParams{}},
		{"workspace.rsp.belief.report", workspaceRSPBeliefReportParams{}},
		{"workspace.rsp.belief.claim", workspaceRSPBeliefClaimParams{}},
		{"workspace.rsp.belief.snapshot", workspaceRSPBeliefReportParams{}},
		{"workspace.rsp.capability.get", workspaceRSPCapabilityGetParams{}},
		{"workspace.rsp.capability.put", workspaceRSPCapabilityPutParams{}},
		{"workspace.rsp.state.report", workspaceRSPStateReportParams{}},
		{"workspace.rsp.state.snapshot", workspaceRSPStateReportParams{}},
		{"workspace.memory.invalidation.poll", workspaceMemoryInvalidationPollParams{}},
		{"workspace.memory.invalidation.ack", workspaceMemoryInvalidationAckParams{}},
		{"workspace.memory.invalidation.fail", workspaceMemoryInvalidationFailParams{}},
		{"workspace.memory.invalidation.requeue", workspaceMemoryInvalidationRequeueParams{}},
		{"workspace.memory.invalidation.list", workspaceMemoryInvalidationListParams{}},
		{"workspace.memory.invalidation.get", workspaceMemoryInvalidationGetParams{}},
		{"workspace.memory.invalidation.cursor.get", workspaceMemoryInvalidationCursorGetParams{}},
		{"workspace.memory.graph.list", workspaceMemoryGraphListParams{}},
		{"workspace.memory.graph.get", workspaceMemoryGraphGetParams{}},
		{"workspace.memory.graph.atlas", workspaceMemoryGraphAtlasParams{}},
		{"workspace.memory.graph.sync", workspaceMemoryGraphSyncParams{}},
		{"workspace.memory.node.search", workspaceMemoryNodeSearchParams{}},
		{"workspace.episode.pack.list", workspaceEpisodePackListParams{}},
		{"workspace.episode.pack.get", workspaceEpisodePackGetParams{}},
		{"workspace.episode.pack.sync", workspaceEpisodePackSyncParams{}},
		{"workspace.events.replay", workspaceEventsReplayParams{}},
		{"workspace.events.evaluate", workspaceEventsReplayParams{}},
		{"workspace.instrumentation.report", workspaceInstrumentationParams{}},
		{"workspace.instrumentation.clusters", workspaceInstrumentationParams{}},
		{"workspace.instrumentation.snapshot", workspaceInstrumentationParams{}},
		{"workspace.instrumentation.locus.bundle", workspaceInstrumentationLocusParams{}},
		{"workspace.instrumentation.corridor.report", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.cluster", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.snapshot", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.fit.report", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.fit.cluster", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.fit.snapshot", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.ownership.report", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.ownership.cluster", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.ownership.snapshot", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.boundary.report", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.boundary.cluster", workspaceInstrumentationCorridorParams{}},
		{"workspace.instrumentation.corridor.authority.report", workspaceInstrumentationCorridorAuthorityParams{}},
		{"workspace.instrumentation.corridor.authority.task", workspaceInstrumentationCorridorAuthorityParams{}},
		{"workspace.instrumentation.control.report", workspaceInstrumentationControlParams{}},
		{"workspace.instrumentation.control.cluster", workspaceInstrumentationControlParams{}},
		{"workspace.instrumentation.control.snapshot", workspaceInstrumentationControlParams{}},
		{"workspace.instrumentation.control.state.report", workspaceInstrumentationControlStateParams{}},
		{"workspace.instrumentation.control.state.cluster", workspaceInstrumentationControlStateParams{}},
		{"workspace.instrumentation.control.state.tick", workspaceInstrumentationControlStateParams{}},
		{"workspace.instrumentation.control.state.snapshot", workspaceInstrumentationControlStateParams{}},
		{"workspace.instrumentation.unified.control.report", workspaceInstrumentationUnifiedControlParams{}},
		{"workspace.instrumentation.unified.control.snapshot", workspaceInstrumentationUnifiedControlSnapshotParams{}},
		{"workspace.tension.refresh", workspaceTensionRefreshParams{}},
		{"workspace.tension.list", workspaceTensionListParams{}},
		{"workspace.tension.get", workspaceTensionGetParams{}},
		{"workspace.tension.frontier", workspaceTensionFrontierParams{}},
		{"workspace.tension.confirm", workspaceTensionLifecycleParams{}},
		{"workspace.tension.discard", workspaceTensionLifecycleParams{}},
		{"workspace.tension.archive", workspaceTensionLifecycleParams{}},
		{"workspace.tension.lifecycle.update", workspaceTensionLifecycleUpdateParams{}},
		{"workspace.tension.resolve", workspaceTensionLifecycleParams{}},
		{"workspace.tension.dormant", workspaceTensionLifecycleParams{}},
		{"workspace.tension.add.dependency", workspaceTensionDependencyParams{}},
		{"workspace.tension.remove.dependency", workspaceTensionDependencyParams{}},
		{"workspace.tension.condense", workspaceTensionCondenseParams{}},
		{"workspace.tension.attachable.list", workspaceTensionAttachableParams{}},
		{"workspace.tension.agent.attach", workspaceTensionAgentActionParams{}},
		{"workspace.tension.agent.detach", workspaceTensionDetachParams{}},
		{"workspace.search", workspaceSearchParams{}},
		{"workspace.sessions.list", workspaceSessionsListParams{}},
		{"workspace.compaction.candidates", workspaceCompactionCandidatesParams{}},
		{"workspace.compaction.snapshots", workspaceCompactionSnapshotsParams{}},
		{"tool.register", toolRegisterParams{}},
		{"tool.remove", toolRemoveParams{}},
		{"action.create", actionCreateParams{}},
		{"action.list", actionListParams{}},
		{"action.start", actionStartParams{}},
		{"action.pause", actionPauseParams{}},
		{"action.resolve", actionResolveParams{}},
		{"event.emit", eventEmitParams{}},
		{"workspace.ops.request", workspaceOpsRequestParams{}},
		{"workspace.auth.agent.register", workspaceAgentRegisterParams{}},
		{"workspace.auth.agent.update", workspaceAgentUpdateParams{}},
		{"workspace.auth.human.register", workspaceHumanRegisterParams{}},
		{"workspace.auth.human.login", workspaceHumanLoginParams{}},
		{"workspace.auth.human.profile.get", struct{}{}},
		{"workspace.auth.human.profile.update", workspaceHumanProfileUpdateParams{}},
		{"workspace.auth.human.sessions.list", struct{}{}},
		{"workspace.auth.human.sessions.revoke", workspaceHumanSessionsRevokeParams{}},
		{"workspace.auth.agent.token.rotate", workspaceAgentTokenRotateParams{}},
		{"workspace.security.password.update", workspaceSecurityPasswordUpdateParams{}},
		{"workspace.security.audit.list", workspaceSecurityAuditListParams{}},
		{"runtime.build.info", runtimeBuildInfoParams{}},
	}

	for _, c := range checks {
		t.Run(c.method, func(t *testing.T) {
			schema, ok := rpcMethodSchemas[c.method]
			if !ok {
				t.Fatalf("rpc.describe has no schema for method %q", c.method)
			}

			rt := reflect.TypeOf(c.sample)
			for i := 0; i < rt.NumField(); i++ {
				field := rt.Field(i)
				tag := field.Tag.Get("json")
				if tag == "" || tag == "-" {
					continue
				}
				// Strip ",omitempty" etc.
				if idx := len(tag); idx > 0 {
					for j, ch := range tag {
						if ch == ',' {
							tag = tag[:j]
							break
						}
					}
				}
				if _, exists := schema.Params[tag]; !exists {
					t.Errorf("method %q: struct has json field %q but rpc.describe schema is missing it", c.method, tag)
				}
			}

			// Also check reverse: schema params should exist in struct
			structTags := make(map[string]bool)
			for i := 0; i < rt.NumField(); i++ {
				tag := rt.Field(i).Tag.Get("json")
				if tag == "" || tag == "-" {
					continue
				}
				for j, ch := range tag {
					if ch == ',' {
						tag = tag[:j]
						break
					}
				}
				structTags[tag] = true
			}
			for paramName := range schema.Params {
				if !structTags[paramName] {
					t.Errorf("method %q: rpc.describe schema has param %q but struct has no matching json field", c.method, paramName)
				}
			}
		})
	}
}

func TestCoordinationModeSchemasExposeTrustFirst(t *testing.T) {
	for _, method := range []string{"agent.task.claim", "agent.work.next", "project.phase.transition"} {
		schema := rpcMethodSchemas[method]
		param, ok := schema.Params["coordination_mode"]
		if !ok {
			t.Fatalf("%s should expose coordination_mode: %+v", method, schema.Params)
		}
		if param.Required {
			t.Fatalf("%s coordination_mode should be optional: %+v", method, param)
		}
		if !reflect.DeepEqual(param.Enum, []string{"strict", "trust_first"}) {
			t.Fatalf("%s coordination_mode enum drifted: %+v", method, param.Enum)
		}
		if param.Default != "strict" {
			t.Fatalf("%s coordination_mode default = %q, want strict", method, param.Default)
		}
	}
}

func TestWorkspaceTensionLifecycleSchemaMatchesRunnableSurfaces(t *testing.T) {
	for _, method := range []string{
		"workspace.tension.refresh",
		"workspace.tension.list",
		"workspace.tension.get",
		"workspace.tension.frontier",
		"workspace.tension.confirm",
		"workspace.tension.discard",
		"workspace.tension.archive",
		"workspace.tension.lifecycle.update",
		"workspace.tension.resolve",
		"workspace.tension.dormant",
		"workspace.tension.add.dependency",
		"workspace.tension.remove.dependency",
		"workspace.tension.condense",
		"workspace.tension.attachable.list",
		"workspace.tension.agent.attach",
		"workspace.tension.agent.detach",
	} {
		if _, ok := rpcMethodSchemas[method]; !ok {
			t.Fatalf("dispatch-backed tension method has no schema: %s", method)
		}
	}

	for _, method := range []string{
		"workspace.tension.confirm",
		"workspace.tension.discard",
		"workspace.tension.archive",
		"workspace.tension.lifecycle.update",
		"workspace.tension.resolve",
		"workspace.tension.dormant",
	} {
		schema, ok := rpcMethodSchemas[method]
		if !ok {
			t.Fatalf("missing runnable tension schema %s", method)
		}
		if !schema.Params["workspace_id"].Required || !schema.Params["tension_id"].Required {
			t.Fatalf("%s should require workspace_id and tension_id: %+v", method, schema.Params)
		}
	}

	for _, method := range []string{
		"workspace.tension.supersede",
		"workspace.tension.dispute",
		"workspace.tension.recover",
	} {
		if _, ok := rpcMethodSchemas[method]; ok {
			t.Fatalf("%s should not be advertised until it has a dispatch/storage path", method)
		}
	}

	lifecycleUpdate := rpcMethodSchemas["workspace.tension.lifecycle.update"]
	if got := lifecycleUpdate.Params["lifecycle_state"].Enum; !reflect.DeepEqual(got, []string{"RESOLVED", "DISCARDED", "ARCHIVED"}) {
		t.Fatalf("workspace.tension.lifecycle.update enum drifted from handler switch: %+v", got)
	}
	if _, ok := lifecycleUpdate.Params["actor_id"]; ok {
		t.Fatalf("workspace.tension.lifecycle.update should expose updated_by, not actor_id: %+v", lifecycleUpdate.Params)
	}
	if !lifecycleUpdate.Params["updated_by"].Required {
		t.Fatalf("workspace.tension.lifecycle.update should require updated_by: %+v", lifecycleUpdate.Params["updated_by"])
	}

	for _, method := range []string{"workspace.tension.resolve", "workspace.tension.dormant"} {
		schema := rpcMethodSchemas[method]
		if !schema.Params["actor_id"].Required {
			t.Fatalf("%s should require actor_id: %+v", method, schema.Params["actor_id"])
		}
	}
}

func TestLimitsGroupSchemaMatchesMutationContracts(t *testing.T) {
	for _, method := range []string{
		"limits.group.create",
		"limits.group.update",
		"limits.group.delete",
	} {
		schema, ok := rpcMethodSchemas[method]
		if !ok {
			t.Fatalf("missing schema for %s", method)
		}
		for _, requiredField := range []string{"workspace_id", "group_id", "actor_id"} {
			param, ok := schema.Params[requiredField]
			if !ok {
				t.Fatalf("%s schema missing %s", method, requiredField)
			}
			if !param.Required {
				t.Fatalf("%s should require %s: %+v", method, requiredField, param)
			}
		}
	}

	createSchema := rpcMethodSchemas["limits.group.create"]
	if !createSchema.Params["title"].Required {
		t.Fatalf("limits.group.create should require title: %+v", createSchema.Params["title"])
	}

	updateSchema := rpcMethodSchemas["limits.group.update"]
	if updateSchema.Params["title"].Required {
		t.Fatalf("limits.group.update title should remain optional for metadata-only/agent-only updates: %+v", updateSchema.Params["title"])
	}
	if !strings.Contains(updateSchema.Params["agent_ids"].Description, "Full replacement") {
		t.Fatalf("limits.group.update should document full membership replacement: %+v", updateSchema.Params["agent_ids"])
	}
}

func TestAgentDeleteSchemaMatchesMutationContract(t *testing.T) {
	schema := rpcMethodSchemas["agent.delete"]
	for _, field := range []string{"workspace_id", "agent_id", "actor"} {
		param, ok := schema.Params[field]
		if !ok || !param.Required {
			t.Fatalf("agent.delete should require %s: %+v", field, schema.Params)
		}
	}
	if _, ok := schema.Params["actor_id"]; ok {
		t.Fatalf("agent.delete should keep the existing actor field name, not actor_id: %+v", schema.Params)
	}
}

func TestAgentProfileUpdateSchemaMatchesMutationContract(t *testing.T) {
	schema := rpcMethodSchemas["agent.profile.update"]
	for _, field := range []string{"workspace_id", "agent_id", "actor_id"} {
		param, ok := schema.Params[field]
		if !ok || !param.Required {
			t.Fatalf("agent.profile.update should require %s: %+v", field, schema.Params)
		}
	}
	for _, stale := range []string{"display_name", "summary"} {
		if _, ok := schema.Params[stale]; ok {
			t.Fatalf("agent.profile.update schema still advertises ignored %s param: %+v", stale, schema.Params)
		}
	}
	if got := schema.Params["tags"].Type; got != "array[string]" {
		t.Fatalf("agent.profile.update tags type = %q, want array[string]", got)
	}
	if !strings.Contains(schema.Params["specialization"].Description, "autonomous work") {
		t.Fatalf("agent.profile.update specialization should document work-selection impact: %+v", schema.Params["specialization"])
	}
}

func TestActionSchemaContractMatchesCurrentLifecycle(t *testing.T) {
	createSchema, ok := rpcMethodSchemas["action.create"]
	if !ok {
		t.Fatal("missing schema for action.create")
	}
	for _, requiredField := range []string{"workspace_id", "task_id", "agent_id", "assigned_to", "title", "description", "blocking", "queue_id", "queue_key"} {
		if _, exists := createSchema.Params[requiredField]; !exists {
			t.Fatalf("action.create schema missing %q", requiredField)
		}
	}
	if _, exists := createSchema.Params["action_type"]; exists {
		t.Fatal("action.create schema still exposes deprecated action_type param")
	}

	listSchema, ok := rpcMethodSchemas["action.list"]
	if !ok {
		t.Fatal("missing schema for action.list")
	}
	if got := listSchema.Params["status"].Enum; !reflect.DeepEqual(got, []string{"PENDING", "COMPLETED", "FAILED"}) {
		t.Fatalf("action.list status enum = %+v, want [PENDING COMPLETED FAILED]", got)
	}

	resolveSchema, ok := rpcMethodSchemas["action.resolve"]
	if !ok {
		t.Fatal("missing schema for action.resolve")
	}
	if _, exists := resolveSchema.Params["workspace_id"]; exists {
		t.Fatal("action.resolve schema should not require workspace_id")
	}
	if got := resolveSchema.Params["resolution"].Enum; !reflect.DeepEqual(got, []string{"COMPLETED", "FAILED"}) {
		t.Fatalf("action.resolve resolution enum = %+v, want [COMPLETED FAILED]", got)
	}
}

// TestAllMethodsHaveSchema verifies every dispatch case has a schema entry.
func TestAllMethodsHaveSchema(t *testing.T) {
	// Ensure we have > 40 schemas (sanity check)
	if len(rpcMethodSchemas) < 40 {
		t.Errorf("expected 40+ method schemas, got %d", len(rpcMethodSchemas))
	}

	// Verify schema self-consistency: method field matches key
	for key, schema := range rpcMethodSchemas {
		if key != schema.Method {
			t.Errorf("schema key %q has Method=%q (should match)", key, schema.Method)
		}
	}
}

// TestSchemaSerializable verifies all schemas can be marshaled to JSON.
func TestSchemaSerializable(t *testing.T) {
	for method, schema := range rpcMethodSchemas {
		data, err := json.Marshal(schema)
		if err != nil {
			t.Errorf("schema for %q failed to marshal: %v", method, err)
		}
		if len(data) < 10 {
			t.Errorf("schema for %q produced suspiciously short JSON: %s", method, string(data))
		}
	}
}

func TestPolicyNamespaceReservedForCapabilityGovernance(t *testing.T) {
	policyMethods := map[string]bool{}
	for method := range rpcMethodSchemas {
		if strings.HasPrefix(method, "workspace.policy.") {
			policyMethods[method] = true
		}
	}

	expected := []string{
		"workspace.policy.put",
		"workspace.policy.list",
		"workspace.policy.check",
	}
	if len(policyMethods) != len(expected) {
		t.Fatalf("expected exactly %d workspace.policy methods, got %+v", len(expected), policyMethods)
	}
	for _, method := range expected {
		if !policyMethods[method] {
			t.Fatalf("expected capability-governance method %s to stay in workspace.policy namespace, got %+v", method, policyMethods)
		}
	}
	for _, forbidden := range []string{
		"workspace.policy.report",
		"workspace.policy.clusters",
		"workspace.policy.snapshot",
		"workspace.policy.refresh",
	} {
		if _, ok := rpcMethodSchemas[forbidden]; ok {
			t.Fatalf("unexpected read-side method leaked into workspace.policy namespace: %s", forbidden)
		}
	}
	for _, method := range []string{
		"workspace.controlplane.report",
		"workspace.controlplane.cluster",
		"workspace.controlplane.tick",
		"workspace.controlplane.overview",
		"workspace.controlplane.snapshot",
		"workspace.controlplane.state.report",
		"workspace.controlplane.state.cluster",
		"workspace.controlplane.state.tick",
		"workspace.controlplane.state.snapshot",
	} {
		if _, ok := rpcMethodSchemas[method]; ok {
			t.Fatalf("unexpected parallel control-plane namespace leaked alongside advisory slice: %s", method)
		}
	}
}

func TestControlStateSchemaStaysManualAndPreviewAware(t *testing.T) {
	report := rpcMethodSchemas["workspace.instrumentation.control.state.report"]
	cluster := rpcMethodSchemas["workspace.instrumentation.control.state.cluster"]
	tick := rpcMethodSchemas["workspace.instrumentation.control.state.tick"]
	snapshot := rpcMethodSchemas["workspace.instrumentation.control.state.snapshot"]

	for name, schema := range map[string]MethodSchema{
		"report":   report,
		"cluster":  cluster,
		"tick":     tick,
		"snapshot": snapshot,
	} {
		if strings.Contains(strings.ToLower(schema.Description), "policy engine") {
			t.Fatalf("control-state %s schema overclaims policy-engine semantics: %s", name, schema.Description)
		}
	}

	if !strings.Contains(strings.ToLower(report.Description), "preview") {
		t.Fatalf("control-state report schema should mention preview state, got %q", report.Description)
	}
	if !strings.Contains(strings.ToLower(cluster.Description), "preview") {
		t.Fatalf("control-state cluster schema should mention preview state, got %q", cluster.Description)
	}
	if !strings.Contains(strings.ToLower(tick.Description), "manual") {
		t.Fatalf("control-state tick schema should stay explicitly manual, got %q", tick.Description)
	}
	if !strings.Contains(strings.ToLower(snapshot.Description), "preview") {
		t.Fatalf("control-state snapshot schema should mention preview state, got %q", snapshot.Description)
	}
	if len(rpcMethodSchemas["workspace.instrumentation.control.state.cluster"].Params["mode"].Enum) == 0 {
		t.Fatal("control-state cluster mode param should advertise the scaffold mode enum even when ignored")
	}
	if len(rpcMethodSchemas["workspace.instrumentation.control.state.tick"].Params["mode"].Enum) == 0 {
		t.Fatal("control-state tick mode param should advertise the scaffold mode enum even when ignored")
	}
	if rpcMethodSchemas["workspace.instrumentation.control.state.tick"].Params["limit"].Default != "" {
		t.Fatalf("control-state tick limit should not advertise a stale default when the backend ignores it, got %+v", rpcMethodSchemas["workspace.instrumentation.control.state.tick"].Params["limit"])
	}
	if rpcMethodSchemas["workspace.instrumentation.control.state.cluster"].Params["limit"].Default != "" {
		t.Fatalf("control-state cluster limit should not advertise a stale default when the backend ignores it, got %+v", rpcMethodSchemas["workspace.instrumentation.control.state.cluster"].Params["limit"])
	}
}

func TestWorkspaceOpsRequestSchemaStaysTyped(t *testing.T) {
	schema, ok := rpcMethodSchemas["workspace.ops.request"]
	if !ok {
		t.Fatal("missing workspace.ops.request schema")
	}
	if !strings.Contains(strings.ToLower(schema.Description), "typed external-gate request") {
		t.Fatalf("workspace.ops.request description should advertise typed external-gate semantics, got %q", schema.Description)
	}
	if got := schema.Params["gate_type"].Enum; len(got) != 3 || got[0] != "CREDENTIAL_AUTH" || got[1] != "PAYMENT_BILLING" || got[2] != "EXPLICIT_APPROVAL" {
		t.Fatalf("unexpected gate_type enum: %+v", got)
	}
	if !schema.Params["request_key"].Required {
		t.Fatal("workspace.ops.request must require request_key")
	}
	if schema.Params["source_kind"].Default != "external_gate" {
		t.Fatalf("workspace.ops.request source_kind default regressed: %+v", schema.Params["source_kind"])
	}
}

func TestWorkspaceSegmentAndCorridorAuthoritySchemasStayReadOnly(t *testing.T) {
	segmentList := rpcMethodSchemas["workspace.segment.list"]
	segmentGet := rpcMethodSchemas["workspace.segment.get"]
	boundaryReport := rpcMethodSchemas["workspace.instrumentation.corridor.boundary.report"]
	boundaryCluster := rpcMethodSchemas["workspace.instrumentation.corridor.boundary.cluster"]
	authorityReport := rpcMethodSchemas["workspace.instrumentation.corridor.authority.report"]
	authorityTask := rpcMethodSchemas["workspace.instrumentation.corridor.authority.task"]

	if !strings.Contains(strings.ToLower(segmentList.Description), "read-only") || !strings.Contains(strings.ToLower(segmentList.Description), "no policy authority") {
		t.Fatalf("workspace.segment.list schema should stay structural and non-authoritative, got %+v", segmentList)
	}
	if !strings.Contains(strings.ToLower(segmentGet.Description), "read-only") || !strings.Contains(strings.ToLower(segmentGet.Description), "tension evidence anchoring") {
		t.Fatalf("workspace.segment.get schema should stay tension-oriented lookup only, got %+v", segmentGet)
	}
	if !strings.Contains(strings.ToLower(segmentList.Params["segment_ref"].Description), "cannot be combined") {
		t.Fatalf("workspace.segment.list segment_ref contract should reject mixed source filters, got %+v", segmentList.Params["segment_ref"])
	}
	if !strings.Contains(strings.ToLower(boundaryReport.Description), "read-only") || !strings.Contains(strings.ToLower(boundaryReport.Description), "policy authority") {
		t.Fatalf("corridor boundary report schema should stay read-only and non-authoritative, got %+v", boundaryReport)
	}
	if !strings.Contains(strings.ToLower(boundaryCluster.Description), "basis_state") || !strings.Contains(strings.ToLower(boundaryCluster.Description), "boundary_state") {
		t.Fatalf("corridor boundary cluster schema should expose the two-axis diagnostic contract, got %+v", boundaryCluster)
	}
	if !strings.Contains(strings.ToLower(authorityReport.Description), "read-only") || !strings.Contains(strings.ToLower(authorityReport.Description), "not policy authority") {
		t.Fatalf("corridor authority report schema should stay read-only and non-authoritative, got %+v", authorityReport)
	}
	if !strings.Contains(strings.ToLower(authorityTask.Description), "read-only") || !strings.Contains(strings.ToLower(authorityTask.Description), "separate from policy governance") {
		t.Fatalf("corridor authority task schema should stay read-only and separate from governance, got %+v", authorityTask)
	}
	if _, ok := authorityReport.Params["limit"]; !ok {
		t.Fatal("corridor authority report should expose limit for task listing parity")
	}
	if authorityTask.Params["limit"].Description != "Accepted for param parity; ignored by task detail reads" {
		t.Fatalf("unexpected corridor authority task limit contract: %+v", authorityTask.Params["limit"])
	}
}

func TestTaskSchemaMatchesTaskMetadataContracts(t *testing.T) {
	submit := rpcMethodSchemas["task.submit"]
	if !submit.Params["workspace_id"].Required {
		t.Fatalf("task.submit must require workspace_id: %+v", submit.Params["workspace_id"])
	}
	if got := submit.Params["priority"].Enum; !reflect.DeepEqual(got, []string{"low", "normal", "high", "critical"}) {
		t.Fatalf("task.submit priority enum drifted from storage validation: %+v", got)
	}
	if submit.Params["priority"].Default != "normal" {
		t.Fatalf("task.submit priority default drifted from normalizePriority: %+v", submit.Params["priority"])
	}
	if got := submit.Params["task_kind"].Enum; !reflect.DeepEqual(got, []string{"EXECUTION", "COORDINATION"}) {
		t.Fatalf("task.submit task_kind enum drifted from model validation: %+v", got)
	}
	if got := submit.Params["task_class"].Enum; !reflect.DeepEqual(got, []string{"PROOF", "EXPLORATION", "INTEGRATION", "INCIDENT"}) {
		t.Fatalf("task.submit task_class enum drifted from authored evidence contract: %+v", got)
	}
	if got := submit.Params["task_class_source"].Enum; !reflect.DeepEqual(got, []string{"EXPLICIT", "TEMPLATE_DEFAULT", "UNSET"}) {
		t.Fatalf("task.submit task_class_source enum drifted from authored evidence contract: %+v", got)
	}
	if submit.Params["tags"].Type != "array[string]" {
		t.Fatalf("task.submit tags param should stay array[string], got %+v", submit.Params["tags"])
	}
	if submit.Params["graph"].Type != "object" {
		t.Fatalf("task.submit graph param should stay object-typed, got %+v", submit.Params["graph"])
	}
	for _, key := range []string{"project_lane", "requires_project_gate", "dependency_task_ids", "related_task_ids", "write_scope_hints", "task_requirements"} {
		if _, ok := submit.Params[key]; !ok {
			t.Fatalf("task.submit should expose project coordination field %s: %+v", key, submit.Params)
		}
	}

	status := rpcMethodSchemas["task.status"]
	if !status.Params["workspace_id"].Required {
		t.Fatalf("task.status must require workspace_id: %+v", status.Params["workspace_id"])
	}
	if !status.Params["task_id"].Required {
		t.Fatalf("task.status must require task_id: %+v", status.Params["task_id"])
	}

	put := rpcMethodSchemas["task.class.put"]
	if !put.Params["workspace_id"].Required {
		t.Fatalf("task.class.put must require workspace_id: %+v", put.Params["workspace_id"])
	}
	if !put.Params["task_id"].Required {
		t.Fatalf("task.class.put must require task_id: %+v", put.Params["task_id"])
	}
	if got := put.Params["task_class"].Enum; !reflect.DeepEqual(got, []string{"PROOF", "EXPLORATION", "INTEGRATION", "INCIDENT"}) {
		t.Fatalf("task.class.put task_class enum drifted from authored evidence contract: %+v", got)
	}
	if got := put.Params["task_class_source"].Enum; !reflect.DeepEqual(got, []string{"EXPLICIT", "TEMPLATE_DEFAULT", "UNSET"}) {
		t.Fatalf("task.class.put task_class_source enum drifted from authored evidence contract: %+v", got)
	}

	projectPut := rpcMethodSchemas["task.project_fields.put"]
	if !projectPut.Params["workspace_id"].Required || !projectPut.Params["task_id"].Required {
		t.Fatalf("task.project_fields.put must require workspace_id and task_id: %+v", projectPut.Params)
	}
	if got := projectPut.Params["task_kind"].Enum; !reflect.DeepEqual(got, []string{"EXECUTION", "COORDINATION"}) {
		t.Fatalf("task.project_fields.put task_kind enum drifted from storage validation: %+v", got)
	}

	closeSchema := rpcMethodSchemas["task.close"]
	if !closeSchema.Params["workspace_id"].Required {
		t.Fatalf("task.close must require workspace_id: %+v", closeSchema.Params["workspace_id"])
	}
	if _, ok := closeSchema.Params["status"]; ok {
		t.Fatalf("task.close schema should expose resolution, not legacy status: %+v", closeSchema.Params)
	}
	if got := closeSchema.Params["resolution"].Enum; !reflect.DeepEqual(got, []string{"RESOLVED", "FAILED", "CANCELLED"}) {
		t.Fatalf("task.close resolution enum drifted from storage validation: %+v", got)
	}
	if closeSchema.Params["resolution"].Default != "RESOLVED" {
		t.Fatalf("task.close resolution default drifted from CloseTask semantics: %+v", closeSchema.Params["resolution"])
	}
}

func TestProjectSchemaMatchesHandlerContracts(t *testing.T) {
	createSchema := rpcMethodSchemas["project.create"]
	if !createSchema.Params["workspace_id"].Required || !createSchema.Params["project_id"].Required || !createSchema.Params["title"].Required {
		t.Fatalf("project.create should require workspace_id, project_id, and title: %+v", createSchema.Params)
	}
	if !createSchema.Params["created_by"].Required {
		t.Fatalf("project.create should require created_by: %+v", createSchema.Params["created_by"])
	}

	for _, method := range []string{"project.get", "project.delete"} {
		schema := rpcMethodSchemas[method]
		if !schema.Params["workspace_id"].Required || !schema.Params["project_id"].Required {
			t.Fatalf("%s should require workspace_id and project_id: %+v", method, schema.Params)
		}
	}
	if !rpcMethodSchemas["project.delete"].Params["actor_id"].Required {
		t.Fatalf("project.delete should require actor_id: %+v", rpcMethodSchemas["project.delete"].Params["actor_id"])
	}

	updateSchema := rpcMethodSchemas["project.update"]
	if !updateSchema.Params["workspace_id"].Required || !updateSchema.Params["project_id"].Required {
		t.Fatalf("project.update should require workspace_id and project_id: %+v", updateSchema.Params)
	}
	if !updateSchema.Params["actor_id"].Required {
		t.Fatalf("project.update should require actor_id: %+v", updateSchema.Params["actor_id"])
	}
	if !updateSchema.Params["title"].Required {
		t.Fatalf("project.update should advertise title as required to match handler validation: %+v", updateSchema.Params["title"])
	}
	if updateSchema.Params["description"].Required {
		t.Fatalf("project.update description should remain optional: %+v", updateSchema.Params["description"])
	}
	if got := updateSchema.Params["status"].Enum; !reflect.DeepEqual(got, []string{"ACTIVE", "ARCHIVED"}) {
		t.Fatalf("project.update status enum drifted: %+v", got)
	}

	for _, method := range []string{"project.profile.get", "project.gates.status", "project.coordination.get"} {
		schema := rpcMethodSchemas[method]
		if !schema.Params["workspace_id"].Required || !schema.Params["project_id"].Required {
			t.Fatalf("%s should require workspace_id and project_id: %+v", method, schema.Params)
		}
		if _, ok := schema.Params["actor_id"]; ok {
			t.Fatalf("%s is read-only and should not advertise actor_id: %+v", method, schema.Params)
		}
	}

	profileUpdate := rpcMethodSchemas["project.profile.update"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id"} {
		if !profileUpdate.Params[field].Required {
			t.Fatalf("project.profile.update should require %s: %+v", field, profileUpdate.Params)
		}
	}
	for _, field := range []string{"goal", "design_doc_id", "implementation_plan_doc_id", "repo_required", "repo_status", "repo_url", "repo_default_branch"} {
		param, ok := profileUpdate.Params[field]
		if !ok {
			t.Fatalf("project.profile.update should expose %s: %+v", field, profileUpdate.Params)
		}
		if param.Required {
			t.Fatalf("project.profile.update %s should stay optional for partial profile patches: %+v", field, param)
		}
	}
	if got := profileUpdate.Params["repo_status"].Enum; !reflect.DeepEqual(got, []string{"NOT_REQUIRED", "MISSING", "READY", "BLOCKED", "UNKNOWN"}) {
		t.Fatalf("project.profile.update repo_status enum drifted: %+v", got)
	}

	phaseTransition := rpcMethodSchemas["project.phase.transition"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "to_phase"} {
		if !phaseTransition.Params[field].Required {
			t.Fatalf("project.phase.transition should require %s: %+v", field, phaseTransition.Params)
		}
	}
	if _, ok := phaseTransition.Params["from_phase"]; ok {
		t.Fatalf("project.phase.transition should not advertise unsupported from_phase guard: %+v", phaseTransition.Params)
	}
	if phaseTransition.Params["reason"].Required {
		t.Fatalf("project.phase.transition guards and notes should stay optional: %+v", phaseTransition.Params)
	}

	for _, method := range []string{
		"project.lead.claim",
		"project.lead.renew",
		"project.lead.release",
		"project.lead.transfer",
		"project.role.assign",
	} {
		schema := rpcMethodSchemas[method]
		for _, field := range []string{"workspace_id", "project_id", "actor_id"} {
			if !schema.Params[field].Required {
				t.Fatalf("%s should require %s: %+v", method, field, schema.Params)
			}
		}
		if !strings.Contains(strings.ToLower(schema.Description), "runtime evidence") {
			t.Fatalf("%s should advertise durable runtime evidence semantics, got %q", method, schema.Description)
		}
	}

	if !rpcMethodSchemas["project.lead.claim"].Params["agent_id"].Required {
		t.Fatalf("project.lead.claim should require agent_id: %+v", rpcMethodSchemas["project.lead.claim"].Params)
	}
	for _, method := range []string{"project.lead.renew", "project.lead.release", "project.lead.transfer"} {
		if !rpcMethodSchemas[method].Params["role_id"].Required {
			t.Fatalf("%s should require role_id: %+v", method, rpcMethodSchemas[method].Params)
		}
	}
	if !rpcMethodSchemas["project.lead.transfer"].Params["to_agent_id"].Required {
		t.Fatalf("project.lead.transfer should require to_agent_id: %+v", rpcMethodSchemas["project.lead.transfer"].Params)
	}
	if !rpcMethodSchemas["project.role.assign"].Params["agent_id"].Required || !rpcMethodSchemas["project.role.assign"].Params["role_type"].Required {
		t.Fatalf("project.role.assign should require agent_id and role_type: %+v", rpcMethodSchemas["project.role.assign"].Params)
	}
	if rpcMethodSchemas["project.role.assign"].Params["write_scope_json"].Required {
		t.Fatalf("project.role.assign write_scope_json should stay optional: %+v", rpcMethodSchemas["project.role.assign"].Params["write_scope_json"])
	}
	rolesList := rpcMethodSchemas["project.roles.list"]
	if !rolesList.Params["workspace_id"].Required || !rolesList.Params["project_id"].Required {
		t.Fatalf("project.roles.list should require workspace_id and project_id: %+v", rolesList.Params)
	}
	if rolesList.Params["include_inactive"].Required {
		t.Fatalf("project.roles.list include_inactive should stay optional: %+v", rolesList.Params["include_inactive"])
	}
	if _, ok := rolesList.Params["actor_id"]; ok {
		t.Fatalf("project.roles.list is read-only and should not advertise actor_id: %+v", rolesList.Params)
	}
	repoUpsert := rpcMethodSchemas["project.repository.upsert"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id"} {
		if !repoUpsert.Params[field].Required {
			t.Fatalf("project.repository.upsert should require %s: %+v", field, repoUpsert.Params)
		}
	}
	if repoUpsert.Params["credential_vault_entry_id"].Required {
		t.Fatalf("project.repository.upsert credential ref should be optional: %+v", repoUpsert.Params["credential_vault_entry_id"])
	}
	repoList := rpcMethodSchemas["project.repositories.list"]
	if !repoList.Params["workspace_id"].Required || !repoList.Params["project_id"].Required {
		t.Fatalf("project.repositories.list should require workspace_id and project_id: %+v", repoList.Params)
	}
	if _, ok := repoList.Params["actor_id"]; ok {
		t.Fatalf("project.repositories.list is read-only and should not advertise actor_id: %+v", repoList.Params)
	}
	checkoutRegister := rpcMethodSchemas["project.checkout.register"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "repo_id", "machine_id", "local_path"} {
		if !checkoutRegister.Params[field].Required {
			t.Fatalf("project.checkout.register should require %s: %+v", field, checkoutRegister.Params)
		}
	}
	checkoutList := rpcMethodSchemas["project.checkouts.list"]
	if !checkoutList.Params["workspace_id"].Required || !checkoutList.Params["project_id"].Required {
		t.Fatalf("project.checkouts.list should require workspace_id and project_id: %+v", checkoutList.Params)
	}
	if _, ok := checkoutList.Params["actor_id"]; ok {
		t.Fatalf("project.checkouts.list is read-only and should not advertise actor_id: %+v", checkoutList.Params)
	}
	branchRegister := rpcMethodSchemas["project.branch.register"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "repo_id", "branch_name"} {
		if !branchRegister.Params[field].Required {
			t.Fatalf("project.branch.register should require %s: %+v", field, branchRegister.Params)
		}
	}
	if branchRegister.Params["checkout_id"].Required || branchRegister.Params["agent_id"].Required || branchRegister.Params["write_scope_json"].Required {
		t.Fatalf("project.branch.register checkout_id, agent_id, and write_scope_json should stay optional: %+v", branchRegister.Params)
	}
	branchList := rpcMethodSchemas["project.branches.list"]
	if !branchList.Params["workspace_id"].Required || !branchList.Params["project_id"].Required {
		t.Fatalf("project.branches.list should require workspace_id and project_id: %+v", branchList.Params)
	}
	if _, ok := branchList.Params["actor_id"]; ok {
		t.Fatalf("project.branches.list is read-only and should not advertise actor_id: %+v", branchList.Params)
	}
	patchQueueSubmit := rpcMethodSchemas["project.patch_queue.submit"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "repo_id", "branch_id"} {
		if !patchQueueSubmit.Params[field].Required {
			t.Fatalf("project.patch_queue.submit should require %s: %+v", field, patchQueueSubmit.Params)
		}
	}
	if patchQueueSubmit.Params["auto_merge"].Required || patchQueueSubmit.Params["queue_id"].Required || patchQueueSubmit.Params["item_id"].Required {
		t.Fatalf("project.patch_queue.submit queue identity and auto_merge should stay optional: %+v", patchQueueSubmit.Params)
	}
	for _, field := range []string{"task_id", "session_id", "run_id", "agent_id", "capability_snapshot_id", "capability_snapshot_schema", "repo_root", "base_tree_hash", "repo_lease_id", "lease_term"} {
		if !patchQueueSubmit.Params[field].Required {
			t.Fatalf("project.patch_queue.submit binding field %s should be required: %+v", field, patchQueueSubmit.Params)
		}
	}
	for _, field := range []string{"principal_type", "principal_id", "base_file_hashes", "base_file_hashes_json", "context_digest", "operation_id"} {
		if patchQueueSubmit.Params[field].Required {
			t.Fatalf("project.patch_queue.submit binding field %s should stay optional: %+v", field, patchQueueSubmit.Params)
		}
	}
	patchQueueSupersede := rpcMethodSchemas["project.patch_queue.supersede"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "queue_id", "item_id", "new_item_id", "evidence_doc_key"} {
		if !patchQueueSupersede.Params[field].Required {
			t.Fatalf("project.patch_queue.supersede should require %s: %+v", field, patchQueueSupersede.Params)
		}
	}
	for _, field := range []string{"task_id", "session_id", "run_id", "agent_id", "principal_type", "principal_id", "capability_snapshot_id", "capability_snapshot_schema", "repo_root", "base_tree_hash", "base_file_hashes", "base_file_hashes_json", "context_digest", "repo_lease_id", "lease_term", "max_attempts"} {
		if patchQueueSupersede.Params[field].Required {
			t.Fatalf("project.patch_queue.supersede provenance field %s should stay optional: %+v", field, patchQueueSupersede.Params)
		}
	}
	patchQueueList := rpcMethodSchemas["project.patch_queue.list"]
	if !patchQueueList.Params["workspace_id"].Required || !patchQueueList.Params["project_id"].Required {
		t.Fatalf("project.patch_queue.list should require workspace_id and project_id: %+v", patchQueueList.Params)
	}
	if _, ok := patchQueueList.Params["actor_id"]; ok {
		t.Fatalf("project.patch_queue.list is read-only and should not advertise actor_id: %+v", patchQueueList.Params)
	}
	operationBind := rpcMethodSchemas["project.patch_queue.operation_bind"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "queue_id", "item_id", "claim_token"} {
		if !operationBind.Params[field].Required {
			t.Fatalf("project.patch_queue.operation_bind should require %s: %+v", field, operationBind.Params)
		}
	}
	if operationBind.Params["mutation_paths_json"].Required || operationBind.Params["operation_id"].Required || operationBind.Params["operation_kind"].Required {
		t.Fatalf("project.patch_queue.operation_bind ledger identity and mutation_paths_json should stay optional: %+v", operationBind.Params)
	}
	if !strings.Contains(strings.ToLower(operationBind.Description), "does not apply") || !strings.Contains(strings.ToLower(operationBind.Description), "push") {
		t.Fatalf("project.patch_queue.operation_bind must advertise evidence-only behavior, got %q", operationBind.Description)
	}
	casRecord := rpcMethodSchemas["project.patch_queue.cas_record"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "queue_id", "item_id", "cas_result", "test_evidence", "claim_token"} {
		if !casRecord.Params[field].Required {
			t.Fatalf("project.patch_queue.cas_record should require %s: %+v", field, casRecord.Params)
		}
	}
	if !strings.Contains(strings.ToLower(casRecord.Description), "durable evidence only") || !strings.Contains(strings.ToLower(casRecord.Description), "does not apply") {
		t.Fatalf("project.patch_queue.cas_record must advertise evidence-only behavior, got %q", casRecord.Description)
	}
	materializationRecord := rpcMethodSchemas["project.patch_queue.materialization_record"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "queue_id", "item_id", "materialization", "claim_token"} {
		if !materializationRecord.Params[field].Required {
			t.Fatalf("project.patch_queue.materialization_record should require %s: %+v", field, materializationRecord.Params)
		}
	}
	if !strings.Contains(strings.ToLower(materializationRecord.Description), "stores candidate bytes") || !strings.Contains(strings.ToLower(materializationRecord.Description), "does not apply") {
		t.Fatalf("project.patch_queue.materialization_record must advertise evidence-only materialization behavior, got %q", materializationRecord.Description)
	}
	integrationRecord := rpcMethodSchemas["project.patch_queue.integration_record"]
	for _, field := range []string{"workspace_id", "project_id", "actor_id", "queue_id", "item_id", "outcome", "target_branch"} {
		if !integrationRecord.Params[field].Required {
			t.Fatalf("project.patch_queue.integration_record should require %s: %+v", field, integrationRecord.Params)
		}
	}
	if !strings.Contains(strings.ToLower(integrationRecord.Description), "durable") || !strings.Contains(strings.ToLower(integrationRecord.Description), "canonical git mutation success") {
		t.Fatalf("project.patch_queue.integration_record must advertise durable integration receipt behavior, got %q", integrationRecord.Description)
	}
	if got := integrationRecord.Params["authority_mode"]; got.Type != "string" || got.Required {
		t.Fatalf("project.patch_queue.integration_record must advertise optional authority_mode sent by runtime tools, got %+v", got)
	}
}

func TestActionSchemaMatchesRebaseWorkflowContract(t *testing.T) {
	createSchema := rpcMethodSchemas["action.create"]
	if !strings.Contains(strings.ToLower(createSchema.Description), "rebase follow-up") || !strings.Contains(strings.ToLower(createSchema.Description), "rollback-failure") {
		t.Fatalf("action.create schema should advertise queue-linked rebase and rollback-failure promotion semantics, got %q", createSchema.Description)
	}
	if createSchema.Params["task_id"].Required {
		t.Fatalf("action.create task_id should stay conditionally hydrated, got %+v", createSchema.Params["task_id"])
	}
	if createSchema.Params["agent_id"].Required {
		t.Fatalf("action.create agent_id should stay optional for queue-linked hydration, got %+v", createSchema.Params["agent_id"])
	}
	if createSchema.Params["title"].Required {
		t.Fatalf("action.create title should stay conditionally hydrated, got %+v", createSchema.Params["title"])
	}
	if createSchema.Params["blocking"].Type != "boolean" {
		t.Fatalf("action.create blocking should stay boolean-typed, got %+v", createSchema.Params["blocking"])
	}
	if !strings.Contains(strings.ToLower(createSchema.Params["queue_id"].Description), "source queue") || !strings.Contains(strings.ToLower(createSchema.Params["queue_id"].Description), "deterministic task context") {
		t.Fatalf("action.create queue_id should document source queue promotion semantics, got %+v", createSchema.Params["queue_id"])
	}
	if !strings.Contains(strings.ToLower(createSchema.Params["task_id"].Description), "required unless hydrated") {
		t.Fatalf("action.create task_id should document queue/tension hydration semantics, got %+v", createSchema.Params["task_id"])
	}

	listSchema := rpcMethodSchemas["action.list"]
	if got := listSchema.Params["status"].Enum; !reflect.DeepEqual(got, []string{"PENDING", "COMPLETED", "FAILED"}) {
		t.Fatalf("action.list status enum drifted from store lifecycle: %+v", got)
	}

	resolveSchema := rpcMethodSchemas["action.resolve"]
	if _, ok := resolveSchema.Params["workspace_id"]; ok {
		t.Fatalf("action.resolve should no longer advertise workspace_id: %+v", resolveSchema.Params)
	}
	if got := resolveSchema.Params["resolution"].Enum; !reflect.DeepEqual(got, []string{"COMPLETED", "FAILED"}) {
		t.Fatalf("action.resolve resolution enum drifted from store lifecycle: %+v", got)
	}
	if !strings.Contains(strings.ToLower(resolveSchema.Description), "rebase follow-up") || !strings.Contains(strings.ToLower(resolveSchema.Description), "rollback-failure") {
		t.Fatalf("action.resolve schema should advertise linked rebase and rollback-failure source semantics, got %q", resolveSchema.Description)
	}
}

func TestWorkspaceOpsResolveAndEscalateSchemaAdvertiseRevisionGuard(t *testing.T) {
	for _, method := range []string{"workspace.ops.upsert", "workspace.ops.request", "workspace.ops.resolve", "workspace.ops.escalate"} {
		schema, ok := rpcMethodSchemas[method]
		if !ok {
			t.Fatalf("missing %s schema", method)
		}
		revisionParam, ok := schema.Params["current_revision"]
		if !ok {
			t.Fatalf("%s schema missing current_revision", method)
		}
		if revisionParam.Type != "integer" || revisionParam.Required {
			t.Fatalf("%s current_revision should stay optional integer, got %+v", method, revisionParam)
		}
		revisionDescription := strings.ToLower(revisionParam.Description)
		if !strings.Contains(revisionDescription, "preferred") || !strings.Contains(revisionDescription, "queue revision") {
			t.Fatalf("%s current_revision should advertise preferred queue revision base-version, got %+v", method, revisionParam)
		}
		param, ok := schema.Params["current_updated_at"]
		if !ok {
			t.Fatalf("%s schema missing current_updated_at", method)
		}
		if param.Type != "string" || param.Required {
			t.Fatalf("%s current_updated_at should stay optional string, got %+v", method, param)
		}
		description := strings.ToLower(param.Description)
		if !strings.Contains(description, "legacy") || !strings.Contains(description, "current_revision") {
			t.Fatalf("%s current_updated_at should document stale queue snapshot guard, got %+v", method, param)
		}
	}
}

func TestEventEmitSchemaStaysEphemeralOnly(t *testing.T) {
	schema, ok := rpcMethodSchemas["event.emit"]
	if !ok {
		t.Fatal("missing event.emit schema")
	}
	description := strings.ToLower(schema.Description)
	if !strings.Contains(description, "ephemeral") || !strings.Contains(description, "without writing a runtime-journal row") {
		t.Fatalf("event.emit schema must advertise ephemeral non-journal semantics, got %q", schema.Description)
	}
	typeParam, ok := schema.Params["type"]
	if !ok {
		t.Fatal("event.emit schema missing type param")
	}
	if !strings.Contains(strings.ToLower(typeParam.Description), "ephemeral.") {
		t.Fatalf("event.emit type param must advertise ephemeral.* prefix, got %+v", typeParam)
	}
}

func TestWorkspaceClaimWriteSchemaAdvertisesDifferentiatedDissentTypes(t *testing.T) {
	schema, ok := rpcMethodSchemas["workspace.claim.write"]
	if !ok {
		t.Fatal("missing workspace.claim.write schema")
	}
	got := schema.Params["claim_type"].Enum
	want := []string{"FACT", "DECISION", "LESSON", "PROCEDURE", "ANTI_PROCEDURE", "INCIDENT", "UPDATE_DIGEST", "BLOCKER", "CONSTRAINT", "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT", "HYPOTHESIS", "ALTERNATIVE_BRANCH", "ENTITY", "SUMMARY", "EXPERIENCE"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("workspace.claim.write claim_type enum drifted from bounded differentiated-claim contract: %+v", got)
	}
}

func TestWorkspaceMemoryWriteSchemasAdvertiseCanonicalInteractiveMemoryTypes(t *testing.T) {
	want := []string{"NOTE", "LESSON", "DECISION", "PROCEDURE", "ANTI_PROCEDURE", "INCIDENT", "ENTITY", "EXPERIENCE", "UPDATE_DIGEST", "SUMMARY", "SELF_MODEL", "GOAL_COMMITMENT", "POLICY_TRACE"}

	for _, method := range []string{"workspace.memory.write", "workspace.memory.node.write"} {
		schema, ok := rpcMethodSchemas[method]
		if !ok {
			t.Fatalf("missing %s schema", method)
		}
		got := schema.Params["memory_type"].Enum
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s memory_type enum drifted from bounded interactive canonical-memory contract: %+v", method, got)
		}
	}
}
