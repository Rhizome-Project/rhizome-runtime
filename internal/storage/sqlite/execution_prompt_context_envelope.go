package sqlite

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	executionPromptContextEnvelopeKey      = "prompt_context_envelope"
	executionPromptContextEnvelopeContract = "prompt_context_envelope.v1"
	executionPromptContextKind             = "authority_bearing_execution_write"
	operatorQueuePromptContextKind         = "authority_bearing_operator_queue_write"
	humanActionPromptContextKind           = "authority_bearing_action_write"
	taskPromptContextKind                  = "authority_bearing_task_write"
	sessionPromptContextKind               = "authority_bearing_session_write"
	agentMessagePromptContextKind          = "authority_bearing_agent_message"
	agentRequestPromptContextKind          = "authority_bearing_agent_request"
	workspaceMemoryPromptContextKind       = "authority_bearing_workspace_memory_write"
	workspaceTensionPromptContextKind      = "authority_bearing_workspace_tension_write"
	knowledgeClaimPromptContextKind        = "authority_bearing_knowledge_claim_write"
	nodeLifecyclePromptContextKind         = "authority_bearing_node_lifecycle_write"
	workspaceDocPromptContextKind          = "authority_bearing_workspace_doc_write"
	agentUpdatePromptContextKind           = "authority_bearing_agent_update_write"
	agentLifecyclePromptContextKind        = "authority_bearing_agent_lifecycle_write"
	agentProfilePromptContextKind          = "authority_bearing_agent_profile_write"
	workspaceArtifactPromptContextKind     = "authority_bearing_workspace_artifact_write"
	projectPromptContextKind               = "authority_bearing_project_write"
	serviceVenturePromptContextKind        = "authority_bearing_service_venture_write"
	limitGroupPromptContextKind            = "authority_bearing_limit_group_write"
	vaultPromptContextKind                 = "authority_bearing_vault_write"
	newsPromptContextKind                  = "authority_bearing_news_write"
	toolCallPromptContextKind              = "authority_bearing_tool_call"
	toolRegistryPromptContextKind          = "authority_bearing_tool_registry_write"
	capabilityPolicyPromptContextKind      = "authority_bearing_capability_policy_write"
	controlCommandPromptContextKind        = "authority_bearing_control_command_write"
	controlStatePromptContextKind          = "authority_bearing_control_state_write"
	executionPromptContextCompilerStatus   = "non_daemon_context_envelope"
	executionPromptContextDaemonClaim      = "not_claimed"
	executionPromptContextCapabilityProof  = "not_present"
	executionPromptContextAuthorityModel   = "workspace_authority"
)

var allowedExecutionPromptContextSurfaces = map[string]struct{}{
	"workspace.execution.run.write":                      {},
	"workspace.execution.step.write":                     {},
	"agent.session.execution.run.sync":                   {},
	"agent.session.execution.step.sync":                  {},
	"tool.call":                                          {},
	"tool.deploy":                                        {},
	"tool.undeploy":                                      {},
	"mcp.server.register":                                {},
	"mcp.server.remove":                                  {},
	"mcp.tool.discover":                                  {},
	"action.create":                                      {},
	"action.start":                                       {},
	"action.pause":                                       {},
	"action.resolve":                                     {},
	"action.chat.send":                                   {},
	"agent.session.start":                                {},
	"agent.session.status":                               {},
	"agent.session.blocked":                              {},
	"agent.session.decision_needed":                      {},
	"agent.session.keepalive":                            {},
	"agent.session.end":                                  {},
	"agent.session.takeover":                             {},
	"agent.message.send":                                 {},
	"agent.request":                                      {},
	"agent.request.list":                                 {},
	"agent.respond":                                      {},
	"agent.task.claim":                                   {},
	"agent.task_frontier.decision":                       {},
	"agent.task.release":                                 {},
	"agent.task.complete":                                {},
	"agent.task.block":                                   {},
	"agent.tension.attach":                               {},
	"agent.tension.detach":                               {},
	"coalition.offer":                                    {},
	"coalition.leave":                                    {},
	"coalition.invite":                                   {},
	"coalition.kick":                                     {},
	"agent.node.claim":                                   {},
	"agent.node.release":                                 {},
	"agent.node.complete":                                {},
	"task.submit":                                        {},
	"task.patch_queue.review.create":                     {},
	"task.patch_queue.decision_continuation.create":      {},
	"task.project_fields.put":                            {},
	"task.close":                                         {},
	"workspace.ops.upsert":                               {},
	"workspace.ops.request":                              {},
	"workspace.ops.resolve":                              {},
	"workspace.ops.escalate":                             {},
	"workspace.memory.write":                             {},
	"workspace.memory.node.write":                        {},
	"workspace.memory.node.touch":                        {},
	"workspace.memory.remove":                            {},
	"workspace.memory.restore":                           {},
	"workspace.tension.refresh":                          {},
	"workspace.tension.confirm":                          {},
	"workspace.tension.discard":                          {},
	"workspace.tension.archive":                          {},
	"workspace.tension.resolve":                          {},
	"workspace.tension.dormant":                          {},
	"workspace.tension.lifecycle.update":                 {},
	"workspace.tension.add.dependency":                   {},
	"workspace.tension.remove.dependency":                {},
	"workspace.tension.condense":                         {},
	"workspace.tension.agent.attach":                     {},
	"workspace.tension.agent.detach":                     {},
	"workspace.claim.write":                              {},
	"workspace.claim.archive":                            {},
	"workspace.claim.review":                             {},
	"workspace.claim.confirm":                            {},
	"workspace.claim.dispute":                            {},
	"workspace.claim.supersede":                          {},
	"workspace.claim.stale":                              {},
	"workspace.claim.escalate":                           {},
	"workspace.doc.put":                                  {},
	"workspace.doc.archive":                              {},
	"workspace.doc.delete":                               {},
	"agent.update.post":                                  {},
	"agent.delete":                                       {},
	"agent.profile.update":                               {},
	"workspace.artifact.write":                           {},
	"project.create":                                     {},
	"project.update":                                     {},
	"project.delete":                                     {},
	"project.profile.update":                             {},
	"project.phase.transition":                           {},
	"project.gate.update":                                {},
	"project.lead.claim":                                 {},
	"project.lead.renew":                                 {},
	"project.lead.release":                               {},
	"project.lead.transfer":                              {},
	"project.role.assign":                                {},
	"project.governance.challenge.raise":                 {},
	"project.governance.challenge.defend":                {},
	"project.governance.vote.cast":                       {},
	"project.governance.challenge.tally":                 {},
	"project.governance.challenge.deadline_sweep":        {},
	"project.repository.upsert":                          {},
	"cli.workspace.project.repository.upsert":            {},
	"project.checkout.register":                          {},
	"project.branch.register":                            {},
	"project.patch_queue.submit":                         {},
	"project.patch_queue.supersede":                      {},
	"project.patch_queue.claim":                          {},
	"project.patch_queue.operation_bind":                 {},
	"project.patch_queue.cas_record":                     {},
	"project.patch_queue.materialization_record":         {},
	"project.patch_queue.integration_record":             {},
	"project.patch_queue.integration_repair":             {},
	"project.patch_queue.rollback_record":                {},
	"project.patch_queue.reviewer_advisory_record":       {},
	"project.patch_queue.operator_enablement_record":     {},
	"cli.project.patch_queue.operator_enablement_record": {},
	"operator.patch_queue.enable":                        {},
	"project.patch_queue.release":                        {},
	"project.patch_queue.decision":                       {},
	"service.direction.upsert":                           {},
	"service.candidate.upsert":                           {},
	"service.run.start":                                  {},
	"service.run.update":                                 {},
	"service.approval.grant":                             {},
	"service.resource.record":                            {},
	"service.spend.record":                               {},
	"service.revenue.record":                             {},
	"service.outcome.record":                             {},
	"limits.group.create":                                {},
	"limits.group.update":                                {},
	"limits.group.delete":                                {},
	"vault.create":                                       {},
	"vault.update":                                       {},
	"vault.delete":                                       {},
	"vault.get":                                          {},
	"vault.list":                                         {},
	"news.publish":                                       {},
	"news.delete":                                        {},
	"tool.register":                                      {},
	"tool.remove":                                        {},
	"mcp.workspace_tool.project":                         {},
	"workspace.policy.put":                               {},
	"workspace.rsp.capability.put":                       {},
	"workspace.control.command.request":                  {},
	"workspace.instrumentation.control.state.tick":       {},
	"workspace.instrumentation.control.state.snapshot":   {},
	"cli.workspace.execution.run.write":                  {},
	"cli.workspace.execution.step.write":                 {},
	"cli.workspace.ops.upsert":                           {},
	"cli.workspace.ops.resolve":                          {},
	"cli.workspace.ops.escalate":                         {},
	"cli.workspace.doc.put":                              {},
	"cli.workspace.doc.archive":                          {},
	"cli.workspace.doc.delete":                           {},
	"cli.agent.update.post":                              {},
	"cli.tool.invoke.update":                             {},
	"cli.workspace.artifact.put":                         {},
	"cli.tool.register":                                  {},
	"cli.tool.remove":                                    {},
	"cli.workspace.policy.put":                           {},
}

var allowedExecutionPromptContextOrigins = map[string]struct{}{
	"server_rpc":                {},
	"server_session_projection": {},
	"server_operation_ledger":   {},
	"server_mcp_projection":     {},
	"serve_loop":                {},
	"cli_local":                 {},
	"agent_tool":                {},
}

var allowedExecutionPromptContextOriginsByRecord = map[string]map[string]struct{}{
	"execution_run": {
		"server_rpc":                {},
		"server_session_projection": {},
		"server_operation_ledger":   {},
		"cli_local":                 {},
	},
	"execution_step": {
		"server_rpc":                {},
		"server_session_projection": {},
		"cli_local":                 {},
	},
	"operator_queue": {
		"server_rpc": {},
		"cli_local":  {},
	},
	"human_action": {
		"server_rpc": {},
	},
	"task_lifecycle": {
		"server_rpc": {},
		"cli_local":  {},
	},
	"session_lifecycle": {
		"server_rpc": {},
		"cli_local":  {},
	},
	"agent_message": {
		"server_rpc": {},
	},
	"agent_request": {
		"server_rpc": {},
	},
	"workspace_memory": {
		"server_rpc": {},
	},
	"workspace_tension": {
		"server_rpc": {},
		"agent_tool": {},
	},
	"knowledge_claim": {
		"server_rpc": {},
	},
	"node_lifecycle": {
		"server_rpc": {},
	},
	"workspace_doc": {
		"server_rpc": {},
		"cli_local":  {},
	},
	"agent_update": {
		"server_rpc": {},
		"cli_local":  {},
	},
	"agent_lifecycle": {
		"server_rpc": {},
	},
	"workspace_artifact": {
		"server_rpc": {},
		"cli_local":  {},
	},
	"project": {
		"server_rpc": {},
		"serve_loop": {},
		"cli_local":  {},
	},
	"service_venture": {
		"server_rpc": {},
	},
	"limit_group": {
		"server_rpc": {},
	},
	"vault_entry": {
		"server_rpc": {},
	},
	"news": {
		"server_rpc": {},
	},
	"tool_call": {
		"server_rpc": {},
	},
	"workspace_tool": {
		"server_rpc":            {},
		"server_mcp_projection": {},
		"cli_local":             {},
	},
	"capability_policy": {
		"server_rpc": {},
		"cli_local":  {},
	},
	"control_command": {
		"server_rpc": {},
	},
	"control_state": {
		"server_rpc": {},
	},
}

var allowedExecutionPromptContextSurfacesByRecord = map[string]map[string]struct{}{
	"execution_run": {
		"workspace.execution.run.write":      {},
		"agent.session.execution.run.sync":   {},
		"tool.call":                          {},
		"tool.deploy":                        {},
		"tool.undeploy":                      {},
		"mcp.server.register":                {},
		"mcp.server.remove":                  {},
		"mcp.tool.discover":                  {},
		"project.patch_queue.operation_bind": {},
		"cli.workspace.execution.run.write":  {},
	},
	"execution_step": {
		"workspace.execution.step.write":     {},
		"agent.session.execution.step.sync":  {},
		"cli.workspace.execution.step.write": {},
	},
	"operator_queue": {
		"workspace.ops.upsert":       {},
		"workspace.ops.request":      {},
		"workspace.ops.resolve":      {},
		"workspace.ops.escalate":     {},
		"cli.workspace.ops.upsert":   {},
		"cli.workspace.ops.resolve":  {},
		"cli.workspace.ops.escalate": {},
	},
	"human_action": {
		"action.create":    {},
		"action.start":     {},
		"action.pause":     {},
		"action.resolve":   {},
		"action.chat.send": {},
	},
	"task_lifecycle": {
		"task.submit":                                   {},
		"task.patch_queue.review.create":                {},
		"task.patch_queue.decision_continuation.create": {},
		"task.project_fields.put":                       {},
		"task.close":                                    {},
		"agent.task.claim":                              {},
		"agent.task_frontier.decision":                  {},
		"agent.task.release":                            {},
		"agent.task.complete":                           {},
		"agent.task.block":                              {},
	},
	"node_lifecycle": {
		"agent.node.claim":    {},
		"agent.node.release":  {},
		"agent.node.complete": {},
	},
	"session_lifecycle": {
		"agent.session.start":           {},
		"agent.session.status":          {},
		"agent.session.blocked":         {},
		"agent.session.decision_needed": {},
		"agent.session.keepalive":       {},
		"agent.session.end":             {},
		"agent.session.takeover":        {},
	},
	"agent_message": {
		"agent.message.send": {},
	},
	"agent_request": {
		"agent.request":      {},
		"agent.request.list": {},
		"agent.respond":      {},
	},
	"workspace_memory": {
		"workspace.memory.write":      {},
		"workspace.memory.node.write": {},
		"workspace.memory.node.touch": {},
		"workspace.memory.remove":     {},
		"workspace.memory.restore":    {},
	},
	"workspace_tension": {
		"workspace.tension.refresh":           {},
		"workspace.tension.confirm":           {},
		"workspace.tension.discard":           {},
		"workspace.tension.archive":           {},
		"workspace.tension.resolve":           {},
		"workspace.tension.dormant":           {},
		"workspace.tension.lifecycle.update":  {},
		"workspace.tension.add.dependency":    {},
		"workspace.tension.remove.dependency": {},
		"workspace.tension.condense":          {},
		"workspace.tension.agent.attach":      {},
		"workspace.tension.agent.detach":      {},
		"agent.tension.attach":                {},
		"agent.tension.detach":                {},
		"coalition.offer":                     {},
		"coalition.leave":                     {},
		"coalition.invite":                    {},
		"coalition.kick":                      {},
	},
	"knowledge_claim": {
		"workspace.claim.write":     {},
		"workspace.claim.archive":   {},
		"workspace.claim.review":    {},
		"workspace.claim.confirm":   {},
		"workspace.claim.dispute":   {},
		"workspace.claim.supersede": {},
		"workspace.claim.stale":     {},
		"workspace.claim.escalate":  {},
	},
	"workspace_doc": {
		"workspace.doc.put":         {},
		"workspace.doc.archive":     {},
		"workspace.doc.delete":      {},
		"cli.workspace.doc.put":     {},
		"cli.workspace.doc.archive": {},
		"cli.workspace.doc.delete":  {},
	},
	"agent_update": {
		"agent.update.post":      {},
		"cli.agent.update.post":  {},
		"cli.tool.invoke.update": {},
	},
	"agent_lifecycle": {
		"agent.delete": {},
	},
	"agent_profile": {
		"agent.profile.update": {},
	},
	"workspace_artifact": {
		"workspace.artifact.write":   {},
		"cli.workspace.artifact.put": {},
	},
	"project": {
		"project.create":                                     {},
		"project.update":                                     {},
		"project.delete":                                     {},
		"project.profile.update":                             {},
		"project.phase.transition":                           {},
		"project.gate.update":                                {},
		"project.lead.claim":                                 {},
		"project.lead.renew":                                 {},
		"project.lead.release":                               {},
		"project.lead.transfer":                              {},
		"project.role.assign":                                {},
		"project.governance.challenge.raise":                 {},
		"project.governance.challenge.defend":                {},
		"project.governance.vote.cast":                       {},
		"project.governance.challenge.tally":                 {},
		"project.governance.challenge.deadline_sweep":        {},
		"project.repository.upsert":                          {},
		"cli.workspace.project.repository.upsert":            {},
		"project.checkout.register":                          {},
		"project.branch.register":                            {},
		"project.patch_queue.submit":                         {},
		"project.patch_queue.supersede":                      {},
		"project.patch_queue.claim":                          {},
		"project.patch_queue.operation_bind":                 {},
		"project.patch_queue.cas_record":                     {},
		"project.patch_queue.materialization_record":         {},
		"project.patch_queue.integration_record":             {},
		"project.patch_queue.integration_repair":             {},
		"project.patch_queue.rollback_record":                {},
		"project.patch_queue.reviewer_advisory_record":       {},
		"project.patch_queue.operator_enablement_record":     {},
		"cli.project.patch_queue.operator_enablement_record": {},
		"operator.patch_queue.enable":                        {},
		"project.patch_queue.release":                        {},
		"project.patch_queue.decision":                       {},
	},
	"service_venture": {
		"service.direction.upsert": {},
		"service.candidate.upsert": {},
		"service.run.start":        {},
		"service.run.update":       {},
		"service.approval.grant":   {},
		"service.resource.record":  {},
		"service.spend.record":     {},
		"service.revenue.record":   {},
		"service.outcome.record":   {},
	},
	"limit_group": {
		"limits.group.create": {},
		"limits.group.update": {},
		"limits.group.delete": {},
	},
	"vault_entry": {
		"vault.create": {},
		"vault.update": {},
		"vault.delete": {},
		"vault.get":    {},
		"vault.list":   {},
	},
	"news": {
		"news.publish": {},
		"news.delete":  {},
	},
	"tool_call": {
		"tool.call": {},
	},
	"workspace_tool": {
		"tool.register":              {},
		"tool.remove":                {},
		"mcp.workspace_tool.project": {},
		"cli.tool.register":          {},
		"cli.tool.remove":            {},
	},
	"capability_policy": {
		"workspace.policy.put":         {},
		"workspace.rsp.capability.put": {},
		"cli.workspace.policy.put":     {},
	},
	"control_command": {
		"workspace.control.command.request": {},
	},
	"control_state": {
		"workspace.instrumentation.control.state.tick":     {},
		"workspace.instrumentation.control.state.snapshot": {},
	},
}

var executionPromptContextKindByRecord = map[string]string{
	"execution_run":      executionPromptContextKind,
	"execution_step":     executionPromptContextKind,
	"operator_queue":     operatorQueuePromptContextKind,
	"human_action":       humanActionPromptContextKind,
	"task_lifecycle":     taskPromptContextKind,
	"session_lifecycle":  sessionPromptContextKind,
	"agent_message":      agentMessagePromptContextKind,
	"agent_request":      agentRequestPromptContextKind,
	"workspace_memory":   workspaceMemoryPromptContextKind,
	"workspace_tension":  workspaceTensionPromptContextKind,
	"knowledge_claim":    knowledgeClaimPromptContextKind,
	"node_lifecycle":     nodeLifecyclePromptContextKind,
	"workspace_doc":      workspaceDocPromptContextKind,
	"agent_update":       agentUpdatePromptContextKind,
	"agent_lifecycle":    agentLifecyclePromptContextKind,
	"agent_profile":      agentProfilePromptContextKind,
	"workspace_artifact": workspaceArtifactPromptContextKind,
	"project":            projectPromptContextKind,
	"service_venture":    serviceVenturePromptContextKind,
	"limit_group":        limitGroupPromptContextKind,
	"vault_entry":        vaultPromptContextKind,
	"news":               newsPromptContextKind,
	"tool_call":          toolCallPromptContextKind,
	"workspace_tool":     toolRegistryPromptContextKind,
	"capability_policy":  capabilityPolicyPromptContextKind,
	"control_command":    controlCommandPromptContextKind,
	"control_state":      controlStatePromptContextKind,
}

func BuildExecutionPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, executionPromptContextKind)
}

func BuildOperatorQueuePromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, operatorQueuePromptContextKind)
}

func BuildHumanActionPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, humanActionPromptContextKind)
}

func BuildTaskPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, taskPromptContextKind)
}

func BuildSessionPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, sessionPromptContextKind)
}

func BuildAgentMessagePromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, agentMessagePromptContextKind)
}

func BuildAgentRequestPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, agentRequestPromptContextKind)
}

func BuildWorkspaceMemoryPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, workspaceMemoryPromptContextKind)
}

func BuildWorkspaceTensionPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, workspaceTensionPromptContextKind)
}

func BuildKnowledgeClaimPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, knowledgeClaimPromptContextKind)
}

func BuildNodeLifecyclePromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, nodeLifecyclePromptContextKind)
}

func BuildWorkspaceDocPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, workspaceDocPromptContextKind)
}

func BuildAgentUpdatePromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, agentUpdatePromptContextKind)
}

func BuildAgentLifecyclePromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, agentLifecyclePromptContextKind)
}

func BuildAgentProfilePromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, agentProfilePromptContextKind)
}

func BuildWorkspaceArtifactPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, workspaceArtifactPromptContextKind)
}

func BuildProjectPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, projectPromptContextKind)
}

func BuildServiceVenturePromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, serviceVenturePromptContextKind)
}

func BuildLimitGroupPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, limitGroupPromptContextKind)
}

func BuildVaultPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, vaultPromptContextKind)
}

func BuildNewsPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, newsPromptContextKind)
}

func BuildToolCallPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, toolCallPromptContextKind)
}

func BuildToolRegistryPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, toolRegistryPromptContextKind)
}

func BuildCapabilityPolicyPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, capabilityPolicyPromptContextKind)
}

func BuildControlCommandPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, controlCommandPromptContextKind)
}

func BuildControlStatePromptContextEnvelope(surface, origin, workspaceID, principalType, principalID string) map[string]any {
	return buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, controlStatePromptContextKind)
}

func buildPromptContextEnvelope(surface, origin, workspaceID, principalType, principalID, contextKind string) map[string]any {
	principalType = strings.TrimSpace(principalType)
	if principalType == "" {
		principalType = "system"
	}
	principalID = strings.TrimSpace(principalID)
	if principalID == "" {
		principalID = "unknown"
	}
	return map[string]any{
		"contract":                           executionPromptContextEnvelopeContract,
		"context_kind":                       contextKind,
		"surface":                            strings.TrimSpace(surface),
		"origin":                             strings.TrimSpace(origin),
		"workspace_id":                       strings.TrimSpace(workspaceID),
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"authority_model":                    executionPromptContextAuthorityModel,
		"compiler_status":                    executionPromptContextCompilerStatus,
		"daemon_prompt_compiler_convergence": executionPromptContextDaemonClaim,
		"prompt_capability_evidence":         executionPromptContextCapabilityProof,
	}
}

func AttachExecutionPromptContextEnvelope(verification map[string]any, envelope map[string]any) map[string]any {
	out := make(map[string]any, len(verification)+1)
	for key, value := range verification {
		out[key] = value
	}
	out[executionPromptContextEnvelopeKey] = envelope
	return out
}

func AttachHumanActionPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("human_action.payload_json", "human_action", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachTaskPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("task_lifecycle.payload_json", "task_lifecycle", out); err != nil {
		return nil, err
	}
	return out, nil
}

func attachAgentTaskPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface, workspaceID, taskID, agentID string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	out, err := AttachTaskPromptContextEnvelope(payload, envelope)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":        strings.TrimSpace(expectedSurface),
		"origin":         "server_rpc",
		"workspace_id":   strings.TrimSpace(workspaceID),
		"principal_type": "agent",
		"principal_id":   strings.TrimSpace(agentID),
		"actor_agent_id": strings.TrimSpace(agentID),
		"agent_id":       strings.TrimSpace(agentID),
		"task_id":        strings.TrimSpace(taskID),
	}
	if expectedClaimStatus := agentTaskClaimStatusForSurface(expectedSurface); expectedClaimStatus != "" {
		required["claim_status"] = expectedClaimStatus
		if got := executionPromptRawString(out, "claim_status"); got != expectedClaimStatus {
			return nil, fmt.Errorf("task_lifecycle.payload_json has claim_status=%q; expected %q", got, expectedClaimStatus)
		}
	}
	if err := validateAgentTaskPromptContextEnvelopeBindings("task_lifecycle.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func validateAgentTaskPromptContextEnvelopeBindings(path string, value any, required map[string]string) error {
	switch typed := value.(type) {
	case map[string]any:
		if executionPromptContextEnvelopeMapNeedsValidation(path, typed) {
			if len(required) == 0 {
				return fmt.Errorf("%s has no task prompt context binding contract", path)
			}
			for key, want := range required {
				if got := executionPromptRawString(typed, key); got != want {
					return fmt.Errorf("%s has %s=%q; expected %q", path, key, got, want)
				}
			}
		}
		for key, nested := range typed {
			if err := validateAgentTaskPromptContextEnvelopeBindings(path+"."+strings.TrimSpace(key), nested, required); err != nil {
				return err
			}
		}
	case []any:
		for idx, nested := range typed {
			if err := validateAgentTaskPromptContextEnvelopeBindings(fmt.Sprintf("%s[%d]", path, idx), nested, required); err != nil {
				return err
			}
		}
	}
	return nil
}

func agentTaskClaimStatusForSurface(surface string) string {
	switch strings.TrimSpace(surface) {
	case "agent.task.claim":
		return "CLAIMED"
	case "agent.task.release":
		return "RELEASED"
	case "agent.task.complete":
		return "COMPLETED"
	case "agent.task.block":
		return "BLOCKED"
	default:
		return ""
	}
}

func AttachSessionPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("session_lifecycle.payload_json", "session_lifecycle", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachAgentMessagePromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("agent_message.payload_json", "agent_message", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachAgentRequestPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("agent_request.payload_json", "agent_request", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachWorkspaceMemoryPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("workspace_memory.payload_json", "workspace_memory", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachWorkspaceTensionPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("workspace_tension.payload_json", "workspace_tension", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachKnowledgeClaimPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("knowledge_claim.payload_json", "knowledge_claim", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachNodeLifecyclePromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("node_lifecycle.payload_json", "node_lifecycle", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachWorkspaceDocPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("workspace_doc.payload_json", "workspace_doc", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachAgentUpdatePromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("agent_update.payload_json", "agent_update", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachAgentLifecyclePromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("agent_lifecycle.payload_json", "agent_lifecycle", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachAgentProfilePromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("agent_profile.payload_json", "agent_profile", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachWorkspaceArtifactPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("workspace_artifact.payload_json", "workspace_artifact", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachProjectPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("project.payload_json", "project", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachServiceVenturePromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("service_venture.payload_json", "service_venture", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachLimitGroupPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("limit_group.payload_json", "limit_group", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachVaultPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("vault_entry.payload_json", "vault_entry", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachNewsPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("news.payload_json", "news", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachToolCallPromptContextEnvelope(payload map[string]any, envelope map[string]any, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out := AttachExecutionPromptContextEnvelope(payload, enriched)
	if err := walkExecutionPromptContextEnvelope("tool_call.payload_json", "tool_call", out); err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface": "tool.call",
		"origin":  "server_rpc",
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("tool_call.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func validateRuntimeEventToolCallContract(record RuntimeEventRecord) error {
	if !runtimeEventRequiresToolCallPromptContext(record.EventType) {
		return nil
	}
	eventType := strings.TrimSpace(record.EventType)
	if strings.TrimSpace(record.AuthorityHolderNodeID) == "" || record.AuthorityTerm <= 0 || strings.TrimSpace(record.AuthorityLeaseTokenFingerprint) == "" {
		return fmt.Errorf("%s runtime event requires workspace authority metadata", eventType)
	}
	if strings.TrimSpace(record.EntityType) != "tool" {
		return fmt.Errorf("%s runtime event requires entity_type %q", eventType, "tool")
	}
	toolID := strings.TrimSpace(record.EntityID)
	if toolID == "" {
		return fmt.Errorf("%s runtime event requires tool entity_id", eventType)
	}
	actorType := strings.TrimSpace(record.ActorType)
	actorID := strings.TrimSpace(record.ActorID)
	if actorType == "" || actorID == "" {
		return fmt.Errorf("%s runtime event requires non-empty actor_type and actor_id", eventType)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(record.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("%s runtime event payload_json must be a JSON object with prompt_context_envelope: %w", eventType, err)
	}
	if payload == nil {
		return fmt.Errorf("%s runtime event payload_json must be a JSON object with prompt_context_envelope", eventType)
	}
	if rawEnvelope, ok := payload[executionPromptContextEnvelopeKey]; !ok || rawEnvelope == nil {
		return fmt.Errorf("%s runtime event payload_json missing %s", eventType, executionPromptContextEnvelopeKey)
	} else if _, ok := rawEnvelope.(map[string]any); !ok {
		return fmt.Errorf("%s runtime event payload_json.%s must be an object", eventType, executionPromptContextEnvelopeKey)
	}
	if err := walkExecutionPromptContextEnvelope("runtime_event.payload_json", "tool_call", payload); err != nil {
		return err
	}
	requestedCapability := strings.TrimSpace(executionPromptRawString(payload, "requested_capability"))
	if requestedCapability == "" {
		return fmt.Errorf("%s runtime event payload_json missing requested_capability", eventType)
	}
	fields := map[string]string{
		"surface":               "tool.call",
		"origin":                "server_rpc",
		"workspace_id":          strings.TrimSpace(record.WorkspaceID),
		"tool_id":               toolID,
		"event_type":            eventType,
		"entity_type":           "tool",
		"entity_id":             toolID,
		"actor_type":            actorType,
		"actor_id":              actorID,
		"requested_capability":  requestedCapability,
		"authority_event_scope": "tool.call",
	}
	if eventType == "tool.call.executed" {
		operationID := strings.TrimSpace(executionPromptRawString(payload, "operation_id"))
		if operationID == "" {
			return fmt.Errorf("%s runtime event payload_json missing operation_id", eventType)
		}
		fields["operation_id"] = operationID
	}
	for key, want := range fields {
		if key == "surface" || key == "origin" {
			continue
		}
		if strings.TrimSpace(want) == "" {
			continue
		}
		if got := executionPromptRawString(payload, key); got != want {
			return fmt.Errorf("%s runtime event payload_json has %s=%q; expected %q", eventType, key, got, want)
		}
	}
	if err := validatePromptContextEnvelopeRequiredBindings("runtime_event.payload_json", payload, fields); err != nil {
		return err
	}
	return nil
}

func runtimeEventRequiresToolCallPromptContext(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "tool.call.executed", "tool.call.denied", "tool.call.approval_required":
		return true
	default:
		return false
	}
}

func AttachToolRegistryPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out := AttachExecutionPromptContextEnvelope(payload, enriched)
	if err := walkExecutionPromptContextEnvelope("workspace_tool.payload_json", "workspace_tool", out); err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface": strings.TrimSpace(expectedSurface),
		"origin":  expectedPromptContextOriginForSurface(expectedSurface),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("workspace_tool.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachCapabilityPolicyPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("capability_policy.payload_json", "capability_policy", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachControlCommandPromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("control_command.payload_json", "control_command", out); err != nil {
		return nil, err
	}
	return out, nil
}

func AttachControlStatePromptContextEnvelope(payload map[string]any, envelope map[string]any) (map[string]any, error) {
	out := AttachExecutionPromptContextEnvelope(payload, envelope)
	if err := walkExecutionPromptContextEnvelope("control_state.payload_json", "control_state", out); err != nil {
		return nil, err
	}
	return out, nil
}

func attachWorkspaceMemoryPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachWorkspaceMemoryPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface": strings.TrimSpace(expectedSurface),
		"origin":  expectedPromptContextOriginForSurface(expectedSurface),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("workspace_memory.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachWorkspaceTensionPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	if strings.TrimSpace(fields["principal_type"]) == "" || strings.TrimSpace(fields["principal_id"]) == "" {
		return nil, fmt.Errorf("workspace_tension prompt context requires expected principal binding")
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachWorkspaceTensionPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
	}
	for key, value := range fields {
		if strings.TrimSpace(value) != "" {
			required[key] = strings.TrimSpace(value)
		}
	}
	if err := validatePromptContextEnvelopeRequiredBindings("workspace_tension.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachKnowledgeClaimPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachKnowledgeClaimPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface": strings.TrimSpace(expectedSurface),
		"origin":  expectedPromptContextOriginForSurface(expectedSurface),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("knowledge_claim.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachAgentNodePromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface, workspaceID, taskID, nodeID, agentID, nodeStatusAfter string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	fields := map[string]string{
		"actor_agent_id": strings.TrimSpace(agentID),
		"agent_id":       strings.TrimSpace(agentID),
		"task_id":        strings.TrimSpace(taskID),
		"node_id":        strings.TrimSpace(nodeID),
	}
	if status := agentNodeLifecycleStatusForSurface(expectedSurface); status != "" {
		fields["status"] = status
		fields["node_claim_status"] = status
	}
	if nodeStatusAfter = strings.TrimSpace(nodeStatusAfter); nodeStatusAfter != "" {
		fields["node_status_after"] = nodeStatusAfter
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachNodeLifecyclePromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":        strings.TrimSpace(expectedSurface),
		"origin":         "server_rpc",
		"workspace_id":   strings.TrimSpace(workspaceID),
		"principal_type": "agent",
		"principal_id":   strings.TrimSpace(agentID),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if status := agentNodeLifecycleStatusForSurface(expectedSurface); status != "" {
		if got := executionPromptRawString(out, "status"); got != status {
			return nil, fmt.Errorf("node_lifecycle.payload_json has status=%q; expected %q", got, status)
		}
		if got := executionPromptRawString(out, "node_claim_status"); got != status {
			return nil, fmt.Errorf("node_lifecycle.payload_json has node_claim_status=%q; expected %q", got, status)
		}
	}
	if err := validatePromptContextEnvelopeRequiredBindings("node_lifecycle.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachWorkspaceDocPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachWorkspaceDocPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface": strings.TrimSpace(expectedSurface),
		"origin":  expectedPromptContextOriginForSurface(expectedSurface),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if actorID := workspaceDocPromptContextActorID(expectedSurface, fields); actorID != "" {
		required["principal_id"] = actorID
	}
	if err := validatePromptContextEnvelopeRequiredBindings("workspace_doc.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachAgentUpdatePromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachAgentUpdatePromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":        strings.TrimSpace(expectedSurface),
		"origin":         expectedPromptContextOriginForSurface(expectedSurface),
		"principal_type": "agent",
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if agentID := strings.TrimSpace(fields["agent_id"]); agentID != "" {
		required["principal_id"] = agentID
	}
	if err := validatePromptContextEnvelopeRequiredBindings("agent_update.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachAgentLifecyclePromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachAgentLifecyclePromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
		"agent_id":     strings.TrimSpace(fields["agent_id"]),
		"actor_id":     strings.TrimSpace(fields["actor_id"]),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("agent_lifecycle.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachAgentProfilePromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachAgentProfilePromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
		"agent_id":     strings.TrimSpace(fields["agent_id"]),
		"actor_id":     strings.TrimSpace(fields["actor_id"]),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("agent_profile.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachWorkspaceArtifactPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachWorkspaceArtifactPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
	}
	if createdBy := strings.TrimSpace(fields["created_by"]); createdBy != "" {
		required["principal_id"] = createdBy
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("workspace_artifact.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachCapabilityPolicyPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachCapabilityPolicyPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
	}
	if createdBy := strings.TrimSpace(fields["created_by"]); createdBy != "" {
		required["principal_id"] = createdBy
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("capability_policy.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachControlCommandPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachControlCommandPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
	}
	if requestedBy := strings.TrimSpace(fields["requested_by"]); requestedBy != "" {
		required["principal_id"] = requestedBy
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("control_command.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func attachControlStatePromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachControlStatePromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("control_state.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}

func workspaceDocPromptContextActorID(surface string, fields map[string]string) string {
	switch strings.TrimSpace(surface) {
	case "workspace.doc.put", "cli.workspace.doc.put":
		return strings.TrimSpace(fields["updated_by"])
	case "workspace.doc.archive", "cli.workspace.doc.archive":
		return strings.TrimSpace(fields["archived_by"])
	case "workspace.doc.delete", "cli.workspace.doc.delete":
		return strings.TrimSpace(fields["deleted_by"])
	default:
		return ""
	}
}

func expectedPromptContextOriginForSurface(surface string) string {
	surface = strings.TrimSpace(surface)
	if strings.HasPrefix(surface, "cli.") {
		return "cli_local"
	}
	switch surface {
	case "agent.tension.attach", "agent.tension.detach":
		return "agent_tool"
	}
	if surface == "mcp.workspace_tool.project" {
		return "server_mcp_projection"
	}
	if surface == "project.governance.challenge.deadline_sweep" {
		return "serve_loop"
	}
	return "server_rpc"
}

func agentNodeLifecycleStatusForSurface(surface string) string {
	switch strings.TrimSpace(surface) {
	case "agent.node.claim":
		return "CLAIMED"
	case "agent.node.release":
		return "RELEASED"
	case "agent.node.complete":
		return "COMPLETED"
	default:
		return ""
	}
}

func mergeAgentUpdatePromptContextPayloadJSON(payloadJSON string, envelope map[string]any, expectedSurface string, fields map[string]string) (string, error) {
	payloadJSON = strings.TrimSpace(payloadJSON)
	if envelope == nil {
		if err := rejectAgentUpdatePayloadPromptContextMarkers(payloadJSON); err != nil {
			return "", err
		}
		return payloadJSON, nil
	}
	payload, err := agentUpdatePromptContextPayloadObject(payloadJSON)
	if err != nil {
		return "", err
	}
	if err := rejectCallerSuppliedPromptContextMarkers("agent_update.payload_json.input", payload); err != nil {
		return "", err
	}
	out, err := attachAgentUpdatePromptContextEnvelope(payload, envelope, expectedSurface, fields)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("encode agent_update prompt context payload_json: %w", err)
	}
	return string(encoded), nil
}

func agentUpdatePromptContextPayloadObject(payloadJSON string) (map[string]any, error) {
	payloadJSON = strings.TrimSpace(payloadJSON)
	if payloadJSON == "" {
		return map[string]any{}, nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(payloadJSON), &decoded); err != nil {
		return map[string]any{"payload_text": payloadJSON}, nil
	}
	if object, ok := decoded.(map[string]any); ok {
		return object, nil
	}
	return map[string]any{"payload_value": decoded}, nil
}

func rejectAgentUpdatePayloadPromptContextMarkers(payloadJSON string) error {
	if strings.TrimSpace(payloadJSON) == "" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(payloadJSON), &decoded); err != nil {
		return nil
	}
	return rejectCallerSuppliedPromptContextMarkers("agent_update.payload_json.input", decoded)
}

func rejectCallerSuppliedPromptContextMarkers(path string, value any) error {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed[executionPromptContextEnvelopeKey]; ok {
			return fmt.Errorf("%s contains caller-supplied %s", path, executionPromptContextEnvelopeKey)
		}
		if executionPromptRawString(typed, "contract") == executionPromptContextEnvelopeContract {
			return fmt.Errorf("%s contains caller-supplied %s contract marker", path, executionPromptContextEnvelopeContract)
		}
		for key, nested := range typed {
			if err := rejectCallerSuppliedPromptContextMarkers(path+"."+strings.TrimSpace(key), nested); err != nil {
				return err
			}
		}
	case []any:
		for idx, nested := range typed {
			if err := rejectCallerSuppliedPromptContextMarkers(fmt.Sprintf("%s[%d]", path, idx), nested); err != nil {
				return err
			}
		}
	}
	return nil
}

func enrichPromptContextEnvelope(envelope map[string]any, fields map[string]string) (map[string]any, error) {
	out := make(map[string]any, len(envelope)+len(fields))
	for key, value := range envelope {
		out[key] = value
	}
	for key, value := range fields {
		key = strings.TrimSpace(key)
		want := strings.TrimSpace(value)
		if key == "" {
			continue
		}
		if got := executionPromptRawString(out, key); got != "" && got != want {
			return nil, fmt.Errorf("prompt_context_envelope has %s=%q; expected %q", key, got, want)
		}
		if want != "" {
			out[key] = want
		}
	}
	return out, nil
}

func validatePromptContextEnvelopeRequiredBindings(path string, value any, required map[string]string) error {
	switch typed := value.(type) {
	case map[string]any:
		if executionPromptContextEnvelopeMapNeedsValidation(path, typed) {
			if len(required) == 0 {
				return fmt.Errorf("%s has no prompt context binding contract", path)
			}
			for key, want := range required {
				if got := executionPromptRawString(typed, key); got != strings.TrimSpace(want) {
					return fmt.Errorf("%s has %s=%q; expected %q", path, key, got, strings.TrimSpace(want))
				}
			}
		}
		for key, nested := range typed {
			if err := validatePromptContextEnvelopeRequiredBindings(path+"."+strings.TrimSpace(key), nested, required); err != nil {
				return err
			}
		}
	case []any:
		for idx, nested := range typed {
			if err := validatePromptContextEnvelopeRequiredBindings(fmt.Sprintf("%s[%d]", path, idx), nested, required); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateExecutionPromptContextEnvelope(surface string, verification map[string]any) error {
	if len(verification) == 0 {
		return nil
	}
	return walkExecutionPromptContextEnvelope(surface+".verification", surface, verification)
}

func validateExecutionPromptContextEnvelopeBinding(recordSurface string, verification map[string]any, workspaceID, expectedAgentID string) error {
	if len(verification) == 0 {
		return nil
	}
	return walkExecutionPromptContextEnvelopeBinding(recordSurface+".verification", verification, strings.TrimSpace(workspaceID), strings.TrimSpace(expectedAgentID))
}

func walkExecutionPromptContextEnvelopeBinding(path string, value any, workspaceID, expectedAgentID string) error {
	switch typed := value.(type) {
	case map[string]any:
		if executionPromptContextEnvelopeMapNeedsValidation(path, typed) {
			envelopeWorkspaceID := executionPromptRawString(typed, "workspace_id")
			if workspaceID != "" && envelopeWorkspaceID != workspaceID {
				return fmt.Errorf("%s has workspace_id=%q that does not match record workspace_id=%q", path, envelopeWorkspaceID, workspaceID)
			}
			principalType := executionPromptRawString(typed, "principal_type")
			principalID := executionPromptRawString(typed, "principal_id")
			origin := executionPromptRawString(typed, "origin")
			if origin == "server_session_projection" && expectedAgentID != "" {
				if principalType != "agent" || principalID != expectedAgentID {
					return fmt.Errorf("%s has principal %s/%s that does not match session projection agent %q", path, principalType, principalID, expectedAgentID)
				}
			}
			if principalType == "agent" && expectedAgentID != "" && principalID != expectedAgentID {
				return fmt.Errorf("%s has agent principal_id=%q that does not match record agent_id=%q", path, principalID, expectedAgentID)
			}
		}
		for key, nested := range typed {
			if err := walkExecutionPromptContextEnvelopeBinding(path+"."+strings.TrimSpace(key), nested, workspaceID, expectedAgentID); err != nil {
				return err
			}
		}
	case []any:
		for idx, nested := range typed {
			if err := walkExecutionPromptContextEnvelopeBinding(fmt.Sprintf("%s[%d]", path, idx), nested, workspaceID, expectedAgentID); err != nil {
				return err
			}
		}
	}
	return nil
}

func mergeOperatorQueuePromptContextPayloadJSON(payloadJSON string, envelope map[string]any) (string, error) {
	payloadJSON = strings.TrimSpace(payloadJSON)
	requiresValidation := envelope != nil ||
		strings.Contains(payloadJSON, executionPromptContextEnvelopeKey) ||
		strings.Contains(payloadJSON, executionPromptContextEnvelopeContract)
	if !requiresValidation {
		return payloadJSON, nil
	}
	payload := map[string]any{}
	if payloadJSON != "" {
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return "", fmt.Errorf("decode operator_queue payload_json for prompt context envelope: %w", err)
		}
		if payload == nil {
			payload = map[string]any{}
		}
	}
	if envelope != nil {
		payload = AttachExecutionPromptContextEnvelope(payload, envelope)
	}
	if err := walkExecutionPromptContextEnvelope("operator_queue.payload_json", "operator_queue", payload); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode operator_queue prompt context payload_json: %w", err)
	}
	return string(encoded), nil
}

func walkExecutionPromptContextEnvelope(path, recordSurface string, value any) error {
	switch typed := value.(type) {
	case map[string]any:
		if envelope, ok := typed[executionPromptContextEnvelopeKey]; ok {
			if _, ok := envelope.(map[string]any); !ok {
				return fmt.Errorf("%s.%s must be an object", path, executionPromptContextEnvelopeKey)
			}
		}
		if executionPromptContextEnvelopeMapNeedsValidation(path, typed) {
			if err := validateExecutionPromptContextEnvelopeMap(path, recordSurface, typed); err != nil {
				return err
			}
		}
		for key, nested := range typed {
			if err := walkExecutionPromptContextEnvelope(path+"."+strings.TrimSpace(key), recordSurface, nested); err != nil {
				return err
			}
		}
	case []any:
		for idx, nested := range typed {
			if err := walkExecutionPromptContextEnvelope(fmt.Sprintf("%s[%d]", path, idx), recordSurface, nested); err != nil {
				return err
			}
		}
	}
	return nil
}

func executionPromptContextEnvelopeMapNeedsValidation(path string, values map[string]any) bool {
	if executionPromptRawString(values, "contract") == executionPromptContextEnvelopeContract {
		return true
	}
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(path)), "."+executionPromptContextEnvelopeKey)
}

func validateExecutionPromptContextEnvelopeMap(path, recordSurface string, values map[string]any) error {
	expectedContextKind := executionPromptContextKind
	if kind := executionPromptContextKindByRecord[recordSurface]; kind != "" {
		expectedContextKind = kind
	}
	required := map[string]string{
		"contract":                           executionPromptContextEnvelopeContract,
		"context_kind":                       expectedContextKind,
		"authority_model":                    executionPromptContextAuthorityModel,
		"compiler_status":                    executionPromptContextCompilerStatus,
		"daemon_prompt_compiler_convergence": executionPromptContextDaemonClaim,
		"prompt_capability_evidence":         executionPromptContextCapabilityProof,
	}
	for key, want := range required {
		if got := executionPromptRawString(values, key); got != want {
			return fmt.Errorf("%s has invalid %s=%q; expected %q", path, key, got, want)
		}
	}
	for _, key := range []string{"surface", "origin", "workspace_id", "principal_type", "principal_id"} {
		value, ok := values[key]
		if !ok || value == nil {
			return fmt.Errorf("%s missing %s", path, key)
		}
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s has non-string %s; prompt context envelope keys must be canonical strings", path, key)
		}
		if text == "" || strings.TrimSpace(text) != text {
			return fmt.Errorf("%s has invalid %s=%q; prompt context envelope strings must be non-empty and unpadded", path, key, text)
		}
	}
	surface := executionPromptRawString(values, "surface")
	if _, ok := allowedExecutionPromptContextSurfaces[surface]; !ok {
		return fmt.Errorf("%s has unsupported surface=%q", path, surface)
	}
	if allowedForRecord, ok := allowedExecutionPromptContextSurfacesByRecord[recordSurface]; ok {
		if _, ok := allowedForRecord[surface]; !ok {
			return fmt.Errorf("%s has surface=%q that is not valid for %s", path, surface, recordSurface)
		}
	}
	origin := executionPromptRawString(values, "origin")
	if _, ok := allowedExecutionPromptContextOrigins[origin]; !ok {
		return fmt.Errorf("%s has unsupported origin=%q", path, origin)
	}
	if allowedForRecord, ok := allowedExecutionPromptContextOriginsByRecord[recordSurface]; ok {
		if _, ok := allowedForRecord[origin]; !ok {
			return fmt.Errorf("%s has origin=%q that is not valid for %s", path, origin, recordSurface)
		}
	}
	return nil
}
