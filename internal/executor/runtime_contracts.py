"""
Frozen Runtime Bridge Contracts for Rhizome.
Defines the exact JSON-RPC payload shapes exchanged between
Go orchestrator and Python executor.

Version: 1.0.0 — FROZEN for MVP
Do not modify field names or types without Sync checkpoint.
"""
import json
import math
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional


# ── Enums ────────────────────────────────────────────────────────────

class RuntimeProfile(Enum):
    COMPUTE = "compute"
    BROWSER = "browser_automation"


class ExecutionOutcome(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    BUILD_ERROR = "BUILD_ERROR"


class ErrorCode(Enum):
    """Maps to contracts-rpc-and-models.md error catalog."""
    EXECUTOR_TIMEOUT = "executor_timeout"           # -32017
    EXECUTOR_RUNTIME_ERROR = "executor_runtime_error"  # -32018
    STATE_TRANSITION_INVALID = "state_transition_invalid"  # -32019

    @property
    def rpc_code(self) -> int:
        return _ERROR_RPC_CODES[self]


_ERROR_RPC_CODES = {
    ErrorCode.EXECUTOR_TIMEOUT: -32017,
    ErrorCode.EXECUTOR_RUNTIME_ERROR: -32018,
    ErrorCode.STATE_TRANSITION_INVALID: -32019,
}


# ── Request Contracts ────────────────────────────────────────────────

@dataclass(frozen=True)
class NodeStartRequest:
    """Frozen contract: request to start a node execution.
    Go orchestrator → Python executor via executor.run_node.

    Required fields match validator.go required params for node.request_execution.
    """
    task_id: str
    node_id: str
    runtime_profile: str         # "compute" | "browser_automation"
    script_ref: str              # Path to script inside container
    timeout_sec: int = 300
    env: dict[str, str] = field(default_factory=dict)
    cpus: str = "1.0"
    memory: str = "512m"
    trace_id: str = ""           # Set by caller or auto-generated

    def to_rpc(self, request_id: Optional[str] = None) -> dict:
        """Serialize to JSON-RPC 2.0 request."""
        rid = request_id or f"exec-{uuid.uuid4().hex[:8]}"
        return {
            "jsonrpc": "2.0",
            "method": "executor.run_node",
            "params": {
                "task_id": self.task_id,
                "node_id": self.node_id,
                "runtime_profile": self.runtime_profile,
                "script_ref": self.script_ref,
                "timeout_sec": self.timeout_sec,
                "env": dict(self.env),
                "cpus": self.cpus,
                "memory": self.memory,
                "trace_id": self.trace_id,
            },
            "id": rid,
        }

    @classmethod
    def from_params(cls, params: dict) -> "NodeStartRequest":
        """Deserialize from JSON-RPC params dict."""
        return cls(
            task_id=str(params["task_id"]),
            node_id=str(params["node_id"]),
            runtime_profile=str(params["runtime_profile"]),
            script_ref=str(params["script_ref"]),
            timeout_sec=int(params.get("timeout_sec", 300)),
            env=dict(params.get("env", {})),
            cpus=str(params.get("cpus", "1.0")),
            memory=str(params.get("memory", "512m")),
            trace_id=str(params.get("trace_id", "")),
        )


@dataclass(frozen=True)
class NodeProgressCallback:
    """Frozen contract: progress update from executor to orchestrator.
    Python executor → Go orchestrator via node.report_progress.

    This is a log-only callback. It is not durable node lifecycle evidence
    and must not be counted as prompt-compiler convergence for agent.node.*.
    """
    task_id: str
    node_id: str
    progress: float              # 0.0 .. 1.0
    message: str = ""
    trace_id: str = ""

    def to_rpc(self, request_id: Optional[str] = None) -> dict:
        rid = request_id or f"prog-{uuid.uuid4().hex[:8]}"
        return {
            "jsonrpc": "2.0",
            "method": "node.report_progress",
            "params": {
                "task_id": self.task_id,
                "node_id": self.node_id,
                "progress": self.progress,
                "message": self.message,
                "trace_id": self.trace_id,
            },
            "id": rid,
        }

    @classmethod
    def from_params(cls, params: dict) -> "NodeProgressCallback":
        raw_task_id = params["task_id"]
        raw_node_id = params["node_id"]
        raw_progress = params["progress"]
        if not isinstance(raw_task_id, str):
            raise ValueError("task_id must be a string")
        if not isinstance(raw_node_id, str):
            raise ValueError("node_id must be a string")
        if isinstance(raw_progress, bool) or not isinstance(raw_progress, (int, float)):
            raise ValueError("progress must be a number")
        task_id = raw_task_id.strip()
        node_id = raw_node_id.strip()
        progress = float(raw_progress)
        if not task_id:
            raise ValueError("task_id is required")
        if not node_id:
            raise ValueError("node_id is required")
        if not math.isfinite(progress) or progress < 0.0 or progress > 1.0:
            raise ValueError("progress must be a finite number between 0.0 and 1.0")
        return cls(
            task_id=task_id,
            node_id=node_id,
            progress=progress,
            message=str(params.get("message", "")),
            trace_id=str(params.get("trace_id", "")),
        )

NODE_PROGRESS_CALLBACK_EXCLUSION = {
    "contract": "executor_progress_callback_exclusion.v1",
    "surface": "node.report_progress",
    "classification": "log_only_non_authority_bearing",
    "durable_runtime_event": False,
    "prompt_context_envelope_required": False,
    "accepted_as_agent_node_lifecycle_evidence": False,
}


# ── Response Contracts ───────────────────────────────────────────────

@dataclass
class ArtifactRef:
    """Reference to a collected artifact."""
    path: str
    size_bytes: int
    content_type: str            # json, png, text, other


@dataclass
class NodeCompletePayload:
    """Frozen contract: successful execution result.
    Returned as JSON-RPC result for executor.run_node.
    """
    task_id: str
    node_id: str
    status: str = "SUCCESS"      # Always "SUCCESS"
    exit_code: int = 0
    duration_sec: float = 0.0
    stdout: str = ""
    stderr: str = ""
    artifacts: list[ArtifactRef] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    trace_id: str = ""

    def to_rpc_result(self) -> dict:
        return {
            "status": self.status,
            "exit_code": self.exit_code,
            "duration_sec": round(self.duration_sec, 2),
            "stdout": self.stdout[:10000],
            "stderr": self.stderr[:10000],
            "artifacts": [
                {"path": a.path, "size_bytes": a.size_bytes, "content_type": a.content_type}
                for a in self.artifacts
            ],
            "metrics": self.metrics,
            "trace_id": self.trace_id,
        }


@dataclass
class NodeFailurePayload:
    """Frozen contract: failed/timeout execution result.
    Returned as JSON-RPC error for executor.run_node.
    """
    task_id: str
    node_id: str
    status: str                  # "FAILED" | "TIMEOUT" | "BUILD_ERROR"
    error_code: str              # "executor_timeout" | "executor_runtime_error"
    error_message: str
    exit_code: int = -1
    duration_sec: float = 0.0
    stderr: str = ""
    trace_id: str = ""

    @property
    def rpc_error_code(self) -> int:
        """Map to JSON-RPC numeric error code."""
        mapping = {
            "executor_timeout": -32017,
            "executor_runtime_error": -32018,
        }
        return mapping.get(self.error_code, -32603)

    def to_rpc_error(self) -> dict:
        return {
            "code": self.rpc_error_code,
            "message": self.error_code,
            "details": {
                "status": self.status,
                "exit_code": self.exit_code,
                "duration_sec": round(self.duration_sec, 2),
                "error_message": self.error_message,
                "stderr": self.stderr[:5000],
                "trace_id": self.trace_id,
            },
        }


# ── Taxonomy ─────────────────────────────────────────────────────────

ERROR_TAXONOMY = {
    "executor_timeout": {
        "rpc_code": -32017,
        "description": "Container killed after timeout exceeded",
        "retryable": True,
    },
    "executor_runtime_error": {
        "rpc_code": -32018,
        "description": "Container exited with non-zero code",
        "retryable": True,
    },
    "state_transition_invalid": {
        "rpc_code": -32019,
        "description": "Invalid node state transition attempted",
        "retryable": False,
    },
    "policy_denied": {
        "rpc_code": -32010,
        "description": "FinOps policy denied the resource request",
        "retryable": False,
    },
    "budget_exceeded": {
        "rpc_code": -32011,
        "description": "Daily or per-task budget limit exceeded",
        "retryable": False,
    },
    "quota_exceeded": {
        "rpc_code": -32012,
        "description": "RPM or TPM quota exceeded",
        "retryable": True,
    },
    "approval_timeout": {
        "rpc_code": -32014,
        "description": "Manual approval TTL expired",
        "retryable": False,
    },
    "approval_rejected": {
        "rpc_code": -32015,
        "description": "Operator rejected the approval request",
        "retryable": False,
    },
    "feature_disabled": {
        "rpc_code": -32020,
        "description": "Feature flag is disabled",
        "retryable": False,
    },
}
