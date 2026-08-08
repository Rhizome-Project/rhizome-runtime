"""
JSON-RPC bridge for the Rhizome executor protocol.
Stdin/stdout bridge between Go orchestrator and Python executor.

Uses frozen runtime contracts for payload serialization.
Integrates structured logging and execution journal.
"""
import json
import logging
import os
import sys
import time
import traceback
from typing import Optional

# Ensure project root is importable when running this file as a script.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from internal.executor.executor_manager import (
    ExecutionResult,
    ExecutionStatus,
    ExecutorManager,
    NodeConfig,
)
from internal.executor.logging_config import (
    execution_context,
    generate_trace_id,
    setup_logging,
)
from internal.executor.recovery import ExecutionJournal, RecoveryManager
from internal.executor.runtime_contracts import (
    ArtifactRef,
    NODE_PROGRESS_CALLBACK_EXCLUSION,
    NodeCompletePayload,
    NodeFailurePayload,
    NodeProgressCallback,
    NodeStartRequest,
)

logger = logging.getLogger("rhizome.rpc_bridge")

# RPC error codes (from contracts-rpc-and-models.md)
ERR_METHOD_NOT_FOUND = -32601
ERR_INVALID_PARAMS = -32602
ERR_INTERNAL = -32603
ERR_EXECUTOR_TIMEOUT = -32017
ERR_EXECUTOR_RUNTIME = -32018


class RPCBridge:
    """JSON-RPC 2.0 bridge for executor operations (v2).

    Uses frozen contracts for type-safe serialization.
    """

    def __init__(
        self,
        executor: Optional[ExecutorManager] = None,
        journal: Optional[ExecutionJournal] = None,
        metrics=None,
    ):
        workspace_root = os.getenv("RHIZOME_WORKSPACE_ROOT", "./data/workspace")
        self.executor = executor or ExecutorManager(workspace_root=workspace_root)
        self.journal = journal or ExecutionJournal()
        self.metrics = metrics  # Optional MetricsCollector

    def handle_request(self, payload: dict) -> dict:
        """Process a single JSON-RPC request."""
        req_id = payload.get("id")
        method = payload.get("method", "")
        params = payload.get("params", {})
        if params is None:
            params = {}

        if payload.get("jsonrpc") != "2.0":
            return self._error_response(req_id, ERR_INVALID_PARAMS, "invalid_jsonrpc_version")
        if not isinstance(params, dict):
            return self._error_response(req_id, ERR_INVALID_PARAMS, "params must be an object")

        try:
            if method == "executor.run_node":
                return self._handle_run_node(req_id, params)
            elif method == "executor.kill_node":
                return self._handle_kill_node(req_id, params)
            elif method == "executor.status":
                return self._handle_status(req_id)
            elif method == "executor.recover":
                return self._handle_recover(req_id)
            elif method == "node.report_progress":
                return self._handle_progress(req_id, params)
            else:
                return self._error_response(req_id, ERR_METHOD_NOT_FOUND, f"method_not_found: {method}")
        except Exception as e:
            logger.error(f"RPC handler error: {e}\n{traceback.format_exc()}")
            return self._error_response(req_id, ERR_INTERNAL, str(e))

    def _handle_run_node(self, req_id, params: dict) -> dict:
        """Handle executor.run_node using frozen NodeStartRequest."""
        required = ["task_id", "node_id", "runtime_profile", "script_ref"]
        missing = [f for f in required if f not in params]
        if missing:
            return self._error_response(
                req_id, ERR_INVALID_PARAMS,
                f"missing_required_params: {', '.join(missing)}"
            )

        # Parse via frozen contract
        start_req = NodeStartRequest.from_params(params)
        trace_id = start_req.trace_id or generate_trace_id()

        # Set structured logging context
        with execution_context(trace_id, start_req.task_id, start_req.node_id):
            logger.info("Received executor.run_node request")

            config = NodeConfig(
                task_id=start_req.task_id,
                node_id=start_req.node_id,
                runtime_profile=start_req.runtime_profile,
                script_ref=start_req.script_ref,
                timeout_sec=start_req.timeout_sec,
                env=dict(start_req.env),
                cpus=start_req.cpus,
                memory=start_req.memory,
            )

            # Record in journal
            container_name = f"rhizome-{config.task_id}-{config.node_id}"
            self.journal.record_start(
                config.task_id, config.node_id, container_name, trace_id,
            )

            result = self.executor.run_node(config)

            # Record completion in journal
            status = "COMPLETED" if result.status == ExecutionStatus.SUCCESS else "FAILED"
            self.journal.record_complete(config.task_id, config.node_id, result.exit_code, status)

            # Record metrics if collector available
            if self.metrics:
                status_str = result.status.value.upper()  # SUCCESS, FAILED, TIMEOUT
                startup_ms = max(0, (result.duration_sec - 0.1)) * 1000 if result.duration_sec else 0
                self.metrics.record_execution(
                    profile=start_req.runtime_profile,
                    status=status_str,
                    duration_sec=result.duration_sec,
                    startup_ms=startup_ms,
                )

            return self._result_to_response(req_id, result, trace_id)

    def _handle_kill_node(self, req_id, params: dict) -> dict:
        task_id = params.get("task_id")
        node_id = params.get("node_id")
        if not task_id or not node_id:
            return self._error_response(req_id, ERR_INVALID_PARAMS, "missing task_id or node_id")
        killed = self.executor.kill_node(task_id, node_id)
        return self._success_response(req_id, {"killed": killed})

    def _handle_status(self, req_id) -> dict:
        in_flight = self.journal.get_in_flight()
        return self._success_response(req_id, {
            "status": "ready",
            "in_flight_count": len(in_flight),
        })

    def _handle_recover(self, req_id) -> dict:
        """Handle executor.recover — restart recovery."""
        recovery = RecoveryManager(self.journal)
        actions = recovery.recover()
        return self._success_response(req_id, {
            "recovered": len(actions),
            "actions": actions,
        })

    def _handle_progress(self, req_id, params: dict) -> dict:
        """Handle node.report_progress using frozen contract."""
        try:
            progress = NodeProgressCallback.from_params(params)
            logger.info(
                f"Progress: {progress.progress:.0%} - {progress.message}",
                extra={"task_id": progress.task_id, "node_id": progress.node_id},
            )
            return self._success_response(req_id, {
                "accepted": True,
                "classification": NODE_PROGRESS_CALLBACK_EXCLUSION,
            })
        except (KeyError, ValueError) as e:
            return self._error_response(req_id, ERR_INVALID_PARAMS, str(e))

    def _result_to_response(self, req_id, result: ExecutionResult, trace_id: str) -> dict:
        """Convert ExecutionResult using frozen contract types."""
        if result.status == ExecutionStatus.SUCCESS:
            payload = NodeCompletePayload(
                task_id=result.task_id,
                node_id=result.node_id,
                exit_code=result.exit_code,
                duration_sec=result.duration_sec,
                stdout=result.output.stdout if result.output else "",
                stderr=result.output.stderr if result.output else "",
                artifacts=[
                    ArtifactRef(path=a.path, size_bytes=a.size_bytes, content_type=a.content_type)
                    for a in (result.output.artifacts if result.output else [])
                ],
                metrics=result.output.metrics if result.output else {},
                trace_id=trace_id,
            )
            return self._success_response(req_id, payload.to_rpc_result())
        else:
            payload = NodeFailurePayload(
                task_id=result.task_id,
                node_id=result.node_id,
                status=result.status.value,
                error_code=result.error_code or "executor_runtime_error",
                error_message=result.error_message or "execution_failed",
                exit_code=result.exit_code,
                duration_sec=result.duration_sec,
                stderr=result.output.stderr[:5000] if result.output and result.output.stderr else "",
                trace_id=trace_id,
            )
            return {
                "jsonrpc": "2.0",
                "error": payload.to_rpc_error(),
                "id": req_id,
            }

    @staticmethod
    def _success_response(req_id, result: dict) -> dict:
        return {"jsonrpc": "2.0", "result": result, "id": req_id}

    @staticmethod
    def _error_response(req_id, code: int, message: str, details: Optional[dict] = None) -> dict:
        error = {"code": code, "message": message}
        if details:
            error["details"] = details
        return {"jsonrpc": "2.0", "error": error, "id": req_id}


def main():
    """Main entry point: read JSON-RPC from stdin, write response to stdout."""
    setup_logging()
    bridge = RPCBridge()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            payload = json.loads(line)
        except json.JSONDecodeError as e:
            response = RPCBridge._error_response(None, ERR_INVALID_PARAMS, f"invalid_json: {e}")
            try:
                out_str = json.dumps(response) + "\n"
            except Exception as se:
                fallback = RPCBridge._error_response(None, ERR_INTERNAL, f"bridge_serialization_error: {se}")
                out_str = json.dumps(fallback) + "\n"

            sys.stdout.write(out_str)
            sys.stdout.flush()
            continue

        response = bridge.handle_request(payload)
        try:
            out_str = json.dumps(response) + "\n"
        except Exception as e:
            fallback = RPCBridge._error_response(
                payload.get("id"), ERR_INTERNAL, f"bridge_serialization_error: {e}"
            )
            out_str = json.dumps(fallback) + "\n"

        sys.stdout.write(out_str)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
