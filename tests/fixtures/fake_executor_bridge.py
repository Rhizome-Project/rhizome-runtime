"""
Fake JSON-RPC executor bridge for Go CLI integration tests.
Reads one request from stdin and emits deterministic success/error response.
"""
import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from internal.executor.metrics import MetricsCollector


def persist_metrics(status: str) -> None:
    metrics_path = os.environ.get("RHIZOME_METRICS_PATH", "").strip()
    if not metrics_path:
        return

    collector = MetricsCollector(persist_path=metrics_path)
    collector.record_execution("generic", status, duration_sec=0.1, startup_ms=25.0)
    collector.snapshot()


def main():
    line = sys.stdin.readline()
    if not line:
        return

    req = json.loads(line)
    req_id = req.get("id")
    params = req.get("params", {})
    mode = params.get("env", {}).get("FAKE_RUNTIME_MODE", "success")

    if mode == "error":
        persist_metrics("FAILED")
        response = {
            "jsonrpc": "2.0",
            "error": {
                "code": -32018,
                "message": "executor_runtime_error",
                "details": {
                    "status": "FAILED",
                    "exit_code": 1,
                    "duration_sec": 0.1,
                    "error_message": "fake runtime failure",
                    "stderr": "fake stderr",
                    "trace_id": params.get("trace_id", ""),
                },
            },
            "id": req_id,
        }
    else:
        persist_metrics("SUCCESS")
        response = {
            "jsonrpc": "2.0",
            "result": {
                "status": "SUCCESS",
                "exit_code": 0,
                "duration_sec": 0.1,
                "stdout": "fake bridge ok",
                "stderr": "",
                "artifacts": [],
                "metrics": {"fake": True},
                "trace_id": params.get("trace_id", ""),
            },
            "id": req_id,
        }

    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
