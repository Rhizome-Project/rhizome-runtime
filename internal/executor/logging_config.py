"""
Structured Logging Configuration for Rhizome Runtime.
Produces JSON log lines with mandatory fields:
  trace_id, task_id, node_id, event, level, ts

Implements NFR-005 from TZ.
"""
import json
import logging
import sys
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional

# Context variables for structured fields
_trace_id: ContextVar[str] = ContextVar("trace_id", default="")
_task_id: ContextVar[str] = ContextVar("task_id", default="")
_node_id: ContextVar[str] = ContextVar("node_id", default="")


class StructuredFormatter(logging.Formatter):
    """JSON log formatter with mandatory Rhizome fields."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
                  + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "component": getattr(record, "component", record.name),
            "event": record.getMessage(),
            "trace_id": getattr(record, "trace_id", _trace_id.get("")),
            "task_id": getattr(record, "task_id", _task_id.get("")),
            "node_id": getattr(record, "node_id", _node_id.get("")),
        }

        # Add extra structured fields
        for key in ("profile", "image", "timeout_sec", "status",
                     "exit_code", "duration_sec", "artifacts_count",
                     "error_code", "container_name",
                     # Lifecycle and watchdog events
                     "found", "killed", "removed", "errors",
                     "tick", "interval", "max_age",
                     "recovery_count", "drained_count", "watchdog_ticks",
                     "watchdog_interval", "watchdog_max_age", "drain_timeout"):
            val = getattr(record, key, None)
            if val is not None:
                entry[key] = val

        return json.dumps(entry, ensure_ascii=False)


def setup_logging(level: int = logging.INFO, stream=None):
    """Configure structured JSON logging for Rhizome runtime.

    Args:
        level: Log level.
        stream: Output stream (default: stderr).
    """
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(StructuredFormatter())

    root = logging.getLogger("rhizome")
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
    root.propagate = False

    return root


def generate_trace_id() -> str:
    """Generate a unique trace ID for a request."""
    return f"tr-{uuid.uuid4().hex[:12]}"


@contextmanager
def execution_context(trace_id: str, task_id: str, node_id: str):
    """Context manager that sets structured log context.

    Usage:
        with execution_context("tr-abc", "task-001", "node-001"):
            logger.info("Starting execution")  # auto-includes trace/task/node
    """
    t1 = _trace_id.set(trace_id)
    t2 = _task_id.set(task_id)
    t3 = _node_id.set(node_id)
    try:
        yield
    finally:
        _trace_id.reset(t1)
        _task_id.reset(t2)
        _node_id.reset(t3)


def get_context() -> dict:
    """Get current logging context."""
    return {
        "trace_id": _trace_id.get(""),
        "task_id": _task_id.get(""),
        "node_id": _node_id.get(""),
    }
