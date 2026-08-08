"""
Recovery Manager for Rhizome Executor.
Persists in-flight execution metadata and handles restart recovery.

Implements AT-007: after restart, in-flight nodes are either
resumed (if container still running) or safely failed.
"""
import json
import subprocess
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

from internal.executor.logging_config import generate_trace_id


class JournalStatus(Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RECOVERED = "RECOVERED"


@dataclass
class JournalEntry:
    """Record of an in-flight execution."""
    task_id: str
    node_id: str
    container_name: str
    trace_id: str
    started_at: float            # time.time()
    status: str = "STARTED"      # JournalStatus value
    completed_at: Optional[float] = None
    exit_code: Optional[int] = None
    recovery_action: Optional[str] = None  # "resumed" | "failed" | "collected"


class ExecutionJournal:
    """Append-only JSONL journal for tracking in-flight executions.

    Journal file persists across restarts, enabling recovery.
    """

    def __init__(self, journal_path: str = "./data/execution_journal.jsonl"):
        self.path = Path(journal_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record_start(self, task_id: str, node_id: str,
                     container_name: str, trace_id: str = "") -> JournalEntry:
        """Record that a node execution has started."""
        entry = JournalEntry(
            task_id=task_id,
            node_id=node_id,
            container_name=container_name,
            trace_id=trace_id or generate_trace_id(),
            started_at=time.time(),
            status=JournalStatus.STARTED.value,
        )
        self._append(entry)
        return entry

    def record_complete(self, task_id: str, node_id: str,
                        exit_code: int, status: str = "COMPLETED") -> JournalEntry:
        """Record that a node execution has completed."""
        normalized_status = self._normalize_completion_status(status, exit_code)
        entry = JournalEntry(
            task_id=task_id,
            node_id=node_id,
            container_name="",
            trace_id="",
            started_at=0,
            status=normalized_status,
            completed_at=time.time(),
            exit_code=exit_code,
        )
        self._append(entry)
        return entry

    def record_recovery(self, task_id: str, node_id: str,
                        action: str, exit_code: int = -1) -> JournalEntry:
        """Record a recovery action for an in-flight node."""
        entry = JournalEntry(
            task_id=task_id,
            node_id=node_id,
            container_name="",
            trace_id="",
            started_at=0,
            status=JournalStatus.RECOVERED.value,
            completed_at=time.time(),
            exit_code=exit_code,
            recovery_action=action,
        )
        self._append(entry)
        return entry

    def get_in_flight(self) -> list[JournalEntry]:
        """Get all entries that were STARTED but not COMPLETED/FAILED/RECOVERED."""
        all_entries = self._read_all()

        # Build sets of completed task/node pairs
        completed = set()
        for e in all_entries:
            if e.status in (JournalStatus.COMPLETED.value,
                            JournalStatus.FAILED.value,
                            JournalStatus.RECOVERED.value):
                completed.add((e.task_id, e.node_id))

        # Return STARTED entries without completion
        in_flight = []
        seen = set()
        for e in all_entries:
            key = (e.task_id, e.node_id)
            if e.status == JournalStatus.STARTED.value and key not in completed and key not in seen:
                in_flight.append(e)
                seen.add(key)

        return in_flight

    def _append(self, entry: JournalEntry):
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(entry)) + "\n")

    @staticmethod
    def _normalize_completion_status(status: str, exit_code: int) -> str:
        normalized = (status or "").strip().upper()
        if normalized in {
            JournalStatus.COMPLETED.value,
            JournalStatus.FAILED.value,
            JournalStatus.RECOVERED.value,
        }:
            return normalized
        if normalized == "SUCCESS":
            return JournalStatus.COMPLETED.value
        if normalized in {"TIMEOUT", "BUILD_ERROR", "RUNTIME_ERROR", "ERROR"}:
            return JournalStatus.FAILED.value
        return JournalStatus.COMPLETED.value if exit_code == 0 else JournalStatus.FAILED.value

    def _read_all(self) -> list[JournalEntry]:
        if not self.path.exists():
            return []
        entries = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    data = json.loads(line)
                    entries.append(JournalEntry(**data))
        return entries


class RecoveryManager:
    """Handles restart recovery for in-flight executions.

    Recovery logic:
    1. Read journal for STARTED but not completed entries.
    2. For each: docker inspect container.
    3. If running: wait for it, collect output → COMPLETED/FAILED.
    4. If exited: collect exit code → COMPLETED/FAILED.
    5. If not found: mark as FAILED (container_lost).
    """

    def __init__(self, journal: ExecutionJournal):
        self.journal = journal

    def recover(self) -> list[dict]:
        """Recover all in-flight executions after restart.

        Returns:
            List of recovery actions taken.
        """
        in_flight = self.journal.get_in_flight()
        if not in_flight:
            return []

        actions = []
        for entry in in_flight:
            action = self._recover_entry(entry)
            actions.append(action)

        return actions

    def _recover_entry(self, entry: JournalEntry) -> dict:
        """Recover a single in-flight entry."""
        container = entry.container_name
        result = {
            "task_id": entry.task_id,
            "node_id": entry.node_id,
            "container": container,
            "action": "unknown",
            "exit_code": -1,
        }

        # 1. Check if container exists
        status = self._inspect_container(container)

        if status is None:
            # Container not found — mark as failed
            result["action"] = "failed:container_lost"
            self.journal.record_recovery(
                entry.task_id, entry.node_id,
                action="failed:container_lost", exit_code=-1,
            )

        elif status == "running":
            # Container still running — wait for it (bounded)
            exit_code = self._wait_container(container, timeout=30)
            if exit_code is not None:
                result["action"] = "collected" if exit_code == 0 else "failed:collected"
                result["exit_code"] = exit_code
                self.journal.record_recovery(
                    entry.task_id, entry.node_id,
                    action=result["action"], exit_code=exit_code,
                )
            else:
                # Still running after wait — kill it
                self._kill_container(container)
                result["action"] = "failed:killed_after_recovery"
                self.journal.record_recovery(
                    entry.task_id, entry.node_id,
                    action="failed:killed_after_recovery", exit_code=-1,
                )

        elif status == "exited":
            # Container exited — collect exit code
            exit_code = self._get_exit_code(container)
            result["action"] = "collected" if exit_code == 0 else "failed:collected"
            result["exit_code"] = exit_code or -1
            self.journal.record_recovery(
                entry.task_id, entry.node_id,
                action=result["action"], exit_code=exit_code or -1,
            )
            # Clean up exited container
            self._rm_container(container)

        else:
            # Unknown state
            result["action"] = f"failed:unknown_state:{status}"
            self.journal.record_recovery(
                entry.task_id, entry.node_id,
                action=result["action"], exit_code=-1,
            )

        return result

    def _inspect_container(self, name: str) -> Optional[str]:
        """Get container status: 'running', 'exited', or None if not found."""
        try:
            proc = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Status}}", name],
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                timeout=5,
            )
            if proc.returncode == 0:
                return proc.stdout.strip()
            return None
        except Exception:
            return None

    def _wait_container(self, name: str, timeout: int = 30) -> Optional[int]:
        """Wait for container to exit, return exit code or None if timeout."""
        try:
            proc = subprocess.run(
                ["docker", "wait", name],
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                timeout=timeout,
            )
            if proc.returncode == 0:
                return int(proc.stdout.strip())
            return None
        except (subprocess.TimeoutExpired, ValueError):
            return None

    def _get_exit_code(self, name: str) -> Optional[int]:
        """Get exit code of a stopped container."""
        try:
            proc = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.ExitCode}}", name],
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                timeout=5,
            )
            if proc.returncode == 0:
                return int(proc.stdout.strip())
            return None
        except Exception:
            return None

    def _kill_container(self, name: str):
        try:
            subprocess.run(["docker", "kill", name], capture_output=True, timeout=5)
        except Exception:
            pass
        self._rm_container(name)

    def _rm_container(self, name: str):
        try:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=5)
        except Exception:
            pass
