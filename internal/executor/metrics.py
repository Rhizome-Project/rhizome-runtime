"""
Runtime Metrics Snapshot for Rhizome Executor.
Collects and reports performance metrics.
"""
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class ProfileMetrics:
    """Metrics for a specific execution profile."""
    profile: str
    total_runs: int = 0
    success_count: int = 0
    failure_count: int = 0
    timeout_count: int = 0
    total_duration_sec: float = 0.0
    min_startup_ms: float = float("inf")
    max_startup_ms: float = 0.0
    sum_startup_ms: float = 0.0

    @property
    def failure_rate(self) -> float:
        if self.total_runs == 0:
            return 0.0
        return (self.failure_count + self.timeout_count) / self.total_runs

    @property
    def avg_startup_ms(self) -> float:
        if self.total_runs == 0:
            return 0.0
        return self.sum_startup_ms / self.total_runs

    @property
    def avg_duration_sec(self) -> float:
        if self.total_runs == 0:
            return 0.0
        return self.total_duration_sec / self.total_runs


@dataclass
class RecoveryMetrics:
    """Metrics for recovery operations."""
    total_recoveries: int = 0
    successful: int = 0
    failed: int = 0
    avg_recovery_time_sec: float = 0.0
    total_recovery_time_sec: float = 0.0


@dataclass
class MetricsSnapshot:
    """Complete runtime metrics snapshot."""
    schema_version: str = "1.0"
    timestamp: str = ""
    profiles: dict[str, ProfileMetrics] = field(default_factory=dict)
    recovery: RecoveryMetrics = field(default_factory=RecoveryMetrics)
    orphan_containers_cleaned: int = 0
    total_disk_usage_bytes: int = 0


class MetricsCollector:
    """Collects runtime performance metrics.

    Usage:
        collector = MetricsCollector()
        collector.record_execution("compute", "SUCCESS", duration_sec=4.5, startup_ms=650)
        snapshot = collector.snapshot()
    """

    def __init__(self, persist_path: Optional[str] = None):
        self._profiles: dict[str, ProfileMetrics] = {}
        self._recovery = RecoveryMetrics()
        self._orphans_cleaned = 0
        self._persist_path = persist_path

    def record_execution(
        self,
        profile: str,
        status: str,       # SUCCESS, FAILED, TIMEOUT
        duration_sec: float,
        startup_ms: float = 0.0,
    ):
        """Record a single execution's metrics."""
        if profile not in self._profiles:
            self._profiles[profile] = ProfileMetrics(profile=profile)

        m = self._profiles[profile]
        m.total_runs += 1
        m.total_duration_sec += duration_sec

        if status == "SUCCESS":
            m.success_count += 1
        elif status == "TIMEOUT":
            m.timeout_count += 1
        else:
            m.failure_count += 1

        if startup_ms > 0:
            m.min_startup_ms = min(m.min_startup_ms, startup_ms)
            m.max_startup_ms = max(m.max_startup_ms, startup_ms)
            m.sum_startup_ms += startup_ms

    def record_recovery(self, success: bool, duration_sec: float):
        """Record a recovery operation."""
        self._recovery.total_recoveries += 1
        self._recovery.total_recovery_time_sec += duration_sec
        if success:
            self._recovery.successful += 1
        else:
            self._recovery.failed += 1
        if self._recovery.total_recoveries > 0:
            self._recovery.avg_recovery_time_sec = (
                self._recovery.total_recovery_time_sec /
                self._recovery.total_recoveries
            )

    def record_cleanup(self, count: int):
        """Record orphan container cleanup."""
        self._orphans_cleaned += count

    def snapshot(self) -> dict:
        """Take a metrics snapshot and return as dict."""
        snap = {
            "schema_version": "1.0",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "profiles": {},
            "recovery": {
                "total_recoveries": self._recovery.total_recoveries,
                "successful": self._recovery.successful,
                "failed": self._recovery.failed,
                "avg_recovery_time_sec": round(self._recovery.avg_recovery_time_sec, 3),
            },
            "orphan_containers_cleaned": self._orphans_cleaned,
        }

        for name, m in self._profiles.items():
            snap["profiles"][name] = {
                "total_runs": m.total_runs,
                "success_count": m.success_count,
                "failure_count": m.failure_count,
                "timeout_count": m.timeout_count,
                "failure_rate": round(m.failure_rate, 4),
                "avg_duration_sec": round(m.avg_duration_sec, 2),
                "avg_startup_ms": round(m.avg_startup_ms, 1),
                "min_startup_ms": round(m.min_startup_ms, 1) if m.min_startup_ms != float("inf") else 0,
                "max_startup_ms": round(m.max_startup_ms, 1),
            }

        if self._persist_path:
            self._persist(snap)

        return snap

    def _persist(self, snap: dict):
        """Append snapshot to JSONL file with bounded log rotation."""
        try:
            path = Path(self._persist_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            # Rotation block: bounded up to 512KB
            if path.exists():
                stat_res = path.stat()
                if stat_res.st_size > 512 * 1024:
                    backup = path.with_suffix(".jsonl.bak")
                    if backup.exists():
                        backup.unlink()  # keep only 1 rotation
                    path.rename(backup)

            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(snap) + "\n")
        except Exception:
            pass
