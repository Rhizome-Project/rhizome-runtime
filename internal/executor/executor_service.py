"""
Historical standalone executor harness notice:
This module is not accepted as first-deployment or production executor evidence.
The supported first-deployment path is the Go daemon Runner/StdioRuntimeClient,
which records a durable `executor_run_node` ACTIVE operation ledger before
launching the Python bridge.

Rhizome Executor Service - historical standalone executor harness.

Wires together all runtime components:
- RPCBridge: JSON-RPC stdin/stdout loop
- GracefulShutdown: SIGTERM/SIGINT → drain
- RecoveryManager: startup recovery of in-flight nodes
- OrphanCleaner: startup + periodic + shutdown cleanup
- Watchdog: background thread for periodic orphan cleanup
- Structured logging: JSON with lifecycle events

Lifecycle events emitted:
  runtime_starting     — process boot
  runtime_ready        — accepting requests
  watchdog_tick        — periodic cleanup started
  watchdog_cleanup     — cleanup result (killed/removed/errors)
  runtime_draining     — signal received, draining
  runtime_stopped      — process exit

Exit codes:
  0 — clean shutdown (stdin EOF)
  1 — error
  2 — signal received, drained successfully

Env vars:
  RHIZOME_WORKSPACE_ROOT    — workspace path (default: ./data/workspace)
  RHIZOME_JOURNAL_PATH      — journal path (default: ./data/execution_journal.jsonl)
  RHIZOME_WATCHDOG_INTERVAL — cleanup interval in seconds (default: 300)
  RHIZOME_WATCHDOG_MAX_AGE  — max container age in seconds (default: 600)
  RHIZOME_DRAIN_TIMEOUT     — drain timeout in seconds (default: 30)

Historical manual use only:
  python -m internal.executor.executor_service
"""
import json
import logging
import os
import sys
import threading
import time

from internal.executor.cleanup import OrphanCleaner
from internal.executor.executor_manager import ExecutorManager
from internal.executor.logging_config import setup_logging
from internal.executor.metrics import MetricsCollector
from internal.executor.recovery import ExecutionJournal, RecoveryManager
from internal.executor.retention import WorkspaceGC, RetentionPolicy
from internal.executor.rpc_bridge import RPCBridge, ERR_INVALID_PARAMS
from internal.executor.shutdown import GracefulShutdown

logger = logging.getLogger("rhizome.service")

# Defaults (overridable via env vars)
DEFAULT_JOURNAL_PATH = "./data/execution_journal.jsonl"
DEFAULT_WORKSPACE = "./data/workspace"
DEFAULT_DRAIN_TIMEOUT = 30
DEFAULT_WATCHDOG_INTERVAL = 300  # 5 minutes
DEFAULT_WATCHDOG_MAX_AGE = 600   # 10 minutes
DEFAULT_METRICS_PATH = "./data/metrics.jsonl"
DEFAULT_RETENTION_MAX_AGE = 86400  # 24 hours


def _emit_event(event: str, **kwargs):
    """Emit a structured lifecycle event."""
    extra = {"event": event, "component": "executor_service"}
    extra.update(kwargs)
    logger.info(event, extra=extra)


class WatchdogThread:
    """Periodic orphan container cleanup in a background thread.

    Runs OrphanCleaner.cleanup() every `interval` seconds.
    Emits structured events: watchdog_tick, watchdog_cleanup.
    Stops cleanly via stop_event.
    """

    def __init__(self, cleaner: OrphanCleaner, interval: int, stop_event: threading.Event):
        self.cleaner = cleaner
        self.interval = interval
        self.stop_event = stop_event
        self._thread = threading.Thread(
            target=self._run, name="rhizome-watchdog", daemon=True,
        )
        self._tick_count = 0

    def start(self):
        self._thread.start()
        _emit_event("watchdog_started", interval=self.interval,
                     max_age=self.cleaner.max_age_seconds)

    def join(self, timeout: float = 5.0):
        """Wait for watchdog thread to stop. Safe to call even if not started."""
        if self._thread.is_alive():
            self._thread.join(timeout=timeout)

    @property
    def tick_count(self) -> int:
        return self._tick_count

    def _run(self):
        while not self.stop_event.is_set():
            # Wait for interval or stop signal
            self.stop_event.wait(self.interval)
            if self.stop_event.is_set():
                break

            self._tick_count += 1
            _emit_event("watchdog_tick", tick=self._tick_count)

            try:
                report = self.cleaner.cleanup()
                _emit_event("watchdog_cleanup",
                             tick=self._tick_count,
                             found=report.found,
                             killed=report.killed,
                             removed=report.removed,
                             errors=len(report.errors))
            except Exception as e:
                logger.error(f"Watchdog cleanup error: {e}",
                             extra={"event": "watchdog_error", "error": str(e)})


def run(
    journal_path: str | None = None,
    workspace: str | None = None,
    drain_timeout: int | None = None,
    watchdog_interval: int | None = None,
    watchdog_max_age: int | None = None,
    metrics_path: str | None = None,
) -> int:
    """Run the executor service.

    All parameters can be overridden by env vars.

    Returns:
        Exit code: 0=clean, 1=error, 2=signal-drained.
    """
    # Resolve config from args → env → defaults
    journal_path = journal_path or os.getenv("RHIZOME_JOURNAL_PATH", DEFAULT_JOURNAL_PATH)
    workspace = workspace or os.getenv("RHIZOME_WORKSPACE_ROOT", DEFAULT_WORKSPACE)
    drain_timeout = drain_timeout or int(os.getenv("RHIZOME_DRAIN_TIMEOUT", str(DEFAULT_DRAIN_TIMEOUT)))
    watchdog_interval = watchdog_interval or int(os.getenv("RHIZOME_WATCHDOG_INTERVAL", str(DEFAULT_WATCHDOG_INTERVAL)))
    watchdog_max_age = watchdog_max_age or int(os.getenv("RHIZOME_WATCHDOG_MAX_AGE", str(DEFAULT_WATCHDOG_MAX_AGE)))
    metrics_path = metrics_path or os.getenv("RHIZOME_METRICS_PATH", DEFAULT_METRICS_PATH)

    # 1. Setup structured logging
    setup_logging()
    _emit_event("runtime_starting",
                watchdog_interval=watchdog_interval,
                watchdog_max_age=watchdog_max_age,
                drain_timeout=drain_timeout)

    # 2. Initialize components
    journal = ExecutionJournal(journal_path)
    executor = ExecutorManager(workspace_root=workspace)
    metrics = MetricsCollector(persist_path=metrics_path)
    bridge = RPCBridge(executor=executor, journal=journal, metrics=metrics)
    shutdown = GracefulShutdown(drain_timeout=drain_timeout)
    cleaner = OrphanCleaner(max_age_seconds=watchdog_max_age)

    retention_max_age = int(os.environ.get("RHIZOME_RETENTION_MAX_AGE", DEFAULT_RETENTION_MAX_AGE))
    gc = WorkspaceGC(
        workspace_root=workspace,
        policy=RetentionPolicy(max_age_seconds=retention_max_age),
    )

    # Shared stop event — coordinates shutdown + watchdog
    stop_event = threading.Event()

    # 3. Install signal handlers (sets shutdown.should_stop on signal)
    shutdown.install()

    # 4. Startup recovery — handle in-flight from previous crash
    recovery = RecoveryManager(journal)
    recovery_actions = recovery.recover()
    if recovery_actions:
        _emit_event("startup_recovery", recovery_count=len(recovery_actions))
        for action in recovery_actions:
            logger.info(f"  Recovered {action['task_id']}/{action['node_id']}: {action['action']}")
            metrics.record_recovery(
                success="failed" not in action.get("action", ""),
                duration_sec=0.0,
            )

    # 5. Startup orphan cleanup
    startup_cleanup = cleaner.cleanup()
    if startup_cleanup.found > 0:
        _emit_event("startup_cleanup",
                     found=startup_cleanup.found,
                     killed=startup_cleanup.killed,
                     removed=startup_cleanup.removed)
        metrics.record_cleanup(startup_cleanup.killed + startup_cleanup.removed)

    # 5b. Baseline metrics snapshot
    _emit_event("metrics_snapshot", phase="baseline")
    metrics.snapshot()

    # 6. Start watchdog thread
    watchdog = WatchdogThread(cleaner, watchdog_interval, stop_event)
    watchdog.start()

    # 7. Main RPC loop
    _emit_event("runtime_ready")
    exit_code = 0

    try:
        for line in sys.stdin:
            if shutdown.should_stop:
                break

            line = line.strip()
            if not line:
                continue

            try:
                payload = json.loads(line)
            except json.JSONDecodeError as e:
                response = RPCBridge._error_response(
                    None, ERR_INVALID_PARAMS, f"invalid_json: {e}",
                )
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
                continue

            response = bridge.handle_request(payload)
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()

    except KeyboardInterrupt:
        shutdown.should_stop = True

    # 8. Stop watchdog FIRST — deterministic thread join before drain
    stop_event.set()
    watchdog.join(timeout=5.0)

    # 9. Drain phase
    if shutdown.should_stop:
        _emit_event("runtime_draining")
        drain_actions = shutdown.drain(journal, kill_func=executor._kill_container)
        if drain_actions:
            _emit_event("runtime_drained", drained_count=len(drain_actions))
        exit_code = 2
    else:
        _emit_event("runtime_eof")

    # 10. Final cleanup
    final_cleanup = cleaner.cleanup()
    if final_cleanup.found > 0:
        _emit_event("watchdog_cleanup",
                     tick="final",
                     found=final_cleanup.found,
                     killed=final_cleanup.killed,
                     removed=final_cleanup.removed,
                     errors=len(final_cleanup.errors))
        metrics.record_cleanup(final_cleanup.killed + final_cleanup.removed)

    # 10b. Final workspace GC
    gc_result = gc.collect()
    if gc_result.removed_dirs > 0 or gc_result.removed_files > 0:
        _emit_event("workspace_gc", phase="final", **gc_result.to_dict())

    # 11. Final metrics snapshot
    _emit_event("metrics_snapshot", phase="final")
    final_snap = metrics.snapshot()

    # 12. Runtime stopped
    _emit_event("runtime_stopped", exit_code=exit_code,
                 watchdog_ticks=watchdog.tick_count)
    return exit_code


def main():
    """CLI entrypoint."""
    code = run()
    sys.exit(code)


if __name__ == "__main__":
    main()
