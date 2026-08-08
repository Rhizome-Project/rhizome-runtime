"""
Graceful Shutdown / Drain for Rhizome Executor.
Handles SIGTERM/SIGINT to safely drain in-flight executions.

On signal:
1. Stop accepting new requests
2. Wait for in-flight containers (bounded)
3. Mark incomplete in journal as FAILED:shutdown_drain
4. Exit cleanly
"""
import logging
import os
import signal
import sys
import threading
import time
from typing import Optional

logger = logging.getLogger("rhizome.shutdown")

# Windows doesn't have SIGTERM via signal, use SIGBREAK
_IS_WINDOWS = sys.platform == "win32"


class GracefulShutdown:
    """Manages graceful shutdown for the executor process.

    Usage:
        shutdown = GracefulShutdown(drain_timeout=30)
        shutdown.install()

        # In your main loop:
        while not shutdown.should_stop:
            process_request(...)

        shutdown.drain(in_flight_containers, journal)
    """

    def __init__(self, drain_timeout: int = 30):
        self.drain_timeout = drain_timeout
        self.should_stop = False
        self._signal_received = None
        self._lock = threading.Lock()

    def install(self):
        """Install signal handlers."""
        signal.signal(signal.SIGINT, self._handle_signal)
        if _IS_WINDOWS:
            # SIGBREAK is the Windows equivalent of SIGTERM
            try:
                signal.signal(signal.SIGBREAK, self._handle_signal)
            except (AttributeError, ValueError):
                pass
        else:
            signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        """Handle shutdown signal."""
        with self._lock:
            if self._signal_received:
                # Second signal = force exit
                logger.warning("Second signal received, forcing exit")
                os._exit(1)
            self._signal_received = signum
            self.should_stop = True
            sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
            logger.info(f"Received {sig_name}, initiating graceful shutdown")

    def drain(self, journal, kill_func=None) -> list[dict]:
        """Drain in-flight executions.

        Args:
            journal: ExecutionJournal instance.
            kill_func: Function to kill a container by name.

        Returns:
            List of drain actions taken.
        """
        from internal.executor.recovery import ExecutionJournal

        actions = []
        in_flight = journal.get_in_flight()

        if not in_flight:
            logger.info("No in-flight executions, clean shutdown")
            return actions

        logger.info(f"Draining {len(in_flight)} in-flight executions (timeout: {self.drain_timeout}s)")

        deadline = time.time() + self.drain_timeout

        for entry in in_flight:
            remaining = deadline - time.time()
            if remaining <= 0:
                # Out of time — force-fail remaining
                action = {
                    "task_id": entry.task_id,
                    "node_id": entry.node_id,
                    "action": "failed:shutdown_drain_timeout",
                }
                journal.record_recovery(
                    entry.task_id, entry.node_id,
                    action="failed:shutdown_drain_timeout",
                )
                actions.append(action)
                continue

            # Try to wait for container
            container = entry.container_name
            waited = self._wait_container(container, min(int(remaining), 10))

            if waited is not None:
                # Exited naturally
                status = "collected" if waited == 0 else "failed:collected"
                action = {
                    "task_id": entry.task_id,
                    "node_id": entry.node_id,
                    "action": status,
                    "exit_code": waited,
                }
                journal.record_recovery(
                    entry.task_id, entry.node_id,
                    action=status, exit_code=waited,
                )
            else:
                # Kill it
                if kill_func:
                    kill_func(container)
                action = {
                    "task_id": entry.task_id,
                    "node_id": entry.node_id,
                    "action": "failed:shutdown_drain",
                }
                journal.record_recovery(
                    entry.task_id, entry.node_id,
                    action="failed:shutdown_drain",
                )

            actions.append(action)

        logger.info(f"Drain complete: {len(actions)} actions")
        return actions

    def _wait_container(self, name: str, timeout: int) -> Optional[int]:
        """Wait for container to exit."""
        import subprocess
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
        except (subprocess.TimeoutExpired, ValueError, Exception):
            return None
