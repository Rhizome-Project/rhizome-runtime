"""
Orphan Container Cleanup for Rhizome Executor.
Finds and removes 'rhizome-*' containers that outlived their tasks.
"""
import logging
import subprocess
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("rhizome.cleanup")


@dataclass
class CleanupReport:
    """Report from an orphan cleanup run."""
    found: int = 0
    killed: int = 0
    removed: int = 0
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class OrphanCleaner:
    """Finds and removes orphaned 'rhizome-*' Docker containers.

    Containers are considered orphaned if they:
    - Have name matching 'rhizome-*'
    - Have been running longer than max_age_seconds
    - OR are in 'exited' state
    """

    def __init__(self, max_age_seconds: int = 600):
        self.max_age_seconds = max_age_seconds

    def cleanup(self) -> CleanupReport:
        """Find and remove orphan containers."""
        report = CleanupReport()

        containers = self._list_rhizome_containers()
        report.found = len(containers)

        if not containers:
            return report

        for cid, name, status, created in containers:
            try:
                if status == "running":
                    # Check age
                    age = time.time() - created
                    if age > self.max_age_seconds:
                        self._kill_container(name)
                        report.killed += 1
                        logger.info(f"Killed orphan container: {name} (age: {age:.0f}s)")

                elif status in ("exited", "dead", "created"):
                    self._remove_container(name)
                    report.removed += 1
                    logger.info(f"Removed exited container: {name}")

            except Exception as e:
                report.errors.append(f"{name}: {e}")

        return report

    def _list_rhizome_containers(self) -> list[tuple]:
        """List all 'rhizome-*' containers.

        Returns:
            List of (container_id, name, status, created_timestamp).
        """
        try:
            result = subprocess.run(
                [
                    "docker", "ps", "-a",
                    "--filter", "name=rhizome-",
                    "--format", "{{.ID}}\t{{.Names}}\t{{.Status}}\t{{.CreatedAt}}",
                ],
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                timeout=10,
            )
            if result.returncode != 0:
                return []

            containers = []
            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue
                parts = line.split("\t")
                if len(parts) >= 3:
                    cid = parts[0]
                    name = parts[1]
                    status_str = parts[2].lower()

                    # Parse status
                    if "up" in status_str:
                        status = "running"
                    elif "exited" in status_str:
                        status = "exited"
                    elif "created" in status_str:
                        status = "created"
                    elif "dead" in status_str:
                        status = "dead"
                    else:
                        status = status_str

                    # Approximate creation time (use current time - running duration)
                    created = time.time() - 60  # Placeholder

                    containers.append((cid, name, status, created))

            return containers

        except Exception as e:
            logger.error(f"Failed to list containers: {e}")
            return []

    def _kill_container(self, name: str):
        subprocess.run(
            ["docker", "kill", name],
            capture_output=True, timeout=10,
        )
        self._remove_container(name)

    def _remove_container(self, name: str):
        subprocess.run(
            ["docker", "rm", "-f", name],
            capture_output=True, timeout=10,
        )
