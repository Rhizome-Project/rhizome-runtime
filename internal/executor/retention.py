"""
Workspace artifact retention / garbage collection.

The executor stores runtime data in:
    workspace_root/
        shared/<task_id>/<node_id>/...
        state/<task_id>/...

Older deployments also used:
    workspace_root/tasks/<task_id>/...

GC removes whole per-task trees once they are old enough and the task is not
currently active. This keeps shared and state volumes in sync and avoids leaving
orphaned directories behind.
"""

from __future__ import annotations

import logging
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

logger = logging.getLogger("rhizome.retention")


@dataclass
class RetentionPolicy:
    max_age_seconds: int = 86400
    max_total_size_mb: float = 500.0
    dry_run: bool = False


@dataclass
class GCResult:
    scanned_dirs: int = 0
    removed_dirs: int = 0
    removed_files: int = 0
    freed_bytes: int = 0
    errors: int = 0
    elapsed_sec: float = 0.0
    skipped_active: int = 0

    def to_dict(self) -> dict:
        return {
            "scanned_dirs": self.scanned_dirs,
            "removed_dirs": self.removed_dirs,
            "removed_files": self.removed_files,
            "freed_bytes": self.freed_bytes,
            "freed_mb": round(self.freed_bytes / (1024 * 1024), 2),
            "errors": self.errors,
            "elapsed_sec": round(self.elapsed_sec, 3),
            "skipped_active": self.skipped_active,
        }


class WorkspaceGC:
    def __init__(
        self,
        workspace_root: str,
        policy: Optional[RetentionPolicy] = None,
        active_task_ids: Optional[set] = None,
    ):
        self.workspace_root = Path(workspace_root)
        self.policy = policy or RetentionPolicy()
        self._active_task_ids = active_task_ids or set()

    def set_active_tasks(self, task_ids: set):
        self._active_task_ids = set(task_ids)

    def collect(self) -> GCResult:
        start = time.time()
        result = GCResult()
        now = time.time()
        cutoff = now - self.policy.max_age_seconds

        for task_id, roots in self._iter_task_roots().items():
            result.scanned_dirs += len(roots)
            if task_id in self._active_task_ids:
                result.skipped_active += 1
                continue

            newest_mtime = self._newest_mtime(roots)
            if newest_mtime is None:
                continue
            if newest_mtime > cutoff:
                continue

            size = sum(self._dir_size(root) for root in roots)
            if self.policy.dry_run:
                result.removed_dirs += len(roots)
                result.freed_bytes += size
                logger.info("GC dry-run: would remove task %s (%d bytes)", task_id, size)
                continue

            for root in roots:
                try:
                    shutil.rmtree(root, ignore_errors=True)
                    result.removed_dirs += 1
                except Exception as exc:  # pragma: no cover - defensive logging
                    result.errors += 1
                    logger.warning("GC error removing %s: %s", root, exc)
            result.freed_bytes += size
            logger.info("GC removed task %s (%d bytes)", task_id, size)

        result.elapsed_sec = time.time() - start
        return result

    def _iter_task_roots(self) -> dict[str, list[Path]]:
        groups: dict[str, list[Path]] = {}
        for parent_name in ("shared", "state", "tasks"):
            parent = self.workspace_root / parent_name
            if not parent.exists():
                continue
            try:
                for entry in parent.iterdir():
                    if not entry.is_dir():
                        continue
                    groups.setdefault(entry.name, []).append(entry)
            except OSError:
                continue
        return groups

    @staticmethod
    def _newest_mtime(paths: Iterable[Path]) -> Optional[float]:
        newest: Optional[float] = None
        for path in paths:
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if newest is None or mtime > newest:
                newest = mtime
        return newest

    @staticmethod
    def _dir_size(path: Path) -> int:
        total = 0
        try:
            for entry in path.rglob("*"):
                if entry.is_file():
                    try:
                        total += entry.stat().st_size
                    except OSError:
                        pass
        except OSError:
            pass
        return total
