"""
Volume Manager for Rhizome Executor.
Handles per-task/node volume isolation, creation and cleanup.
"""
import os
import shutil
from pathlib import Path
from typing import Optional


class VolumeManager:
    """Manages shared and state volumes for task/node execution.

    Directory structure:
        {root}/shared/{task_id}/{node_id}/   — node I/O artifacts
        {root}/state/{task_id}/              — persistent session data
    """

    def __init__(self, workspace_root: str = "./data/workspace"):
        self.root = Path(workspace_root).resolve()
        self.shared_root = self.root / "shared"
        self.state_root = self.root / "state"

    def _safe_resolve(self, base_root: Path, *parts: str) -> Path:
        """Safely resolve a path and ensure it remains inside the base root."""
        resolved = base_root.joinpath(*parts).resolve()
        if not str(resolved).startswith(str(base_root)):
            raise ValueError(f"Path traversal detected: {resolved} is outside {base_root}")
        return resolved

    def prepare_node_volumes(self, task_id: str, node_id: str) -> dict[str, str]:
        """Create and return paths for node execution volumes.

        Returns:
            Dict with 'shared' and 'state' absolute paths.
        """
        shared_dir = self._safe_resolve(self.shared_root, task_id, node_id)
        state_dir = self._safe_resolve(self.state_root, task_id)

        shared_dir.mkdir(parents=True, exist_ok=True)
        state_dir.mkdir(parents=True, exist_ok=True)

        return {
            "shared": str(shared_dir),
            "state": str(state_dir),
        }

    def get_shared_dir(self, task_id: str, node_id: str) -> str:
        """Get the shared volume path for a node."""
        return str(self._safe_resolve(self.shared_root, task_id, node_id))

    def get_state_dir(self, task_id: str) -> str:
        """Get the state volume path for a task."""
        return str(self._safe_resolve(self.state_root, task_id))

    def list_artifacts(self, task_id: str, node_id: str) -> list[str]:
        """List all files in the node's shared volume."""
        shared_dir = self._safe_resolve(self.shared_root, task_id, node_id)
        if not shared_dir.exists():
            return []
        return [
            str(p.relative_to(shared_dir))
            for p in shared_dir.rglob("*")
            if p.is_file()
        ]

    def cleanup_node(self, task_id: str, node_id: str) -> None:
        """Remove shared volume for a completed node."""
        shared_dir = self._safe_resolve(self.shared_root, task_id, node_id)
        if shared_dir.exists():
            shutil.rmtree(shared_dir, ignore_errors=True)

    def cleanup_task(self, task_id: str) -> None:
        """Remove all volumes (shared + state) for a completed task."""
        task_shared = self._safe_resolve(self.shared_root, task_id)
        task_state = self._safe_resolve(self.state_root, task_id)

        if task_shared.exists():
            shutil.rmtree(task_shared, ignore_errors=True)
        if task_state.exists():
            shutil.rmtree(task_state, ignore_errors=True)

    def get_disk_usage(self, task_id: Optional[str] = None) -> dict:
        """Get disk usage for volumes.

        Args:
            task_id: If provided, return usage for specific task. Otherwise total.

        Returns:
            Dict with 'shared_bytes', 'state_bytes', 'total_bytes'.
        """
        if task_id:
            shared_path = self._safe_resolve(self.shared_root, task_id)
            state_path = self._safe_resolve(self.state_root, task_id)
        else:
            shared_path = self.shared_root
            state_path = self.state_root

        shared_bytes = self._dir_size(shared_path)
        state_bytes = self._dir_size(state_path)

        return {
            "shared_bytes": shared_bytes,
            "state_bytes": state_bytes,
            "total_bytes": shared_bytes + state_bytes,
        }

    @staticmethod
    def _dir_size(path: Path) -> int:
        """Calculate total size of files in a directory."""
        if not path.exists():
            return 0
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
