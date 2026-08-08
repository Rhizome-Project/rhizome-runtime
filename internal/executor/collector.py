"""
Output Collector for Rhizome Executor.
Collects stdout, stderr, exit code, and artifacts from Docker container runs.
"""
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from internal.security.redactor import Redactor


@dataclass
class Artifact:
    """Represents a file artifact collected from shared volume."""
    path: str          # Relative path within shared volume
    abs_path: str      # Absolute path on host
    size_bytes: int
    content_type: str  # Inferred: json, png, txt, other


@dataclass
class OutputBundle:
    """Complete output from a container execution."""
    exit_code: int
    stdout: str
    stderr: str
    artifacts: list[Artifact] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


# Content type inference
_EXTENSION_MAP = {
    ".json": "json",
    ".jsonl": "jsonl",
    ".png": "png",
    ".jpg": "jpeg",
    ".jpeg": "jpeg",
    ".txt": "text",
    ".log": "text",
    ".html": "html",
    ".csv": "csv",
    ".md": "markdown",
}


class OutputCollector:
    """Collects and processes output from Docker container executions.

    Applies secret redaction to stdout/stderr before returning.
    """

    def __init__(self, redactor: Optional[Redactor] = None):
        self.redactor = redactor or Redactor()

    def collect(
        self,
        exit_code: int,
        stdout: str,
        stderr: str,
        shared_dir: str,
        secrets: Optional[list[str]] = None,
    ) -> OutputBundle:
        """Collect and process container output.

        Args:
            exit_code: Container exit code.
            stdout: Raw stdout from container.
            stderr: Raw stderr from container.
            shared_dir: Path to shared volume on host.
            secrets: Optional list of secret values to redact.

        Returns:
            OutputBundle with redacted output and collected artifacts.
        """
        # Redact secrets from output
        redacted_stdout = self.redactor.redact(stdout, secrets or [])
        redacted_stderr = self.redactor.redact(stderr, secrets or [])

        # Collect artifacts from shared volume
        artifacts = self._collect_artifacts(shared_dir)

        # Extract metrics if present
        metrics = self._extract_metrics(shared_dir)

        return OutputBundle(
            exit_code=exit_code,
            stdout=redacted_stdout,
            stderr=redacted_stderr,
            artifacts=artifacts,
            metrics=metrics,
        )

    def _collect_artifacts(self, shared_dir: str) -> list[Artifact]:
        """Scan shared volume for output artifacts."""
        shared_path = Path(shared_dir)
        if not shared_path.exists():
            return []

        artifacts = []
        for file_path in shared_path.rglob("*"):
            if not file_path.is_file():
                continue

            rel_path = str(file_path.relative_to(shared_path))
            ext = file_path.suffix.lower()
            content_type = _EXTENSION_MAP.get(ext, "other")

            artifacts.append(Artifact(
                path=rel_path,
                abs_path=str(file_path),
                size_bytes=file_path.stat().st_size,
                content_type=content_type,
            ))

        return artifacts

    def _extract_metrics(self, shared_dir: str) -> dict:
        """Try to extract metrics from a metrics.json file in shared volume."""
        metrics_path = Path(shared_dir) / "metrics.json"
        if not metrics_path.exists():
            return {}
        try:
            with open(metrics_path, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
