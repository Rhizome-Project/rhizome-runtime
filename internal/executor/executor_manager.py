"""
Executor Manager for Rhizome.
Production-ready Docker execution engine for running agent nodes.

Implements:
- FR-008: Node execution in Docker containers
- FR-009: Shared volume I/O
- FR-011: stdout/stderr collection
- FR-012: Timeout enforcement
- FR-013: CPU/RAM limits
"""
import json
import logging
import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

from internal.executor.collector import OutputBundle, OutputCollector
from internal.executor.profiles import ExecutionProfile, get_profile
from internal.executor.volumes import VolumeManager
from internal.security.redactor import Redactor

logger = logging.getLogger("rhizome.executor")


class ExecutionStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    BUILD_ERROR = "BUILD_ERROR"


@dataclass
class NodeConfig:
    """Configuration for a node to execute."""
    task_id: str
    node_id: str
    runtime_profile: str  # 'compute', 'browser_automation', etc.
    script_ref: str       # Path to script file or inline script
    timeout_sec: Optional[int] = None  # Override profile default
    env: dict[str, str] = field(default_factory=dict)  # Extra env vars
    cpus: Optional[str] = None  # Override profile default
    memory: Optional[str] = None  # Override profile default


@dataclass
class ExecutionResult:
    """Result of a node execution."""
    task_id: str
    node_id: str
    status: ExecutionStatus
    exit_code: int
    output: Optional[OutputBundle] = None
    error_code: Optional[str] = None   # Maps to RPC error catalog
    error_message: Optional[str] = None
    duration_sec: float = 0.0
    container_id: Optional[str] = None


class ExecutorManager:
    """Manages Docker container lifecycle for node execution.

    Usage:
        manager = ExecutorManager()
        result = manager.run_node(NodeConfig(
            task_id="task-001",
            node_id="node-001",
            runtime_profile="compute",
            script_ref="task_script.py",
        ))
    """

    def __init__(
        self,
        workspace_root: str = "./data/workspace",
        image_cache: Optional[dict[str, bool]] = None,
    ):
        self.volumes = VolumeManager(workspace_root)
        self.redactor = Redactor()
        self.collector = OutputCollector(self.redactor)
        self._image_cache = image_cache or {}

    def run_node(self, config: NodeConfig) -> ExecutionResult:
        """Execute a node in a Docker container.

        Complete lifecycle:
        1. Resolve execution profile
        2. Ensure Docker image is available
        3. Prepare volumes
        4. Run container with limits
        5. Collect output (redacted)
        6. Cleanup container
        7. Return result

        Args:
            config: Node configuration.

        Returns:
            ExecutionResult with status, output, and error details.
        """
        start_time = time.time()
        profile = self._resolve_profile(config)
        container_name = f"rhizome-{config.task_id}-{config.node_id}"

        logger.info(
            "Starting node execution",
            extra={
                "task_id": config.task_id,
                "node_id": config.node_id,
                "profile": profile.name,
                "image": profile.image,
                "timeout_sec": config.timeout_sec or profile.default_timeout_sec,
            },
        )

        # 1. Ensure image is available
        if not self._ensure_image(profile):
            return ExecutionResult(
                task_id=config.task_id,
                node_id=config.node_id,
                status=ExecutionStatus.BUILD_ERROR,
                exit_code=-1,
                error_code="executor_runtime_error",
                error_message=f"Failed to pull/build image: {profile.image}",
                duration_sec=time.time() - start_time,
            )

        # 2. Prepare volumes
        vol_paths = self.volumes.prepare_node_volumes(config.task_id, config.node_id)

        # 3. Extract secrets for redaction
        secrets = self.redactor.extract_secrets_from_env(config.env)

        # 4. Build docker run command
        timeout = config.timeout_sec or profile.default_timeout_sec
        cpus = config.cpus or profile.cpus
        memory = config.memory or profile.memory

        cmd = self._build_run_command(
            container_name=container_name,
            profile=profile,
            config=config,
            vol_paths=vol_paths,
            cpus=cpus,
            memory=memory,
        )

        # 5. Execute with timeout
        exit_code, stdout, stderr, timed_out = self._run_container(
            cmd, container_name, timeout
        )

        duration = time.time() - start_time

        if timed_out:
            logger.warning(
                "Node execution timed out",
                extra={
                    "task_id": config.task_id,
                    "node_id": config.node_id,
                    "timeout_sec": timeout,
                },
            )
            return ExecutionResult(
                task_id=config.task_id,
                node_id=config.node_id,
                status=ExecutionStatus.TIMEOUT,
                exit_code=-1,
                error_code="executor_timeout",
                error_message=f"Container killed after {timeout}s timeout",
                duration_sec=duration,
                container_id=container_name,
            )

        # 6. Collect output with redaction
        output = self.collector.collect(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            shared_dir=vol_paths["shared"],
            secrets=secrets,
        )

        # 7. Determine status
        if exit_code == 0:
            status = ExecutionStatus.SUCCESS
            error_code = None
            error_message = None
        else:
            status = ExecutionStatus.FAILED
            error_code = "executor_runtime_error"
            error_message = f"Container exited with code {exit_code}"

        logger.info(
            "Node execution completed",
            extra={
                "task_id": config.task_id,
                "node_id": config.node_id,
                "status": status.value,
                "exit_code": exit_code,
                "duration_sec": round(duration, 2),
                "artifacts_count": len(output.artifacts),
            },
        )

        return ExecutionResult(
            task_id=config.task_id,
            node_id=config.node_id,
            status=status,
            exit_code=exit_code,
            output=output,
            error_code=error_code,
            error_message=error_message,
            duration_sec=duration,
            container_id=container_name,
        )

    def kill_node(self, task_id: str, node_id: str) -> bool:
        """Kill a running container for a node.

        Args:
            task_id: Task ID.
            node_id: Node ID.

        Returns:
            True if container was found and killed.
        """
        container_name = f"rhizome-{task_id}-{node_id}"
        return self._kill_container(container_name)

    def cleanup_node(self, task_id: str, node_id: str) -> None:
        """Cleanup volumes for a completed node."""
        self.volumes.cleanup_node(task_id, node_id)

    def cleanup_task(self, task_id: str) -> None:
        """Cleanup all volumes for a completed task."""
        self.volumes.cleanup_task(task_id)

    # -- private helpers --------------------------------------------------

    def _resolve_profile(self, config: NodeConfig) -> ExecutionProfile:
        """Resolve runtime profile from config."""
        return get_profile(config.runtime_profile)

    def _ensure_image(self, profile: ExecutionProfile) -> bool:
        """Ensure Docker image is available (pull or build)."""
        if profile.image in self._image_cache:
            return True

        try:
            # Check if image exists locally
            result = subprocess.run(
                ["docker", "image", "inspect", profile.image],
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                timeout=10,
            )
            if result.returncode == 0:
                self._image_cache[profile.image] = True
                return True

            # Pull the image
            logger.info(f"Pulling Docker image: {profile.image}")
            result = subprocess.run(
                ["docker", "pull", profile.image],
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                timeout=600,  # Large images like Playwright
            )
            if result.returncode == 0:
                self._image_cache[profile.image] = True

                # Install pip packages if needed
                if profile.pip_packages:
                    self._install_pip_packages(profile)

                return True
            else:
                logger.error(f"Failed to pull image: {result.stderr[:500]}")
                return False

        except subprocess.TimeoutExpired:
            logger.error(f"Timeout pulling image: {profile.image}")
            return False
        except Exception as e:
            logger.error(f"Error ensuring image: {e}")
            return False

    def _install_pip_packages(self, profile: ExecutionProfile) -> None:
        """Install pip packages into a profile image by creating a tagged layer."""
        packages = " ".join(profile.pip_packages)
        tag = f"{profile.image}-rhizome"

        # Create a Dockerfile-like layer
        cmd = [
            "docker", "run", "--rm", "--name", "rhizome-pip-install",
            profile.image,
            "pip", "install", "--no-cache-dir",
        ] + profile.pip_packages

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                timeout=120,
            )
            if result.returncode == 0:
                logger.info(f"Installed packages: {packages}")
        except Exception as e:
            logger.warning(f"Failed to install pip packages: {e}")

    def _build_run_command(
        self,
        container_name: str,
        profile: ExecutionProfile,
        config: NodeConfig,
        vol_paths: dict[str, str],
        cpus: str,
        memory: str,
    ) -> list[str]:
        """Build the docker run command."""
        cmd = [
            "docker", "run",
            "--rm",
            "--name", container_name,
            "--cpus", cpus,
            "--memory", memory,
            "-v", f"{vol_paths['shared']}:/workspace/shared",
            "-v", f"{vol_paths['state']}:/workspace/state",
            "-w", profile.working_dir,
        ]

        # Inject environment variables
        for key, value in config.env.items():
            cmd.extend(["-e", f"{key}={value}"])

        # Image and command
        cmd.append(profile.image)
        cmd.extend([profile.shell, config.script_ref])

        return cmd

    def _run_container(
        self,
        cmd: list[str],
        container_name: str,
        timeout: int,
        max_retries: int = 2,
    ) -> tuple[int, str, str, bool]:
        """Run container with timeout enforcement and retry on transient failures.

        On Windows, Docker daemon occasionally stalls during container creation.
        Retry with cleanup and unique name suffix.

        Returns:
            (exit_code, stdout, stderr, timed_out)
        """
        for attempt in range(1 + max_retries):
            # For retries: use unique name and clean up previous attempt
            if attempt > 0:
                logger.warning(
                    f"Retry {attempt}/{max_retries} for container {container_name}",
                )
                self._kill_container(container_name)
                # Use unique name for retry to avoid Docker name conflict
                retry_name = f"{container_name}-r{attempt}"
                retry_cmd = [c if c != container_name else retry_name for c in cmd]
                self._kill_container(retry_name)
                import time as _time
                _time.sleep(2)  # Backoff to let daemon recover
            else:
                retry_name = container_name
                retry_cmd = cmd

            import threading
            proc = subprocess.Popen(
                retry_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            limit_bytes = 4 * 1024 * 1024  # 4MB limit to prevent OOM

            def _drain(stream, output_list):
                total = 0
                try:
                    for chunk in iter(lambda: stream.read(65536), b""):
                        if total < limit_bytes:
                            allowed = limit_bytes - total
                            output_list.append(chunk[:allowed])
                            total += len(chunk[:allowed])
                except Exception:
                    pass
                finally:
                    stream.close()

            out_chunks = []
            err_chunks = []

            t_out = threading.Thread(target=_drain, args=(proc.stdout, out_chunks), daemon=True)
            t_err = threading.Thread(target=_drain, args=(proc.stderr, err_chunks), daemon=True)
            t_out.start()
            t_err.start()

            timed_out = False
            try:
                proc.wait(timeout=timeout)
                t_out.join(timeout=1.0)
                t_err.join(timeout=1.0)

                stdout = b"".join(out_chunks).decode("utf-8", errors="replace")
                stderr = b"".join(err_chunks).decode("utf-8", errors="replace")
                exit_code = proc.returncode
                return exit_code, stdout, stderr, False
            except subprocess.TimeoutExpired:
                # Kill the subprocess tree
                try:
                    proc.kill()
                    proc.wait(timeout=5)
                except Exception:
                    pass

                # Kill the Docker container
                self._kill_container(retry_name)

                # Return immediately on timeout to prevent zombie containers
                return -1, "", "", True

    def _kill_container(self, container_name: str) -> bool:
        """Kill and remove a Docker container by name."""
        killed = False
        try:
            result = subprocess.run(
                ["docker", "kill", container_name],
                capture_output=True, timeout=10,
            )
            killed = result.returncode == 0
        except Exception:
            pass

        try:
            subprocess.run(
                ["docker", "rm", "-f", container_name],
                capture_output=True, timeout=10,
            )
        except Exception:
            pass

        return killed
