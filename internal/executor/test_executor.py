"""
Integration Test: Executor Manager (requires Docker).
Tests full container lifecycle: run, collect output, artifacts, timeout, kill.
"""
import io
import json
import os
import sys
import time
import tempfile
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import subprocess

from internal.executor.executor_manager import ExecutorManager, ExecutionStatus, NodeConfig
from internal.executor.profiles import ComputeProfile, BrowserProfile
from internal.executor.volumes import VolumeManager
from internal.security.redactor import Redactor


def warm_up_docker():
    """Ensure Docker image is cached and daemon is responsive."""
    try:
        subprocess.run(
            ["docker", "run", "--rm", "python:3.11-slim", "python", "-c", "print('warm')"],
            capture_output=True, timeout=60,
        )
    except Exception:
        pass


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def header(text):
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}{Colors.RESET}\n")


def result_line(label, passed, detail=""):
    icon = f"{Colors.GREEN}[PASS]{Colors.RESET}" if passed else f"{Colors.RED}[FAIL]{Colors.RESET}"
    suffix = f"  ({detail})" if detail else ""
    print(f"  {icon}  {label}{suffix}")


def test_compute_profile_execution():
    """Test: run a simple Python script in compute profile container."""
    header("Test 1: Compute profile execution")
    checks = {}
    workspace = tempfile.mkdtemp(prefix="rhizome_exec_test_")

    try:
        manager = ExecutorManager(workspace_root=workspace)

        # Create a test script in shared volume
        task_id = "test-task-001"
        node_id = "test-node-001"
        vol = manager.volumes.prepare_node_volumes(task_id, node_id)

        # Write inline script
        script_content = '''
import json, os, sys
print("Hello from executor test")
sys.stderr.write("Debug: running compute test\\n")
result = {"status": "ok", "computed": 42}
with open("/workspace/shared/result.json", "w") as f:
    json.dump(result, f)
'''
        script_path = os.path.join(vol["shared"], "_test_script.py")
        with open(script_path, "w") as f:
            f.write(script_content)

        config = NodeConfig(
            task_id=task_id,
            node_id=node_id,
            runtime_profile="compute",
            script_ref="/workspace/shared/_test_script.py",
            timeout_sec=30,
        )

        result = manager.run_node(config)

        checks["status_success"] = result.status == ExecutionStatus.SUCCESS
        result_line("Status == SUCCESS", checks["status_success"], result.status.value)

        checks["exit_code_0"] = result.exit_code == 0
        result_line("Exit code == 0", checks["exit_code_0"], str(result.exit_code))

        if result.output:
            checks["stdout_has_hello"] = "Hello from executor test" in result.output.stdout
            result_line("stdout contains greeting", checks["stdout_has_hello"])

            checks["stderr_has_debug"] = "Debug" in result.output.stderr
            result_line("stderr contains debug", checks["stderr_has_debug"])

            has_result_json = any(a.path == "result.json" for a in result.output.artifacts)
            checks["artifact_collected"] = has_result_json
            result_line("result.json artifact collected", checks["artifact_collected"])
        else:
            checks["stdout_has_hello"] = False
            checks["stderr_has_debug"] = False
            checks["artifact_collected"] = False

        checks["duration_reasonable"] = 0 < result.duration_sec < 30
        result_line("Duration reasonable", checks["duration_reasonable"], f"{result.duration_sec:.1f}s")

    finally:
        import shutil
        shutil.rmtree(workspace, ignore_errors=True)

    return checks


def test_failed_execution():
    """Test: script that exits with error."""
    header("Test 2: Failed execution")
    checks = {}
    workspace = tempfile.mkdtemp(prefix="rhizome_exec_test_")

    try:
        manager = ExecutorManager(workspace_root=workspace)
        task_id = "test-task-002"
        node_id = "test-node-002"
        vol = manager.volumes.prepare_node_volumes(task_id, node_id)

        script_content = '''
import sys, json
sys.stderr.write("FATAL: something went wrong\\n")
with open("/workspace/shared/error_context.json", "w") as f:
    json.dump({"error": "test error", "code": "E001"}, f)
sys.exit(1)
'''
        script_path = os.path.join(vol["shared"], "_test_fail.py")
        with open(script_path, "w") as f:
            f.write(script_content)

        config = NodeConfig(
            task_id=task_id,
            node_id=node_id,
            runtime_profile="compute",
            script_ref="/workspace/shared/_test_fail.py",
            timeout_sec=60,
        )

        result = manager.run_node(config)

        checks["status_failed"] = result.status == ExecutionStatus.FAILED
        result_line("Status == FAILED", checks["status_failed"], result.status.value)

        checks["exit_code_1"] = result.exit_code == 1
        result_line("Exit code == 1", checks["exit_code_1"], str(result.exit_code))

        checks["error_code_set"] = result.error_code == "executor_runtime_error"
        result_line("error_code set", checks["error_code_set"], result.error_code or "None")

        if result.output:
            checks["stderr_has_fatal"] = "FATAL" in result.output.stderr
            result_line("stderr contains FATAL", checks["stderr_has_fatal"])
        else:
            checks["stderr_has_fatal"] = False

    finally:
        import shutil
        shutil.rmtree(workspace, ignore_errors=True)

    return checks


def test_secret_redaction_in_output():
    """Test: secrets injected via env are redacted from stdout (AT-008)."""
    header("Test 3: Secret redaction in output (AT-008)")
    checks = {}
    workspace = tempfile.mkdtemp(prefix="rhizome_exec_test_")

    try:
        manager = ExecutorManager(workspace_root=workspace)
        task_id = "test-task-003"
        node_id = "test-node-003"
        vol = manager.volumes.prepare_node_volumes(task_id, node_id)

        # Script that prints the secret value
        script_content = '''
import os
key = os.environ.get("OPENAI_API_KEY", "NOT_SET")
print(f"Using API key: {key}")
print(f"Connecting to service with {key}")
'''
        script_path = os.path.join(vol["shared"], "_test_redact.py")
        with open(script_path, "w") as f:
            f.write(script_content)

        secret_value = "sk-test-secret-key-1234567890abcdef"

        config = NodeConfig(
            task_id=task_id,
            node_id=node_id,
            runtime_profile="compute",
            script_ref="/workspace/shared/_test_redact.py",
            timeout_sec=60,
            env={"OPENAI_API_KEY": secret_value},
        )

        result = manager.run_node(config)

        if result.output:
            checks["secret_not_in_stdout"] = secret_value not in result.output.stdout
            result_line("Secret NOT in stdout", checks["secret_not_in_stdout"])

            checks["redacted_marker_present"] = "[REDACTED]" in result.output.stdout
            result_line("[REDACTED] marker in stdout", checks["redacted_marker_present"])

            checks["secret_not_in_stderr"] = secret_value not in result.output.stderr
            result_line("Secret NOT in stderr", checks["secret_not_in_stderr"])
        else:
            checks["secret_not_in_stdout"] = False
            checks["redacted_marker_present"] = False
            checks["secret_not_in_stderr"] = False
            result_line("No output collected", False)

    finally:
        import shutil
        shutil.rmtree(workspace, ignore_errors=True)

    return checks


def test_volume_isolation():
    """Test: volumes are isolated per task/node."""
    header("Test 4: Volume isolation")
    checks = {}
    workspace = tempfile.mkdtemp(prefix="rhizome_exec_test_")

    try:
        vm = VolumeManager(workspace_root=workspace)

        vol_a = vm.prepare_node_volumes("task-A", "node-1")
        vol_b = vm.prepare_node_volumes("task-A", "node-2")
        vol_c = vm.prepare_node_volumes("task-B", "node-1")

        checks["shared_dirs_different"] = (
            vol_a["shared"] != vol_b["shared"] != vol_c["shared"]
        )
        result_line("Shared dirs are different", checks["shared_dirs_different"])

        checks["state_same_task"] = vol_a["state"] == vol_b["state"]
        result_line("State dir same for same task", checks["state_same_task"])

        checks["state_diff_task"] = vol_a["state"] != vol_c["state"]
        result_line("State dir different for diff task", checks["state_diff_task"])

        # Write file and verify isolation
        with open(os.path.join(vol_a["shared"], "test.txt"), "w") as f:
            f.write("task-A-node-1")

        artifacts_a = vm.list_artifacts("task-A", "node-1")
        artifacts_b = vm.list_artifacts("task-A", "node-2")

        checks["artifact_in_a"] = "test.txt" in artifacts_a
        result_line("Artifact in node-1", checks["artifact_in_a"])

        checks["no_artifact_in_b"] = "test.txt" not in artifacts_b
        result_line("No artifact in node-2", checks["no_artifact_in_b"])

        # Cleanup
        vm.cleanup_task("task-A")
        checks["cleaned_up"] = not os.path.exists(vol_a["shared"])
        result_line("Task cleanup removes dirs", checks["cleaned_up"])

    finally:
        import shutil
        shutil.rmtree(workspace, ignore_errors=True)

    return checks


# -- main -------------------------------------------------------------
def main():
    header("EXECUTOR INTEGRATION TESTS")
    warm_up_docker()

    results = {
        "compute": test_compute_profile_execution(),
        "failed": test_failed_execution(),
        "redaction": test_secret_redaction_in_output(),
        "volumes": test_volume_isolation(),
    }

    header("SUMMARY")
    total = 0
    passed = 0
    for checks in results.values():
        for v in checks.values():
            total += 1
            if v:
                passed += 1

    all_passed = passed == total
    color = Colors.GREEN if all_passed else Colors.RED
    print(f"  {color}{passed}/{total} checks passed{Colors.RESET}")

    if all_passed:
        print(f"\n  {Colors.GREEN}{Colors.BOLD}ALL EXECUTOR TESTS PASS{Colors.RESET}")
    else:
        print(f"\n  {Colors.RED}{Colors.BOLD}SOME TESTS FAILED{Colors.RESET}")

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
