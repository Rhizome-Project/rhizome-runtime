"""
Execution Profiles for Rhizome Executor.
Maps runtime_profile strings to Docker image configurations.
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class ExecutionProfile:
    """Configuration for a specific execution environment."""
    name: str
    image: str
    default_timeout_sec: int
    cpus: str = "1.0"
    memory: str = "512m"
    pip_packages: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    working_dir: str = "/app"
    shell: str = "python"


# Predefined profiles
ComputeProfile = ExecutionProfile(
    name="compute",
    image="python:3.11-slim",
    default_timeout_sec=300,
    cpus="1.0",
    memory="512m",
)

BrowserProfile = ExecutionProfile(
    name="browser",
    image="mcr.microsoft.com/playwright/python:v1.51.0-noble",
    default_timeout_sec=60,
    cpus="1.0",
    memory="1g",  # Playwright needs more RAM
    pip_packages=["playwright==1.51.0"],
)

# Profile registry
_PROFILES: dict[str, ExecutionProfile] = {
    "compute": ComputeProfile,
    "browser_automation": BrowserProfile,
    "browser": BrowserProfile,
    # Aliases
    "python": ComputeProfile,
    "playwright": BrowserProfile,
}


def get_profile(runtime_profile: str) -> ExecutionProfile:
    """Resolve a runtime_profile string to an ExecutionProfile.

    Args:
        runtime_profile: Profile name from node config (e.g. 'compute', 'browser_automation').

    Returns:
        ExecutionProfile for the given name.

    Raises:
        ValueError: If profile name is not registered.
    """
    profile = _PROFILES.get(runtime_profile)
    if profile is None:
        available = ", ".join(sorted(_PROFILES.keys()))
        raise ValueError(
            f"Unknown runtime_profile '{runtime_profile}'. Available: {available}"
        )
    return profile


def register_profile(name: str, profile: ExecutionProfile) -> None:
    """Register a custom execution profile."""
    _PROFILES[name] = profile
