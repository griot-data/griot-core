"""
Container runtime for executing container-based checks.

Uses Podman or Docker to execute container images that receive
Arrow IPC data and return CheckResult.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from griot_core.models import Check

from .types import CheckResult, ExecutorSpec


@dataclass
class ContainerConfig:
    """Configuration for container execution."""

    runtime: str = "podman"  # "podman" or "docker"
    network_mode: str = "none"  # Disable network by default for security
    memory_limit: str = "512m"
    cpu_limit: float = 1.0
    timeout_seconds: int = 300
    pull_policy: str = "if-not-present"  # "always", "never", "if-not-present"


@dataclass
class ContainerExecutionResult:
    """Result of container execution."""

    check_result: CheckResult
    execution_time_ms: float
    container_id: Optional[str] = None
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""


class ContainerRuntimeNotFoundError(Exception):
    """Raised when container runtime is not available."""

    pass


class ContainerExecutionError(Exception):
    """Raised when container execution fails."""

    pass


class ContainerImageNotFoundError(Exception):
    """Raised when a container image cannot be found."""

    pass


class ContainerRuntime:
    """
    Runtime for executing container-based validation checks.

    Containers receive Arrow data via a mounted volume and parameters
    via environment variables. Results are returned via stdout.

    The container interface expects:
    - Input: Arrow IPC file at /data/input.arrow
    - Parameters: PARAMETERS environment variable (JSON)
    - Output: JSON-encoded CheckResult to stdout

    Example:
        >>> runtime = ContainerRuntime(config=ContainerConfig(runtime="docker"))
        >>> result = await runtime.execute(spec, check, arrow_data)
        >>> print(result.check_result.passed)
    """

    def __init__(self, config: Optional[ContainerConfig] = None):
        """
        Initialize the container runtime.

        Args:
            config: Container execution configuration
        """
        self.config = config or ContainerConfig()
        self._runtime_path: Optional[str] = None
        self._verify_runtime()

    def _verify_runtime(self) -> None:
        """Verify the container runtime is available."""
        # Check for configured runtime
        runtime = shutil.which(self.config.runtime)
        if runtime:
            self._runtime_path = runtime
            return

        # Try alternatives
        for alt in ["podman", "docker"]:
            if alt != self.config.runtime:
                runtime = shutil.which(alt)
                if runtime:
                    self._runtime_path = runtime
                    self.config.runtime = alt
                    return

        # Runtime not found - we'll handle this gracefully
        self._runtime_path = None

    async def execute(
        self,
        spec: ExecutorSpec,
        check: Check,
        arrow_data: bytes,
        timeout: Optional[int] = None,
    ) -> ContainerExecutionResult:
        """
        Execute a container check.

        Args:
            spec: Executor specification
            check: The check to execute
            arrow_data: Arrow IPC format data
            timeout: Optional timeout override

        Returns:
            ContainerExecutionResult with the check result

        Raises:
            ContainerRuntimeNotFoundError: If no container runtime
            ContainerExecutionError: If execution fails
        """
        if not self._runtime_path:
            return ContainerExecutionResult(
                check_result=CheckResult(
                    passed=False,
                    error="Container runtime not available (podman/docker not found)",
                ),
                execution_time_ms=0,
            )

        start_time = time.perf_counter()
        temp_dir = None

        try:
            # Create temp directory for data exchange
            temp_dir = tempfile.mkdtemp(prefix="griot_")
            data_path = Path(temp_dir) / "input.arrow"

            # Write Arrow data to temp file
            data_path.write_bytes(arrow_data)

            # Pull image if needed
            image = self._get_image_reference(spec)
            await self._ensure_image(image)

            # Build container command
            cmd = self._build_run_command(
                image=image,
                data_path=data_path,
                parameters=check.parameters,
                timeout=timeout or self.config.timeout_seconds,
            )

            # Run container
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Wait with timeout
            effective_timeout = timeout or self.config.timeout_seconds
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=effective_timeout,
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise ContainerExecutionError(
                    f"Container execution timed out after {effective_timeout}s"
                )

            execution_time = (time.perf_counter() - start_time) * 1000

            # Parse result from stdout
            if process.returncode != 0:
                return ContainerExecutionResult(
                    check_result=CheckResult(
                        passed=False,
                        error=f"Container exited with code {process.returncode}: {stderr.decode()}",
                    ),
                    execution_time_ms=execution_time,
                    exit_code=process.returncode,  # type: ignore[arg-type]
                    stdout=stdout.decode(),
                    stderr=stderr.decode(),
                )

            # Parse CheckResult from stdout
            try:
                check_result = CheckResult.from_json(stdout)
            except (json.JSONDecodeError, KeyError) as e:
                check_result = CheckResult(
                    passed=False,
                    error=f"Failed to parse container output: {e}",
                    details={"stdout": stdout.decode()[:1000]},
                )

            return ContainerExecutionResult(
                check_result=check_result,
                execution_time_ms=execution_time,
                exit_code=process.returncode,
                stdout=stdout.decode(),
                stderr=stderr.decode(),
            )

        except Exception as e:
            execution_time = (time.perf_counter() - start_time) * 1000
            return ContainerExecutionResult(
                check_result=CheckResult(
                    passed=False,
                    error=str(e),
                ),
                execution_time_ms=execution_time,
            )

        finally:
            # Clean up temp directory
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def _get_image_reference(self, spec: ExecutorSpec) -> str:
        """Get the container image reference from spec."""
        url = spec.artifact_url

        # Handle different URI schemes
        if url.startswith("oci://"):
            return url[6:]  # Remove oci:// prefix
        elif url.startswith("docker://"):
            return url[9:]  # Remove docker:// prefix
        else:
            return url

    async def _ensure_image(self, image: str) -> None:
        """Ensure the container image is available."""
        if self.config.pull_policy == "never":
            return

        if self.config.pull_policy == "if-not-present":
            # Check if image exists
            check_cmd = [self._runtime_path, "image", "exists", image]
            process = await asyncio.create_subprocess_exec(
                *check_cmd,  # type: ignore[arg-type]
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await process.wait()
            if process.returncode == 0:
                return

        # Pull the image
        pull_cmd = [self._runtime_path, "pull", image]
        process = await asyncio.create_subprocess_exec(
            *pull_cmd,  # type: ignore[arg-type]
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise ContainerImageNotFoundError(f"Failed to pull image {image}: {stderr.decode()}")

    def _build_run_command(
        self,
        image: str,
        data_path: Path,
        parameters: Dict[str, Any],
        timeout: int,
    ) -> List[str]:
        """Build the container run command."""
        cmd = [
            self._runtime_path,
            "run",
            "--rm",  # Remove container after execution
            "--network",
            self.config.network_mode,
            "--memory",
            self.config.memory_limit,
            "--cpus",
            str(self.config.cpu_limit),
            "-v",
            f"{data_path}:/data/input.arrow:ro",
            "-e",
            f"PARAMETERS={json.dumps(parameters)}",
            "-e",
            f"TIMEOUT={timeout}",
        ]

        # Add security options
        if self.config.runtime == "podman":
            cmd.extend(["--security-opt", "no-new-privileges"])

        cmd.append(image)

        return cmd  # type: ignore[return-value]

    async def pull_image(self, image: str) -> bool:
        """
        Pull a container image.

        Args:
            image: Image reference to pull

        Returns:
            True if successful
        """
        if not self._runtime_path:
            return False

        try:
            await self._ensure_image(image)
            return True
        except ContainerImageNotFoundError:
            return False

    def is_available(self) -> bool:
        """Check if the container runtime is available."""
        return self._runtime_path is not None

    def get_runtime_info(self) -> Dict[str, Any]:
        """Get information about the container runtime."""
        return {
            "available": self.is_available(),
            "runtime": self.config.runtime,
            "path": self._runtime_path,
            "memory_limit": self.config.memory_limit,
            "cpu_limit": self.config.cpu_limit,
            "network_mode": self.config.network_mode,
        }
