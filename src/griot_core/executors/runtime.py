"""
Unified executor runtime.

Combines WASM and Container runtimes into a single interface
that automatically selects the appropriate runtime based on
executor specification and preferences.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from griot_core.models import Check
from griot_core.models.enums import Runtime

from .container_runtime import ContainerConfig, ContainerRuntime
from .types import ExecutorResult, ExecutorSpec
from .wasm_runtime import WasmRuntime


@dataclass
class RuntimeCapabilities:
    """Capabilities of the runtime environment."""

    wasm_available: bool
    container_available: bool
    container_runtime: Optional[str] = None  # "podman" or "docker"


class ExecutorRuntime:
    """
    Unified executor runtime that handles both WASM and Container execution.

    Automatically selects the appropriate runtime based on the executor
    specification and the configured runtime preferences.

    Example:
        >>> runtime = ExecutorRuntime(
        ...     wasm_cache_dir=Path("/tmp/wasm"),
        ...     container_config=ContainerConfig(runtime="docker")
        ... )
        >>> result = await runtime.execute(spec, check, arrow_data)
        >>> print(result.check_result.passed)
    """

    def __init__(
        self,
        wasm_cache_dir: Optional[Path] = None,
        container_config: Optional[ContainerConfig] = None,
        runtime_preference: Optional[List[Runtime]] = None,
    ):
        """
        Initialize the unified executor runtime.

        Args:
            wasm_cache_dir: Cache directory for WASM modules
            container_config: Configuration for container execution
            runtime_preference: Preferred runtime order (default: [WASM, CONTAINER])
        """
        self._wasm_runtime = WasmRuntime(cache_dir=wasm_cache_dir)
        self._container_runtime = ContainerRuntime(config=container_config)
        self._runtime_preference = runtime_preference or [Runtime.WASM, Runtime.CONTAINER]

    async def execute(
        self,
        spec: ExecutorSpec,
        check: Check,
        arrow_data: bytes,
        runtime_preference: Optional[List[Runtime]] = None,
        timeout: Optional[int] = None,
    ) -> ExecutorResult:
        """
        Execute a check using the appropriate runtime.

        Args:
            spec: Executor specification
            check: The check to execute
            arrow_data: Arrow IPC format data
            runtime_preference: Override runtime preference for this execution
            timeout: Optional timeout in seconds

        Returns:
            ExecutorResult with the check result and execution metadata
        """
        # Determine which runtime to use
        runtime = self._select_runtime(spec, runtime_preference)

        if runtime == Runtime.WASM:
            result = await self._wasm_runtime.execute(spec, check, arrow_data, timeout)
            return ExecutorResult(
                check_result=result.check_result,
                executor_id=spec.id,
                executor_version=spec.version,
                execution_time_ms=result.execution_time_ms,
                memory_used_bytes=result.memory_used_bytes,
                runtime=Runtime.WASM,
            )
        else:
            result = await self._container_runtime.execute(spec, check, arrow_data, timeout)  # type: ignore[assignment]
            return ExecutorResult(
                check_result=result.check_result,
                executor_id=spec.id,
                executor_version=spec.version,
                execution_time_ms=result.execution_time_ms,
                runtime=Runtime.CONTAINER,
            )

    def _select_runtime(
        self,
        spec: ExecutorSpec,
        preference: Optional[List[Runtime]] = None,
    ) -> Runtime:
        """
        Select the runtime to use for an executor.

        Selection order:
        1. If executor requires a specific runtime, use that
        2. Try runtimes in preference order
        3. Fall back to the executor's native runtime
        """
        prefs = preference or self._runtime_preference

        # If executor specifies a runtime, check if it's available
        if spec.runtime == Runtime.WASM:
            # WASM is always available (fallback to mock)
            return Runtime.WASM
        elif spec.runtime == Runtime.CONTAINER:
            if self._container_runtime.is_available():
                return Runtime.CONTAINER
            # Can't run container executor without container runtime
            return Runtime.CONTAINER  # Will fail gracefully

        # Try preferred runtimes
        for runtime in prefs:
            if runtime == Runtime.WASM:
                return Runtime.WASM
            elif runtime == Runtime.CONTAINER:
                if self._container_runtime.is_available():
                    return Runtime.CONTAINER

        # Fall back to WASM
        return Runtime.WASM

    def get_capabilities(self) -> RuntimeCapabilities:
        """Get the capabilities of this runtime environment."""
        container_info = self._container_runtime.get_runtime_info()
        return RuntimeCapabilities(
            wasm_available=True,  # WASM is always available
            container_available=container_info["available"],
            container_runtime=container_info.get("runtime")
            if container_info["available"]
            else None,
        )

    def is_runtime_available(self, runtime: Runtime) -> bool:
        """Check if a specific runtime is available."""
        if runtime == Runtime.WASM:
            return True
        elif runtime == Runtime.CONTAINER:
            return self._container_runtime.is_available()
        return False

    async def preload_executors(self, specs: List[ExecutorSpec]) -> Dict[str, bool]:
        """
        Preload executors to warm up caches.

        Args:
            specs: List of executor specifications to preload

        Returns:
            Dictionary mapping executor IDs to preload success status
        """
        results = {}
        for spec in specs:
            try:
                if spec.runtime == Runtime.WASM:
                    self._wasm_runtime.preload_module(spec)
                    results[spec.id] = True
                elif spec.runtime == Runtime.CONTAINER:
                    image = spec.artifact_url
                    if image.startswith("oci://"):
                        image = image[6:]
                    success = await self._container_runtime.pull_image(image)
                    results[spec.id] = success
            except Exception:
                results[spec.id] = False
        return results

    def clear_caches(self) -> None:
        """Clear all runtime caches."""
        self._wasm_runtime.clear_cache()
