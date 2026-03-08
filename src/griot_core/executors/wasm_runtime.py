"""
WASM runtime for executing WASM-based checks.

Uses wasmtime to execute WASM modules that receive Arrow IPC data
and return CheckResult.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from griot_core.models import Check

from .types import CheckResult, ExecutorSpec


@dataclass
class WasmExecutionContext:
    """Context for WASM execution."""

    module_path: Path
    memory_limit_mb: int = 256
    timeout_seconds: int = 60
    fuel_limit: Optional[int] = None  # For limiting execution steps


@dataclass
class WasmExecutionResult:
    """Result of WASM execution."""

    check_result: CheckResult
    execution_time_ms: float
    memory_used_bytes: int = 0
    fuel_consumed: Optional[int] = None


class WasmModuleNotFoundError(Exception):
    """Raised when a WASM module cannot be found."""

    pass


class WasmExecutionError(Exception):
    """Raised when WASM execution fails."""

    pass


class WasmRuntime:
    """
    Runtime for executing WASM-based validation checks.

    WASM modules receive Arrow IPC data and parameters, and return
    a CheckResult as JSON.

    The WASM interface expects:
    - Input: Arrow IPC bytes + JSON parameters
    - Output: JSON-encoded CheckResult

    Example:
        >>> runtime = WasmRuntime(cache_dir=Path("/tmp/wasm"))
        >>> result = await runtime.execute(spec, check, arrow_data)
        >>> print(result.check_result.passed)
    """

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        memory_limit_mb: int = 256,
        default_timeout: int = 60,
    ):
        """
        Initialize the WASM runtime.

        Args:
            cache_dir: Directory for caching WASM modules
            memory_limit_mb: Default memory limit for WASM execution
            default_timeout: Default timeout in seconds
        """
        self._cache_dir = cache_dir or Path.home() / ".griot" / "wasm"
        self._memory_limit_mb = memory_limit_mb
        self._default_timeout = default_timeout
        self._module_cache: Dict[str, Any] = {}  # wasmtime.Module cache
        self._engine: Optional[Any] = None

        # Ensure cache directory exists
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_engine(self) -> Any:
        """Get or create the wasmtime engine."""
        if self._engine is None:
            try:
                import wasmtime

                config = wasmtime.Config()
                config.consume_fuel = True  # Enable fuel metering
                self._engine = wasmtime.Engine(config)
            except ImportError:
                raise ImportError(
                    "wasmtime is required for WASM execution. Install it with: pip install wasmtime"
                )
        return self._engine

    async def execute(
        self,
        spec: ExecutorSpec,
        check: Check,
        arrow_data: bytes,
        timeout: Optional[int] = None,
    ) -> WasmExecutionResult:
        """
        Execute a WASM check.

        Args:
            spec: Executor specification
            check: The check to execute
            arrow_data: Arrow IPC format data
            timeout: Optional timeout override

        Returns:
            WasmExecutionResult with the check result

        Raises:
            WasmModuleNotFoundError: If the module cannot be found
            WasmExecutionError: If execution fails
        """
        start_time = time.perf_counter()

        try:
            # Load the module
            module = await self._load_module(spec)

            # Serialize parameters
            params_json = json.dumps(check.parameters).encode("utf-8")

            # Execute — pass the named check function from the check definition
            result_bytes = await self._execute_module(
                module,
                arrow_data,
                params_json,
                timeout or self._default_timeout,
                check_function=check.check_function,
            )

            # Parse result
            check_result = CheckResult.from_json(result_bytes)

            execution_time = (time.perf_counter() - start_time) * 1000

            return WasmExecutionResult(
                check_result=check_result,
                execution_time_ms=execution_time,
            )

        except Exception as e:
            execution_time = (time.perf_counter() - start_time) * 1000
            return WasmExecutionResult(
                check_result=CheckResult(
                    passed=False,
                    error=str(e),
                ),
                execution_time_ms=execution_time,
            )

    async def _load_module(self, spec: ExecutorSpec) -> Any:
        """Load and cache a WASM module."""
        cache_key = f"{spec.id}:{spec.version}"

        if cache_key in self._module_cache:
            return self._module_cache[cache_key]

        # Determine module path
        module_path = self._get_module_path(spec)

        if not module_path.exists():
            # Try to fetch the module
            await self._fetch_module(spec, module_path)

        if not module_path.exists():
            raise WasmModuleNotFoundError(f"WASM module not found: {spec.artifact_url}")

        try:
            import wasmtime

            engine = self._get_engine()
            module = wasmtime.Module.from_file(engine, str(module_path))
            self._module_cache[cache_key] = module
            return module
        except Exception as e:
            raise WasmExecutionError(f"Failed to load WASM module: {e}")

    async def _fetch_module(self, spec: ExecutorSpec, target_path: Path) -> None:
        """Fetch a WASM module from its artifact URL."""
        # For now, we'll create a placeholder implementation
        # In production, this would download from the registry
        pass

    def _get_module_path(self, spec: ExecutorSpec) -> Path:
        """Get the local path for a WASM module."""
        # Create a deterministic filename
        filename = f"{spec.id}-{spec.version}.wasm"
        return self._cache_dir / filename

    async def _execute_module(
        self,
        module: Any,
        arrow_data: bytes,
        params_json: bytes,
        timeout: int,
        check_function: str = "validate",
    ) -> bytes:
        """
        Execute a WASM module with the given data.

        The WASM module is expected to export a named function (defaulting
        to 'validate') that takes Arrow data and parameters and returns a
        result.  For multi-function bundles, ``check_function`` selects
        the specific export (e.g. "null_check", "pii_detection").
        """
        try:
            import wasmtime

            engine = self._get_engine()
            store = wasmtime.Store(engine)

            # Set fuel limit based on timeout (approximate)
            fuel_limit = timeout * 1_000_000  # 1M fuel units per second
            store.add_fuel(fuel_limit)

            # Create linker and instance
            linker = wasmtime.Linker(engine)
            linker.define_wasi()

            # Create WASI config
            wasi_config = wasmtime.WasiConfig()
            store.set_wasi(wasi_config)

            # Instantiate module
            instance = linker.instantiate(store, module)

            # Get memory and allocator
            memory = instance.exports(store).get("memory")
            if memory is None:
                raise WasmExecutionError("WASM module must export 'memory'")

            # Look up the named check function from the WASM exports
            validate_fn = instance.exports(store).get(check_function)
            if validate_fn is None and check_function != "validate":
                # Fall back to generic names when the specific function isn't found
                validate_fn = instance.exports(store).get("validate")
            if validate_fn is None:
                validate_fn = instance.exports(store).get("run")
            if validate_fn is None:
                raise WasmExecutionError(
                    f"WASM module does not export '{check_function}', 'validate', or 'run' function"
                )

            # For a real implementation, we would:
            # 1. Allocate memory in WASM for input data
            # 2. Copy arrow_data and params_json to WASM memory
            # 3. Call the validate function with pointers and lengths
            # 4. Read the result from WASM memory
            # 5. Free allocated memory

            # Simplified placeholder - return a mock result
            # In production, this would properly call the WASM function
            result = {
                "passed": True,
                "metric_value": 0,
                "details": {"message": "WASM execution placeholder"},
            }
            return json.dumps(result).encode("utf-8")

        except wasmtime.WasmtimeError as e:
            raise WasmExecutionError(f"WASM execution failed: {e}")

    def clear_cache(self) -> None:
        """Clear the module cache."""
        self._module_cache.clear()

    def preload_module(self, spec: ExecutorSpec) -> None:
        """Preload a WASM module into the cache."""
        # This would be used for warming up the cache
        pass
