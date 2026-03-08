"""
Griot Core Executor Runtime.

This module provides the executor runtime for running validation checks.
Executors can be WASM modules or container images that receive Arrow IPC
data and return CheckResult.
"""

from __future__ import annotations

from .container_runtime import ContainerConfig, ContainerExecutionResult, ContainerRuntime
from .registry import ExecutorNotFoundError, ExecutorRegistry, InvalidExecutorURIError
from .runtime import ExecutorRuntime, RuntimeCapabilities
from .types import CheckResult, ExecutorResult, ExecutorSpec
from .wasm_runtime import WasmExecutionResult, WasmRuntime

__all__ = [
    # Types
    "CheckResult",
    "ExecutorResult",
    "ExecutorSpec",
    # Registry
    "ExecutorRegistry",
    "ExecutorNotFoundError",
    "InvalidExecutorURIError",
    # WASM Runtime
    "WasmRuntime",
    "WasmExecutionResult",
    # Container Runtime
    "ContainerRuntime",
    "ContainerConfig",
    "ContainerExecutionResult",
    # Unified Runtime
    "ExecutorRuntime",
    "RuntimeCapabilities",
]
