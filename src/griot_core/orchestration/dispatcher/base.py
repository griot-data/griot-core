"""
Base classes for compute dispatchers.

Dispatchers are responsible for sending validation jobs to workers.
This new interface separates WASM worker dispatch from container dispatch
to avoid Docker-in-Docker issues in Kubernetes environments.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from griot_core.orchestration.types import (
    ContainerJobSpec,
    DispatchResult,
    WasmJobSpec,
)


class ComputeBackend(str, Enum):
    """Supported compute backends."""

    AWS_LAMBDA = "aws_lambda"
    KUBERNETES = "kubernetes"
    CLOUD_RUN = "cloud_run"
    LOCAL = "local"


@dataclass
class DispatcherConfig:
    """
    Configuration for a compute dispatcher.

    Attributes:
        backend: Compute backend type
        wasm_worker_image: Container image for WASM workers
        timeout_seconds: Default timeout for jobs
        memory_mb: Memory allocation (if supported)
        cpu_millicores: CPU allocation in millicores
        retry_count: Number of retries on failure
        environment: Environment variables for workers
        labels: Labels for K8s resources
    """

    backend: ComputeBackend
    wasm_worker_image: str = "griot/wasm-worker:latest"
    timeout_seconds: int = 600
    memory_mb: int = 512
    cpu_millicores: int = 1000
    retry_count: int = 3
    environment: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "backend": self.backend.value,
            "wasm_worker_image": self.wasm_worker_image,
            "timeout_seconds": self.timeout_seconds,
            "memory_mb": self.memory_mb,
            "cpu_millicores": self.cpu_millicores,
            "retry_count": self.retry_count,
            "environment": self.environment,
            "labels": self.labels,
        }


class ComputeDispatcher(ABC):
    """
    Abstract base class for compute dispatchers.

    This new interface separates WASM worker dispatch from container dispatch:

    - `dispatch_wasm_worker()`: Dispatches all WASM checks to a single griot-core
      worker container. The worker runs all WASM checks sequentially using the
      embedded WASM runtime.

    - `dispatch_container()`: Dispatches a single container check as a native
      pod/function. This avoids Docker-in-Docker by running containers directly
      on the compute backend.

    Example usage:
        dispatcher = KubernetesDispatcher(config)

        # Split job has WASM and container checks
        if split_job.has_wasm_checks:
            result = await dispatcher.dispatch_wasm_worker(split_job.wasm_job)

        for container_job in split_job.container_jobs:
            result = await dispatcher.dispatch_container(container_job)
    """

    def __init__(self, config: DispatcherConfig):
        """Initialize dispatcher with configuration.

        Args:
            config: Dispatcher configuration
        """
        self.config = config

    @property
    @abstractmethod
    def backend(self) -> ComputeBackend:
        """Return the compute backend type."""
        ...

    @abstractmethod
    async def dispatch_wasm_worker(self, spec: WasmJobSpec) -> DispatchResult:
        """
        Dispatch WASM checks to a griot-core worker container.

        Creates a single worker container that runs all WASM checks
        sequentially. The worker uses the embedded WASM runtime
        (no Docker/Podman needed inside the container).

        Args:
            spec: WASM job specification with all WASM checks

        Returns:
            DispatchResult indicating success or failure
        """
        ...

    @abstractmethod
    async def dispatch_container(self, spec: ContainerJobSpec) -> DispatchResult:
        """
        Dispatch a single container check as a native pod/function.

        Creates a native K8s pod, Lambda function, or Cloud Run job
        that runs the container check directly. This avoids
        Docker-in-Docker by letting the orchestrator spawn the container.

        Args:
            spec: Container job specification for a single check

        Returns:
            DispatchResult indicating success or failure
        """
        ...

    @abstractmethod
    async def check_status(self, invocation_id: str) -> dict[str, Any]:
        """
        Check the status of a dispatched job.

        Args:
            invocation_id: The invocation ID from DispatchResult

        Returns:
            Backend-specific status information
        """
        ...

    @abstractmethod
    async def cancel(self, invocation_id: str) -> bool:
        """
        Cancel a running job.

        Args:
            invocation_id: The invocation ID to cancel

        Returns:
            True if cancellation was successful
        """
        ...

    async def health_check(self) -> bool:
        """
        Check if the compute backend is healthy.

        Returns:
            True if the backend is available
        """
        return True

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(backend={self.backend.value})"
