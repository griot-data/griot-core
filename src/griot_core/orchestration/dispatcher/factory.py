"""
Factory for creating compute dispatchers.

Creates the appropriate dispatcher based on backend configuration.
"""

from __future__ import annotations

import logging
from typing import Any

from griot_core.orchestration.dispatcher.base import (
    ComputeBackend,
    ComputeDispatcher,
    DispatcherConfig,
)

logger = logging.getLogger(__name__)


def create_dispatcher(
    config: DispatcherConfig,
    **kwargs: Any,
) -> ComputeDispatcher:
    """
    Create a compute dispatcher based on configuration.

    Factory function that instantiates the appropriate dispatcher
    for the configured backend.

    Args:
        config: Dispatcher configuration specifying the backend
        **kwargs: Additional backend-specific arguments

    Returns:
        Configured ComputeDispatcher instance

    Raises:
        ValueError: If the backend is not supported

    Example:
        # Kubernetes dispatcher
        config = DispatcherConfig(
            backend=ComputeBackend.KUBERNETES,
            wasm_worker_image="griot/wasm-worker:v1.0",
        )
        dispatcher = create_dispatcher(
            config,
            namespace="griot",
        )

        # Lambda dispatcher
        config = DispatcherConfig(
            backend=ComputeBackend.AWS_LAMBDA,
        )
        dispatcher = create_dispatcher(
            config,
            wasm_function_name="griot-wasm-worker",
        )
    """
    backend = config.backend

    if backend == ComputeBackend.KUBERNETES:
        from griot_core.orchestration.dispatcher.kubernetes import KubernetesDispatcher

        return KubernetesDispatcher(
            config,
            namespace=kwargs.get("namespace", "default"),
            service_account=kwargs.get("service_account"),
        )

    elif backend == ComputeBackend.AWS_LAMBDA:
        from griot_core.orchestration.dispatcher.lambda_dispatcher import (
            LambdaDispatcher,
        )

        return LambdaDispatcher(
            config,
            wasm_function_name=kwargs.get("wasm_function_name", "griot-wasm-worker"),
            container_function_prefix=kwargs.get("container_function_prefix", "griot-check-"),
        )

    elif backend == ComputeBackend.CLOUD_RUN:
        from griot_core.orchestration.dispatcher.cloudrun import CloudRunDispatcher

        return CloudRunDispatcher(
            config,
            project_id=kwargs.get("project_id"),
            region=kwargs.get("region", "us-central1"),
            wasm_service_url=kwargs.get("wasm_service_url"),
            use_auth=kwargs.get("use_auth", True),
        )

    elif backend == ComputeBackend.LOCAL:
        from griot_core.orchestration.dispatcher.local import LocalDispatcher

        return LocalDispatcher(
            config,
            max_workers=kwargs.get("max_workers", 4),
            callback_timeout=kwargs.get("callback_timeout", 30.0),
        )

    else:
        raise ValueError(
            f"Unsupported compute backend: {backend}. "
            f"Supported: {[b.value for b in ComputeBackend]}"
        )


def create_dispatcher_from_dict(
    config_dict: dict[str, Any],
    **kwargs: Any,
) -> ComputeDispatcher:
    """
    Create a dispatcher from a configuration dictionary.

    Convenience function that creates a DispatcherConfig from a dict
    and then creates the appropriate dispatcher.

    Args:
        config_dict: Dictionary with configuration values
        **kwargs: Additional backend-specific arguments

    Returns:
        Configured ComputeDispatcher instance

    Example:
        config = {
            "backend": "kubernetes",
            "wasm_worker_image": "griot/wasm-worker:v1.0",
            "timeout_seconds": 600,
        }
        dispatcher = create_dispatcher_from_dict(
            config,
            namespace="griot",
        )
    """
    backend_str = config_dict.get("backend", "local")
    try:
        backend = ComputeBackend(backend_str)
    except ValueError:
        raise ValueError(
            f"Invalid backend: {backend_str}. Supported: {[b.value for b in ComputeBackend]}"
        )

    config = DispatcherConfig(
        backend=backend,
        wasm_worker_image=config_dict.get("wasm_worker_image", "griot/wasm-worker:latest"),
        timeout_seconds=config_dict.get("timeout_seconds", 600),
        memory_mb=config_dict.get("memory_mb", 512),
        cpu_millicores=config_dict.get("cpu_millicores", 1000),
        retry_count=config_dict.get("retry_count", 3),
        environment=config_dict.get("environment", {}),
        labels=config_dict.get("labels", {}),
    )

    return create_dispatcher(config, **kwargs)
