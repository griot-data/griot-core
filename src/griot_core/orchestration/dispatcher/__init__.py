"""
Compute dispatchers for orchestrated validation jobs.

This module provides the dispatcher implementations for different
compute backends (Kubernetes, Lambda, Cloud Run, Local).

The key difference from the old griot-registry dispatchers:

Old approach (Docker-in-Docker):
    Registry -> K8s Pod (worker) -> Docker/Podman (container check)

New approach (Native pods):
    Registry -> Orchestrator -> K8s Pod (WASM worker)
                             -> K8s Pod (container check A)
                             -> K8s Pod (container check B)
"""

from griot_core.orchestration.dispatcher.base import (
    ComputeBackend,
    ComputeDispatcher,
    DispatcherConfig,
)
from griot_core.orchestration.dispatcher.cloudrun import CloudRunDispatcher
from griot_core.orchestration.dispatcher.factory import (
    create_dispatcher,
    create_dispatcher_from_dict,
)
from griot_core.orchestration.dispatcher.kubernetes import KubernetesDispatcher
from griot_core.orchestration.dispatcher.lambda_dispatcher import LambdaDispatcher
from griot_core.orchestration.dispatcher.local import LocalDispatcher

__all__ = [
    # Base classes
    "ComputeBackend",
    "ComputeDispatcher",
    "DispatcherConfig",
    # Factory
    "create_dispatcher",
    "create_dispatcher_from_dict",
    # Implementations
    "KubernetesDispatcher",
    "LambdaDispatcher",
    "CloudRunDispatcher",
    "LocalDispatcher",
]
