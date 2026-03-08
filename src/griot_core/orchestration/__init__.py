"""
Griot Core Orchestration Module.

This module provides compute orchestration for validation jobs,
including smart job splitting to avoid Docker-in-Docker issues.

Architecture:
    Contract with mixed checks
            |
            v
    ┌──────────────────┐
    │   JobSplitter    │  Separates WASM vs Container checks
    └──────────────────┘
            |
            ├──► WASM checks ────► griot-core worker pod (runs all WASM)
            |                              |
            ├──► Container check A ──► K8s pod (native)  |
            |                                            ├──► PARALLEL
            ├──► Container check B ──► K8s pod (native)  |
            |                                            |
            v                                            |
    ┌──────────────────┐                                 |
    │ ResultAggregator │ ◄───────────────────────────────┘
    └──────────────────┘
            |
            v
      AggregatedResult

Key components:
- ValidationOrchestrator: Main entry point for orchestrated validation
- JobSplitter: Separates WASM and container checks
- ResultAggregator: Combines results from parallel executions
- ComputeDispatcher: Interface for compute backends (K8s, Lambda, etc.)

Example:
    from griot_core.orchestration import (
        ValidationOrchestrator,
        DispatcherConfig,
        ComputeBackend,
    )

    # Configure for Kubernetes
    config = DispatcherConfig(
        backend=ComputeBackend.KUBERNETES,
        wasm_worker_image="griot/wasm-worker:v1.0",
    )

    orchestrator = ValidationOrchestrator(
        dispatcher_config=config,
        namespace="griot",
    )

    # Execute validation
    result = await orchestrator.validate(
        contract=my_contract,
        profile="data_engineering",
        data_reference={"s3": "s3://bucket/data.parquet"},
    )
"""

from griot_core.orchestration.aggregator import ResultAggregator
from griot_core.orchestration.dispatcher import (
    CloudRunDispatcher,
    ComputeBackend,
    ComputeDispatcher,
    DispatcherConfig,
    KubernetesDispatcher,
    LambdaDispatcher,
    LocalDispatcher,
    create_dispatcher,
    create_dispatcher_from_dict,
)
from griot_core.orchestration.orchestrator import ValidationOrchestrator
from griot_core.orchestration.splitter import JobSplitter
from griot_core.orchestration.types import (
    AggregatedResult,
    CheckResultItem,
    CheckRuntime,
    CheckSpec,
    ContainerJobSpec,
    DispatchResult,
    JobStatus,
    SplitJob,
    WasmJobSpec,
)

__all__ = [
    # Types
    "AggregatedResult",
    "CheckResultItem",
    "CheckRuntime",
    "CheckSpec",
    "ContainerJobSpec",
    "DispatchResult",
    "JobStatus",
    "SplitJob",
    "WasmJobSpec",
    # Splitter and Aggregator
    "JobSplitter",
    "ResultAggregator",
    # Orchestrator
    "ValidationOrchestrator",
    # Dispatcher
    "ComputeBackend",
    "ComputeDispatcher",
    "DispatcherConfig",
    "create_dispatcher",
    "create_dispatcher_from_dict",
    # Dispatcher implementations
    "KubernetesDispatcher",
    "LambdaDispatcher",
    "CloudRunDispatcher",
    "LocalDispatcher",
]
