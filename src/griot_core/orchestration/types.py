"""
Type definitions for orchestration.

Defines the data structures used for job splitting, dispatching,
and result aggregation in the orchestration layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class CheckRuntime(str, Enum):
    """Runtime type for checks."""

    WASM = "wasm"
    CONTAINER = "container"


class JobStatus(str, Enum):
    """Status of a dispatched job."""

    PENDING = "pending"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class CheckSpec:
    """
    Specification for a single check to be executed.

    Attributes:
        name: Unique name of the check
        executor_uri: URI to the executor (WASM module or container image)
        runtime: The runtime type (wasm or container)
        parameters: Parameters to pass to the executor
        severity: How critical this check is
        timeout_seconds: Maximum execution time
    """

    name: str
    executor_uri: str
    runtime: CheckRuntime
    parameters: dict[str, Any] = field(default_factory=dict)
    severity: str = "warning"
    timeout_seconds: int = 300

    @classmethod
    def from_executor_uri(cls, name: str, executor_uri: str, **kwargs) -> "CheckSpec":
        """Create a CheckSpec by inferring runtime from executor URI.

        Args:
            name: Check name
            executor_uri: URI to the executor
            **kwargs: Additional attributes

        Returns:
            CheckSpec with inferred runtime
        """
        # Infer runtime from URI scheme
        if executor_uri.startswith("oci://") or executor_uri.startswith("docker://"):
            runtime = CheckRuntime.CONTAINER
        elif executor_uri.startswith("registry://") or executor_uri.endswith(".wasm"):
            runtime = CheckRuntime.WASM
        else:
            # Default to WASM for registry executors
            runtime = CheckRuntime.WASM

        return cls(name=name, executor_uri=executor_uri, runtime=runtime, **kwargs)


@dataclass
class WasmJobSpec:
    """
    Specification for a WASM worker job.

    All WASM checks are batched into a single worker container
    that runs them sequentially using the WASM runtime.

    Attributes:
        job_id: Unique job identifier
        contract_id: Contract being validated
        contract_version: Version of the contract
        profile: Validation profile
        checks: List of WASM checks to execute
        data_reference: Reference to data (S3 URL, etc.)
        callback_url: URL to POST results back to
        timeout_seconds: Maximum execution time
        metadata: Additional job metadata
    """

    job_id: str
    contract_id: str
    contract_version: str
    profile: str
    checks: list[CheckSpec]
    data_reference: dict[str, Any]
    callback_url: str | None = None
    timeout_seconds: int = 600
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "job_id": self.job_id,
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "profile": self.profile,
            "checks": [
                {
                    "name": c.name,
                    "executor_uri": c.executor_uri,
                    "runtime": c.runtime.value,
                    "parameters": c.parameters,
                    "severity": c.severity,
                    "timeout_seconds": c.timeout_seconds,
                }
                for c in self.checks
            ],
            "data_reference": self.data_reference,
            "callback_url": self.callback_url,
            "timeout_seconds": self.timeout_seconds,
            "metadata": self.metadata,
        }


@dataclass
class ContainerJobSpec:
    """
    Specification for a single container check job.

    Each container check runs as a separate native K8s pod
    to avoid Docker-in-Docker.

    Attributes:
        job_id: Unique job identifier
        parent_job_id: ID of the parent orchestration job
        contract_id: Contract being validated
        contract_version: Version of the contract
        check: The container check to execute
        data_reference: Reference to data (S3 URL, etc.)
        callback_url: URL to POST results back to
        image: Container image to run
        timeout_seconds: Maximum execution time
        resource_limits: CPU/memory limits
        metadata: Additional job metadata
    """

    job_id: str
    parent_job_id: str
    contract_id: str
    contract_version: str
    check: CheckSpec
    data_reference: dict[str, Any]
    callback_url: str | None = None
    image: str | None = None
    timeout_seconds: int = 600
    resource_limits: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Extract image from check executor URI if not provided."""
        if self.image is None and self.check.executor_uri:
            uri = self.check.executor_uri
            if uri.startswith("oci://"):
                self.image = uri[6:]
            elif uri.startswith("docker://"):
                self.image = uri[9:]
            else:
                self.image = uri

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "job_id": self.job_id,
            "parent_job_id": self.parent_job_id,
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "check": {
                "name": self.check.name,
                "executor_uri": self.check.executor_uri,
                "runtime": self.check.runtime.value,
                "parameters": self.check.parameters,
                "severity": self.check.severity,
                "timeout_seconds": self.check.timeout_seconds,
            },
            "data_reference": self.data_reference,
            "callback_url": self.callback_url,
            "image": self.image,
            "timeout_seconds": self.timeout_seconds,
            "resource_limits": self.resource_limits,
            "metadata": self.metadata,
        }


@dataclass
class SplitJob:
    """
    Result of splitting a validation job.

    Contains the separated WASM and container jobs that will
    be dispatched in parallel.

    Attributes:
        parent_job_id: ID of the original validation job
        wasm_job: Optional WASM worker job (if any WASM checks)
        container_jobs: List of container jobs (one per container check)
        total_checks: Total number of checks in the original job
    """

    parent_job_id: str
    wasm_job: WasmJobSpec | None = None
    container_jobs: list[ContainerJobSpec] = field(default_factory=list)
    total_checks: int = 0

    @property
    def has_wasm_checks(self) -> bool:
        """Whether there are any WASM checks."""
        return self.wasm_job is not None and len(self.wasm_job.checks) > 0

    @property
    def has_container_checks(self) -> bool:
        """Whether there are any container checks."""
        return len(self.container_jobs) > 0

    @property
    def wasm_check_count(self) -> int:
        """Number of WASM checks."""
        return len(self.wasm_job.checks) if self.wasm_job else 0

    @property
    def container_check_count(self) -> int:
        """Number of container checks."""
        return len(self.container_jobs)


@dataclass
class CheckResultItem:
    """
    Result of a single check execution.

    Attributes:
        check_name: Name of the check
        passed: Whether the check passed
        runtime: Runtime used (wasm or container)
        metric_value: Computed metric value
        threshold: Applied threshold
        operator: Comparison operator used
        severity: Severity of the check
        execution_time_ms: Execution time in milliseconds
        error: Error message if failed
        details: Additional result details
    """

    check_name: str
    passed: bool
    runtime: CheckRuntime
    metric_value: float | None = None
    threshold: float | None = None
    operator: str | None = None
    severity: str = "warning"
    execution_time_ms: float | None = None
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "runtime": self.runtime.value,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "operator": self.operator,
            "severity": self.severity,
            "execution_time_ms": self.execution_time_ms,
            "error": self.error,
            "details": self.details,
        }


@dataclass
class DispatchResult:
    """
    Result of dispatching a job to a compute backend.

    Attributes:
        success: Whether dispatch succeeded
        job_id: The job ID that was dispatched
        job_type: Type of job (wasm_worker or container)
        backend: The backend that received the job
        invocation_id: Backend-specific invocation identifier
        error: Error message if dispatch failed
        dispatched_at: When the job was dispatched
    """

    success: bool
    job_id: str
    job_type: str  # "wasm_worker" or "container"
    backend: str
    invocation_id: str | None = None
    error: str | None = None
    dispatched_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "job_id": self.job_id,
            "job_type": self.job_type,
            "backend": self.backend,
            "invocation_id": self.invocation_id,
            "error": self.error,
            "dispatched_at": self.dispatched_at.isoformat(),
        }


@dataclass
class AggregatedResult:
    """
    Aggregated result from all parallel job executions.

    Combines results from the WASM worker and individual
    container checks into a single validation result.

    Attributes:
        job_id: Parent job ID
        contract_id: Contract that was validated
        contract_version: Version of the contract
        profile: Validation profile used
        is_valid: Overall validation result
        total_checks: Total number of checks executed
        passed_checks: Number of checks that passed
        failed_checks: Number of checks that failed
        check_results: Individual check results
        wasm_execution_time_ms: WASM worker execution time
        container_execution_time_ms: Total container execution time
        total_execution_time_ms: Total wall-clock time
        started_at: When validation started
        completed_at: When validation completed
        errors: List of error messages
        metadata: Additional result metadata
    """

    job_id: str
    contract_id: str
    contract_version: str
    profile: str
    is_valid: bool
    total_checks: int
    passed_checks: int
    failed_checks: int
    check_results: list[CheckResultItem]
    wasm_execution_time_ms: float | None = None
    container_execution_time_ms: float | None = None
    total_execution_time_ms: float | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_critical_failures(self) -> bool:
        """Whether any critical severity checks failed."""
        return any(not r.passed and r.severity == "critical" for r in self.check_results)

    @property
    def has_warnings(self) -> bool:
        """Whether any warning severity checks failed."""
        return any(not r.passed and r.severity == "warning" for r in self.check_results)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "job_id": self.job_id,
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "profile": self.profile,
            "is_valid": self.is_valid,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "check_results": [r.to_dict() for r in self.check_results],
            "wasm_execution_time_ms": self.wasm_execution_time_ms,
            "container_execution_time_ms": self.container_execution_time_ms,
            "total_execution_time_ms": self.total_execution_time_ms,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "errors": self.errors,
            "metadata": self.metadata,
        }
