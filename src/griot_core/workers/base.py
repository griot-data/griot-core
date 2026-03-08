"""
Base worker types and protocols.

Defines the common interface for all worker implementations.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol


class WorkerStatus(str, Enum):
    """Status of a worker execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class JobPayload:
    """
    Payload for a validation job.

    This is the data structure passed to workers from the registry.

    Attributes:
        job_id: Unique job identifier
        contract_id: Contract to validate
        contract_version: Version of the contract
        profile: Execution profile
        environment: Environment (production, staging, dev)
        arrow_data: Pre-fetched Arrow IPC data keyed by schema ID
        options: Additional validation options
        callback_url: URL to report results back to registry
        timeout_seconds: Maximum execution time
        metadata: Additional job metadata
    """

    job_id: str
    contract_id: str
    contract_version: Optional[str] = None
    profile: str = "default"
    environment: str = "production"
    arrow_data: Optional[Dict[str, bytes]] = None
    options: Dict[str, Any] = field(default_factory=dict)
    callback_url: Optional[str] = None
    timeout_seconds: int = 300
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data: Dict[str, Any] = {
            "job_id": self.job_id,
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "profile": self.profile,
            "environment": self.environment,
            "options": self.options,
            "callback_url": self.callback_url,
            "timeout_seconds": self.timeout_seconds,
            "metadata": self.metadata,
        }
        # Arrow data is binary, handle separately
        if self.arrow_data:
            data["arrow_data_keys"] = list(self.arrow_data.keys())
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobPayload":
        """Create from dictionary."""
        return cls(
            job_id=data["job_id"],
            contract_id=data["contract_id"],
            contract_version=data.get("contract_version"),
            profile=data.get("profile", "default"),
            environment=data.get("environment", "production"),
            arrow_data=data.get("arrow_data"),
            options=data.get("options", {}),
            callback_url=data.get("callback_url"),
            timeout_seconds=data.get("timeout_seconds", 300),
            metadata=data.get("metadata", {}),
        )

    def to_json(self) -> str:
        """Convert to JSON string (without binary data)."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "JobPayload":
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class WorkerResult:
    """
    Result from a worker execution.

    Attributes:
        job_id: Job identifier
        status: Execution status
        is_valid: Whether validation passed
        started_at: When execution started
        completed_at: When execution completed
        duration_ms: Execution duration in milliseconds
        validation_result: Full validation result as dict
        errors: List of error messages if failed
        worker_id: Identifier of the worker that executed
        worker_type: Type of worker (local, lambda, k8s, cloudrun)
        metadata: Additional result metadata
    """

    job_id: str
    status: WorkerStatus
    is_valid: bool = False
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    validation_result: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)
    worker_id: Optional[str] = None
    worker_type: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "is_valid": self.is_valid,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
            "validation_result": self.validation_result,
            "errors": self.errors,
            "worker_id": self.worker_id,
            "worker_type": self.worker_type,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkerResult":
        """Create from dictionary."""
        started_at = None
        if data.get("started_at"):
            started_at = datetime.fromisoformat(data["started_at"])

        completed_at = None
        if data.get("completed_at"):
            completed_at = datetime.fromisoformat(data["completed_at"])

        return cls(
            job_id=data["job_id"],
            status=WorkerStatus(data["status"]),
            is_valid=data.get("is_valid", False),
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=data.get("duration_ms"),
            validation_result=data.get("validation_result"),
            errors=data.get("errors", []),
            worker_id=data.get("worker_id"),
            worker_type=data.get("worker_type"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class WorkerConfig:
    """
    Configuration for a worker.

    Attributes:
        worker_id: Unique worker identifier
        worker_type: Type of worker
        max_concurrent_jobs: Maximum concurrent jobs
        default_timeout: Default timeout in seconds
        registry_url: URL of the Griot registry
        callback_enabled: Whether to report results via callback
        extra_options: Additional worker-specific options
    """

    worker_id: str
    worker_type: str = "local"
    max_concurrent_jobs: int = 1
    default_timeout: int = 300
    registry_url: Optional[str] = None
    callback_enabled: bool = True
    extra_options: Dict[str, Any] = field(default_factory=dict)


class ContractFetcher(Protocol):
    """Protocol for fetching contracts."""

    def fetch(self, contract_id: str, version: Optional[str] = None) -> Dict[str, Any]:
        """Fetch a contract by ID and optional version."""
        ...


class Worker(ABC):
    """
    Abstract base class for workers.

    Workers execute validation jobs received from the registry.
    They use the ValidationEngine to run checks and report results.
    """

    def __init__(self, config: WorkerConfig):
        """
        Initialize the worker.

        Args:
            config: Worker configuration
        """
        self.config = config

    @abstractmethod
    async def execute(self, payload: JobPayload) -> WorkerResult:
        """
        Execute a validation job.

        Args:
            payload: Job payload with contract and data

        Returns:
            WorkerResult with validation results
        """
        ...

    @abstractmethod
    async def report_result(self, result: WorkerResult) -> bool:
        """
        Report results back to the registry.

        Args:
            result: Worker result to report

        Returns:
            True if report was successful
        """
        ...

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Check worker health.

        Returns:
            Health status dictionary
        """
        ...
