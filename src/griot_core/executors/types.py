"""
Executor types and result models.

This module defines the types used by the executor runtime, including
ExecutorSpec for describing executors and CheckResult for validation results.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from griot_core.models.enums import Runtime


@dataclass
class ExecutorSpec:
    """
    Specification for an executor.

    Executors are versioned, portable validation modules that can be
    either WASM modules or container images.

    Attributes:
        id: Unique identifier for this executor (e.g., "null-check")
        version: Semantic version (e.g., "1.0.0")
        runtime: The runtime type (wasm or container)
        artifact_url: URL to the executable artifact
                     - WASM: URL to .wasm file
                     - Container: OCI image reference
        description: Human-readable description
        input_schema: JSON Schema describing expected parameters
        output_schema: JSON Schema describing the result format
        tags: Tags for categorization
    """

    id: str
    version: str
    runtime: Runtime
    artifact_url: str
    description: str = ""
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    tags: List[str] = field(default_factory=list)

    @property
    def uri(self) -> str:
        """Get the registry URI for this executor."""
        return f"registry://executors/{self.id}@{self.version}"


@dataclass
class CheckResult:
    """
    Result of a check execution.

    Returned by executors after validating data.

    Attributes:
        passed: Whether the check passed
        metric_value: The computed metric value (e.g., null count, duplicate count)
        threshold: The threshold that was applied
        operator: The comparison operator used (e.g., "lte", "gte", "eq")
        details: Additional details about the check execution
        samples: Sample rows that failed the check (for debugging)
        error: Error message if the check failed to execute
    """

    passed: bool
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    operator: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    samples: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None

    @classmethod
    def from_json(cls, data: bytes) -> "CheckResult":
        """Parse a CheckResult from JSON bytes."""
        parsed = json.loads(data)
        return cls(
            passed=parsed.get("passed", False),
            metric_value=parsed.get("metric_value"),
            threshold=parsed.get("threshold"),
            operator=parsed.get("operator"),
            details=parsed.get("details", {}),
            samples=parsed.get("samples", []),
            error=parsed.get("error"),
        )

    def to_json(self) -> bytes:
        """Serialize this CheckResult to JSON bytes."""
        return json.dumps(
            {
                "passed": self.passed,
                "metric_value": self.metric_value,
                "threshold": self.threshold,
                "operator": self.operator,
                "details": self.details,
                "samples": self.samples,
                "error": self.error,
            }
        ).encode()


@dataclass
class ExecutorResult:
    """
    Full result from an executor invocation.

    Includes the check result plus execution metadata.

    Attributes:
        check_result: The validation result
        executor_id: ID of the executor that ran
        executor_version: Version of the executor
        execution_time_ms: How long execution took in milliseconds
        memory_used_bytes: Memory used during execution
        runtime: The runtime used (wasm or container)
    """

    check_result: CheckResult
    executor_id: str
    executor_version: str
    execution_time_ms: Optional[float] = None
    memory_used_bytes: Optional[int] = None
    runtime: Optional[Runtime] = None
