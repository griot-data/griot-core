"""
Validation result types.

Defines the result structures returned by the validation engine
after running checks against data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from griot_core.models.enums import Severity


class ValidationMode(str, Enum):
    """Mode of validation execution."""

    FULL = "full"  # Run all checks
    SAMPLE = "sample"  # Run on sample data only
    SCHEMA_ONLY = "schema_only"  # Only validate schema structure
    QUICK = "quick"  # Run critical checks only


class CheckStatus(str, Enum):
    """Status of a check execution."""

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class CheckExecutionResult:
    """
    Result of executing a single check.

    Attributes:
        check_name: Name of the check that was executed
        status: Execution status (passed, failed, skipped, error)
        passed: Whether the check passed (alias for status == PASSED)
        severity: Severity level of the check
        metric_value: The computed metric value
        threshold: The threshold that was applied
        operator: The comparison operator used
        details: Additional details about the check
        samples: Sample rows that failed (for debugging)
        error_message: Error message if status is ERROR
        execution_time_ms: How long the check took to run
        executor_id: ID of the executor that ran the check
        executor_version: Version of the executor
    """

    check_name: str
    status: CheckStatus
    severity: Severity = Severity.WARNING
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    operator: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    samples: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None
    execution_time_ms: Optional[float] = None
    executor_id: Optional[str] = None
    executor_version: Optional[str] = None

    @property
    def passed(self) -> bool:
        """Whether the check passed."""
        return self.status == CheckStatus.PASSED

    @property
    def failed(self) -> bool:
        """Whether the check failed."""
        return self.status == CheckStatus.FAILED

    @property
    def is_critical(self) -> bool:
        """Whether this is a critical check."""
        return self.severity == Severity.CRITICAL


@dataclass
class SchemaValidationResult:
    """
    Result of validating a single schema.

    Attributes:
        schema_id: ID of the schema that was validated
        schema_name: Name of the schema
        is_valid: Whether all checks passed
        check_results: Results of individual check executions
        total_checks: Total number of checks run
        passed_checks: Number of checks that passed
        failed_checks: Number of checks that failed
        critical_failures: Number of critical checks that failed
        warnings: Number of warning-level failures
        skipped_checks: Number of checks that were skipped
    """

    schema_id: str
    schema_name: str
    is_valid: bool
    check_results: List[CheckExecutionResult] = field(default_factory=list)
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    critical_failures: int = 0
    warnings: int = 0
    skipped_checks: int = 0

    def add_result(self, result: CheckExecutionResult) -> None:
        """Add a check result and update counts."""
        self.check_results.append(result)
        self.total_checks += 1

        if result.status == CheckStatus.PASSED:
            self.passed_checks += 1
        elif result.status == CheckStatus.FAILED:
            self.failed_checks += 1
            if result.severity == Severity.CRITICAL:
                self.critical_failures += 1
                self.is_valid = False
            elif result.severity == Severity.WARNING:
                self.warnings += 1
        elif result.status == CheckStatus.SKIPPED:
            self.skipped_checks += 1
        elif result.status == CheckStatus.ERROR:
            self.failed_checks += 1
            if result.severity == Severity.CRITICAL:
                self.critical_failures += 1
                self.is_valid = False

    def get_failed_checks(self) -> List[CheckExecutionResult]:
        """Get all failed check results."""
        return [r for r in self.check_results if r.status == CheckStatus.FAILED]

    def get_critical_failures(self) -> List[CheckExecutionResult]:
        """Get all critical failures."""
        return [
            r
            for r in self.check_results
            if r.status == CheckStatus.FAILED and r.severity == Severity.CRITICAL
        ]


@dataclass
class ValidationSummary:
    """
    Summary of a validation run.

    Provides high-level statistics about the validation.
    """

    total_schemas: int = 0
    valid_schemas: int = 0
    invalid_schemas: int = 0
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    critical_failures: int = 0
    warnings: int = 0
    skipped_checks: int = 0
    error_checks: int = 0

    @property
    def pass_rate(self) -> float:
        """Calculate the overall pass rate."""
        if self.total_checks == 0:
            return 1.0
        return self.passed_checks / self.total_checks


@dataclass
class ValidationResult:
    """
    Complete result of a validation run.

    This is the top-level result returned by the validation engine
    after validating a contract against data.

    Attributes:
        is_valid: Whether the validation passed (no critical failures)
        contract_id: ID of the contract that was validated
        contract_version: Version of the contract
        profile_used: The execution profile that was used
        mode: The validation mode that was used
        schema_results: Results for each schema in the contract
        summary: Summary statistics
        inheritance_chain: Contract inheritance chain (if resolved)
        started_at: When validation started
        completed_at: When validation completed
        duration_ms: Total duration in milliseconds
        errors: Any errors that occurred during validation
        warnings: Any warnings from validation
    """

    is_valid: bool
    contract_id: str
    contract_version: str = "1.0.0"
    profile_used: str = "default"
    mode: ValidationMode = ValidationMode.FULL
    schema_results: List[SchemaValidationResult] = field(default_factory=list)
    summary: ValidationSummary = field(default_factory=ValidationSummary)
    inheritance_chain: List[str] = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_schema_result(self, result: SchemaValidationResult) -> None:
        """Add a schema result and update summary."""
        self.schema_results.append(result)

        # Update summary
        self.summary.total_schemas += 1
        if result.is_valid:
            self.summary.valid_schemas += 1
        else:
            self.summary.invalid_schemas += 1
            self.is_valid = False

        self.summary.total_checks += result.total_checks
        self.summary.passed_checks += result.passed_checks
        self.summary.failed_checks += result.failed_checks
        self.summary.critical_failures += result.critical_failures
        self.summary.warnings += result.warnings
        self.summary.skipped_checks += result.skipped_checks

    def get_schema_result(self, schema_id: str) -> Optional[SchemaValidationResult]:
        """Get the result for a specific schema."""
        for result in self.schema_results:
            if result.schema_id == schema_id:
                return result
        return None

    def get_all_failures(self) -> List[CheckExecutionResult]:
        """Get all failed checks across all schemas."""
        failures = []
        for schema_result in self.schema_results:
            failures.extend(schema_result.get_failed_checks())
        return failures

    def get_all_critical_failures(self) -> List[CheckExecutionResult]:
        """Get all critical failures across all schemas."""
        failures = []
        for schema_result in self.schema_results:
            failures.extend(schema_result.get_critical_failures())
        return failures

    def to_dict(self) -> Dict[str, Any]:
        """Convert the result to a dictionary."""
        return {
            "is_valid": self.is_valid,
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "profile_used": self.profile_used,
            "mode": self.mode.value,
            "summary": {
                "total_schemas": self.summary.total_schemas,
                "valid_schemas": self.summary.valid_schemas,
                "invalid_schemas": self.summary.invalid_schemas,
                "total_checks": self.summary.total_checks,
                "passed_checks": self.summary.passed_checks,
                "failed_checks": self.summary.failed_checks,
                "critical_failures": self.summary.critical_failures,
                "warnings": self.summary.warnings,
                "pass_rate": self.summary.pass_rate,
            },
            "schema_results": [
                {
                    "schema_id": sr.schema_id,
                    "schema_name": sr.schema_name,
                    "is_valid": sr.is_valid,
                    "total_checks": sr.total_checks,
                    "passed_checks": sr.passed_checks,
                    "failed_checks": sr.failed_checks,
                    "critical_failures": sr.critical_failures,
                }
                for sr in self.schema_results
            ],
            "inheritance_chain": self.inheritance_chain,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
            "errors": self.errors,
            "warnings": self.warnings,
        }
