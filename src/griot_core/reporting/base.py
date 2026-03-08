"""
Base protocol and data types for the reporting framework.

Defines the ToolResultsParser protocol that all tool parsers must implement,
and the tool-agnostic data types that flow through the reporting pipeline.

Follows the DataConnector Protocol pattern from connectors/base.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Protocol, runtime_checkable


@runtime_checkable
class ToolResultsParser(Protocol):
    """Protocol for parsing test/validation results from external tools.

    Implement this to add support for a new ETL/testing framework.
    Each implementation converts a tool's native output format into
    the standard ToolValidationReport.

    Example implementation:

        class SodaResultsParser:
            @property
            def tool_name(self) -> str:
                return "soda"

            def parse(self, results_path: Path) -> ToolValidationReport:
                # Read soda scan results and convert...
                ...

            def supports_file(self, path: Path) -> bool:
                return path.suffix in (".yml", ".yaml")
    """

    @property
    def tool_name(self) -> str:
        """The name of the tool (e.g., 'dbt', 'soda', 'great_expectations')."""
        ...

    def parse(self, results_path: Path) -> "ToolValidationReport":
        """Parse results file and return a structured report.

        Args:
            results_path: Path to the tool's output file.

        Returns:
            ToolValidationReport with parsed results.

        Raises:
            FileNotFoundError: If results file does not exist.
            ValueError: If results file cannot be parsed.
        """
        ...

    def supports_file(self, path: Path) -> bool:
        """Check if this parser can handle the given file.

        Args:
            path: Path to check.

        Returns:
            True if this parser can handle the file.
        """
        ...


@dataclass
class ToolCheckResult:
    """Individual check/test result from an external tool — tool-agnostic.

    Aligns field naming with orchestration.types.CheckResultItem and
    validation.result.CheckExecutionResult for consistency across layers.
    Adds tool-specific fields (column, table, asset_id) needed for
    external tool results (dbt, Soda, etc.).

    Named ToolCheckResult (not CheckResult) to avoid collision with
    executors.types.CheckResult which serves the internal executor layer.
    """

    check_name: str
    status: str  # "pass"/"fail"/"warn"/"error"/"skip"
    column: str | None = None
    table: str | None = None
    id: str | None = None  # check run identifier (e.g. dbt hash suffix)
    execution_time_ms: float = 0.0
    severity: str = "warning"
    failures: int = 0
    metric_value: float | None = None
    threshold: float | None = None
    operator: str | None = None
    error: str | None = None
    compiled_code: str | None = None  # SQL (only for failures)
    asset_id: str | None = None  # registry data asset UUID
    is_source_test: bool = False
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary — matches CheckResultItem.to_dict() pattern."""
        result: Dict[str, Any] = {
            "check_name": self.check_name,
            "status": self.status,
            "execution_time_ms": self.execution_time_ms,
            "severity": self.severity,
            "failures": self.failures,
        }
        # Only include non-None optional fields to keep payload compact
        if self.column is not None:
            result["column"] = self.column
        if self.table is not None:
            result["table"] = self.table
        if self.id is not None:
            result["id"] = self.id
        if self.metric_value is not None:
            result["metric_value"] = self.metric_value
        if self.threshold is not None:
            result["threshold"] = self.threshold
        if self.operator is not None:
            result["operator"] = self.operator
        if self.error is not None:
            result["error"] = self.error
        if self.compiled_code is not None:
            result["compiled_code"] = self.compiled_code
        if self.asset_id is not None:
            result["asset_id"] = self.asset_id
        if self.is_source_test:
            result["is_source_test"] = True
        if self.details:
            result["details"] = self.details
        return result


@dataclass
class SchemaTestResult:
    """Test results for a single schema -- tool-agnostic.

    Attributes:
        schema_name: Contract schema name (e.g., "products").
        total: Total number of tests.
        passed: Number of tests that passed.
        failed: Number of tests that failed.
        warned: Number of tests that warned.
        errored: Number of tests that errored.
        skipped: Number of tests that were skipped.
        duration_ms: Total duration in milliseconds.
        errors: List of error details for failures.
        check_results: Individual check-level results.
    """

    schema_name: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    warned: int = 0
    errored: int = 0
    skipped: int = 0
    duration_ms: float = 0.0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    check_results: List[ToolCheckResult] = field(default_factory=list)


@dataclass
class ToolValidationReport:
    """Structured output from parsing any tool's results -- the universal format.

    This is the intermediate representation that flows between the parser
    and the reporter. It is completely tool-agnostic.

    Attributes:
        tool: Tool name (e.g., "dbt", "soda").
        schemas: Per-schema test results, keyed by contract schema name.
        overall: Aggregated totals across all schemas.
        all_passed: True if no tests failed or errored.
        metadata: Tool-specific metadata (version, elapsed time, etc.).
    """

    tool: str
    schemas: Dict[str, SchemaTestResult] = field(default_factory=dict)
    overall: SchemaTestResult = field(
        default_factory=lambda: SchemaTestResult(schema_name="_overall")
    )
    all_passed: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
