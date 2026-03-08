"""
Result aggregator for combining parallel execution results.

The ResultAggregator collects results from WASM worker and container
checks executed in parallel, combining them into a single AggregatedResult.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from griot_core.orchestration.types import (
    AggregatedResult,
    CheckResultItem,
    CheckRuntime,
    SplitJob,
)

logger = logging.getLogger(__name__)


class ResultAggregator:
    """
    Aggregates results from parallel WASM and container executions.

    Collects results as they complete and combines them into a final
    AggregatedResult when all jobs have finished.

    Example:
        aggregator = ResultAggregator(split_job)

        # As results come in from callbacks
        aggregator.add_wasm_result(wasm_callback_data)
        aggregator.add_container_result(container_1_callback_data)
        aggregator.add_container_result(container_2_callback_data)

        # When all complete
        if aggregator.is_complete:
            result = aggregator.aggregate()
    """

    def __init__(self, split_job: SplitJob):
        """
        Initialize the aggregator.

        Args:
            split_job: The split job containing expected WASM and container jobs
        """
        self.split_job = split_job
        self.started_at: datetime | None = None
        self.completed_at: datetime | None = None

        # Track expected jobs
        self._wasm_job_id = split_job.wasm_job.job_id if split_job.wasm_job else None
        self._container_job_ids = {j.job_id for j in split_job.container_jobs}

        # Store results
        self._wasm_result: dict[str, Any] | None = None
        self._container_results: dict[str, dict[str, Any]] = {}

        # Track errors
        self._errors: list[str] = []

    @property
    def is_complete(self) -> bool:
        """Whether all expected results have been received."""
        # Check WASM result if expected
        if self._wasm_job_id and self._wasm_result is None:
            return False

        # Check all container results
        received = set(self._container_results.keys())
        expected = self._container_job_ids
        return received == expected

    @property
    def pending_jobs(self) -> list[str]:
        """List of job IDs that haven't completed yet."""
        pending = []

        if self._wasm_job_id and self._wasm_result is None:
            pending.append(self._wasm_job_id)

        for job_id in self._container_job_ids:
            if job_id not in self._container_results:
                pending.append(job_id)

        return pending

    def start(self) -> None:
        """Mark the aggregation as started."""
        self.started_at = datetime.utcnow()

    def add_wasm_result(self, result: dict[str, Any]) -> None:
        """
        Add result from WASM worker.

        Args:
            result: Callback data from WASM worker
        """
        job_id = result.get("job_id")

        if job_id != self._wasm_job_id:
            logger.warning(
                "Received WASM result for unexpected job %s (expected %s)",
                job_id,
                self._wasm_job_id,
            )
            return

        self._wasm_result = result
        logger.info("Added WASM result for job %s", job_id)

        if result.get("error"):
            self._errors.append(f"WASM worker error: {result['error']}")

    def add_container_result(self, result: dict[str, Any]) -> None:
        """
        Add result from container check.

        Args:
            result: Callback data from container check
        """
        job_id = result.get("job_id")

        if job_id not in self._container_job_ids:
            logger.warning(
                "Received container result for unexpected job %s",
                job_id,
            )
            return

        self._container_results[job_id] = result
        logger.info(
            "Added container result for job %s (%d/%d)",
            job_id,
            len(self._container_results),
            len(self._container_job_ids),
        )

        if result.get("error"):
            check_name = result.get("check_name", "unknown")
            self._errors.append(f"Container check '{check_name}' error: {result['error']}")

    def add_result(self, result: dict[str, Any]) -> None:
        """
        Add a result, automatically routing to WASM or container handler.

        Args:
            result: Callback data
        """
        job_type = result.get("job_type")
        job_id = result.get("job_id")

        if job_type == "wasm_worker" or job_id == self._wasm_job_id:
            self.add_wasm_result(result)
        else:
            self.add_container_result(result)

    def aggregate(self) -> AggregatedResult:
        """
        Aggregate all results into a final result.

        Returns:
            AggregatedResult combining all check results
        """
        self.completed_at = datetime.utcnow()

        # Collect all check results
        check_results: list[CheckResultItem] = []

        # Process WASM results
        wasm_execution_time: float | None = None
        if self._wasm_result:
            wasm_execution_time = self._wasm_result.get("duration_ms")
            wasm_checks = self._wasm_result.get("check_results", [])

            for check_data in wasm_checks:
                check_results.append(
                    CheckResultItem(
                        check_name=check_data.get("check_name", "unknown"),
                        passed=check_data.get("passed", False),
                        runtime=CheckRuntime.WASM,
                        metric_value=check_data.get("metric_value"),
                        threshold=check_data.get("threshold"),
                        operator=check_data.get("operator"),
                        severity=check_data.get("severity", "warning"),
                        execution_time_ms=check_data.get("execution_time_ms"),
                        error=check_data.get("error"),
                        details=check_data.get("details", {}),
                    )
                )

        # Process container results
        container_execution_time: float = 0
        for job_id, result in self._container_results.items():
            check_data = result.get("check_result", {})

            # Add individual execution time
            exec_time = result.get("duration_ms")
            if exec_time:
                container_execution_time = max(container_execution_time, exec_time)

            check_results.append(
                CheckResultItem(
                    check_name=result.get("check_name", "unknown"),
                    passed=check_data.get("passed", False),
                    runtime=CheckRuntime.CONTAINER,
                    metric_value=check_data.get("metric_value"),
                    threshold=check_data.get("threshold"),
                    operator=check_data.get("operator"),
                    severity=check_data.get("severity", "warning"),
                    execution_time_ms=exec_time,
                    error=check_data.get("error") or result.get("error"),
                    details=check_data.get("details", {}),
                )
            )

        # Calculate totals
        passed_checks = sum(1 for r in check_results if r.passed)
        failed_checks = sum(1 for r in check_results if not r.passed)

        # Determine overall validity
        # Valid if no critical failures and no errors
        has_critical_failures = any(
            not r.passed and r.severity == "critical" for r in check_results
        )
        is_valid = not has_critical_failures and len(self._errors) == 0

        # Calculate total execution time
        total_time: float | None = None
        if self.started_at and self.completed_at:
            total_time = (self.completed_at - self.started_at).total_seconds() * 1000

        # Get contract info from split job
        contract_id = self.split_job.wasm_job.contract_id if self.split_job.wasm_job else ""
        contract_version = (
            self.split_job.wasm_job.contract_version if self.split_job.wasm_job else ""
        )
        profile = self.split_job.wasm_job.profile if self.split_job.wasm_job else "default"

        if not contract_id and self.split_job.container_jobs:
            first_job = self.split_job.container_jobs[0]
            contract_id = first_job.contract_id
            contract_version = first_job.contract_version

        return AggregatedResult(
            job_id=self.split_job.parent_job_id,
            contract_id=contract_id,
            contract_version=contract_version,
            profile=profile,
            is_valid=is_valid,
            total_checks=len(check_results),
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            check_results=check_results,
            wasm_execution_time_ms=wasm_execution_time,
            container_execution_time_ms=container_execution_time
            if container_execution_time > 0
            else None,
            total_execution_time_ms=total_time,
            started_at=self.started_at,
            completed_at=self.completed_at,
            errors=self._errors,
        )

    def get_progress(self) -> dict[str, Any]:
        """
        Get current aggregation progress.

        Returns:
            Progress information
        """
        expected_total = (1 if self._wasm_job_id else 0) + len(self._container_job_ids)
        completed = (1 if self._wasm_result else 0) + len(self._container_results)

        return {
            "parent_job_id": self.split_job.parent_job_id,
            "total_jobs": expected_total,
            "completed_jobs": completed,
            "pending_jobs": self.pending_jobs,
            "is_complete": self.is_complete,
            "errors": self._errors,
        }
