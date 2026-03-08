"""
Local worker for development and testing.

Executes validation jobs in the local process, useful for
development, testing, and debugging.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from griot_core.executors import ExecutorRegistry, ExecutorRuntime
from griot_core.models import Contract
from griot_core.parsing import parse_contract_json, parse_contract_yaml
from griot_core.validation import ValidationEngine, ValidationOptions

from .base import (
    JobPayload,
    Worker,
    WorkerConfig,
    WorkerResult,
    WorkerStatus,
)


class LocalContractFetcher:
    """
    Simple contract fetcher for local testing.

    Stores contracts in memory and retrieves them by ID.
    """

    def __init__(self) -> None:
        self._contracts: Dict[str, Contract] = {}

    def add_contract(self, contract: Contract) -> None:
        """Add a contract to the fetcher."""
        self._contracts[contract.id] = contract

    def add_contract_yaml(self, yaml_str: str) -> Contract:
        """Parse and add a contract from YAML."""
        contract = parse_contract_yaml(yaml_str)
        self._contracts[contract.id] = contract
        return contract

    def add_contract_json(self, json_str: str) -> Contract:
        """Parse and add a contract from JSON."""
        contract = parse_contract_json(json_str)
        self._contracts[contract.id] = contract
        return contract

    def fetch(self, contract_id: str, version: Optional[str] = None) -> Contract:
        """Fetch a contract by ID."""
        if contract_id not in self._contracts:
            raise KeyError(f"Contract not found: {contract_id}")
        return self._contracts[contract_id]


class LocalWorker(Worker):
    """
    Local worker for development and testing.

    Executes validation jobs synchronously in the local process.
    Useful for:
    - Development and debugging
    - Unit testing
    - CI/CD pipelines
    - Simple single-machine deployments

    Example:
        >>> config = WorkerConfig(worker_id="local-1", worker_type="local")
        >>> worker = LocalWorker(config)
        >>> worker.contract_fetcher.add_contract(my_contract)
        >>> result = await worker.execute(JobPayload(
        ...     job_id="job-1",
        ...     contract_id="my-contract",
        ...     profile="data_engineering",
        ... ))
        >>> print(f"Valid: {result.is_valid}")
    """

    def __init__(
        self,
        config: Optional[WorkerConfig] = None,
        validation_engine: Optional[ValidationEngine] = None,
        contract_fetcher: Optional[LocalContractFetcher] = None,
    ):
        """
        Initialize the local worker.

        Args:
            config: Worker configuration
            validation_engine: Optional pre-configured validation engine
            contract_fetcher: Optional contract fetcher
        """
        if config is None:
            config = WorkerConfig(
                worker_id=f"local-{uuid.uuid4().hex[:8]}",
                worker_type="local",
            )

        super().__init__(config)

        self.contract_fetcher = contract_fetcher or LocalContractFetcher()

        if validation_engine:
            self._engine = validation_engine
        else:
            # Create default validation engine
            self._engine = ValidationEngine(
                executor_runtime=ExecutorRuntime(),
                executor_registry=ExecutorRegistry(),
            )

        self._results: Dict[str, WorkerResult] = {}

    async def execute(self, payload: JobPayload) -> WorkerResult:
        """
        Execute a validation job locally.

        Args:
            payload: Job payload with contract and data

        Returns:
            WorkerResult with validation results
        """
        started_at = datetime.now()
        start_time = time.perf_counter()

        result = WorkerResult(
            job_id=payload.job_id,
            status=WorkerStatus.RUNNING,
            started_at=started_at,
            worker_id=self.config.worker_id,
            worker_type="local",
        )

        try:
            # Fetch contract
            contract = self.contract_fetcher.fetch(
                payload.contract_id,
                payload.contract_version,
            )

            # Build validation options
            options = ValidationOptions(
                profile=payload.profile,
                environment=payload.environment,
                timeout_seconds=payload.timeout_seconds,
                **payload.options,
            )

            # Run validation with timeout
            timeout = payload.timeout_seconds or self.config.default_timeout
            try:
                validation_result = await asyncio.wait_for(
                    self._engine.validate(
                        contract=contract,
                        profile=payload.profile,
                        environment=payload.environment,
                        options=options,
                        arrow_data=payload.arrow_data,
                    ),
                    timeout=timeout,
                )

                result.status = WorkerStatus.COMPLETED
                result.is_valid = validation_result.is_valid
                result.validation_result = self._serialize_result(validation_result)

            except asyncio.TimeoutError:
                result.status = WorkerStatus.TIMEOUT
                result.errors.append(f"Job timed out after {timeout} seconds")

        except KeyError as e:
            result.status = WorkerStatus.FAILED
            result.errors.append(f"Contract not found: {e}")

        except Exception as e:
            result.status = WorkerStatus.FAILED
            result.errors.append(str(e))

        # Complete timing
        result.completed_at = datetime.now()
        result.duration_ms = (time.perf_counter() - start_time) * 1000

        # Store result
        self._results[payload.job_id] = result

        # Report if callback configured
        if self.config.callback_enabled and payload.callback_url:
            await self.report_result(result)

        return result

    def _serialize_result(self, validation_result: Any) -> Dict[str, Any]:
        """Serialize validation result to dictionary."""
        # Convert ValidationResult to dict
        return {
            "is_valid": validation_result.is_valid,
            "contract_id": validation_result.contract_id,
            "contract_version": validation_result.contract_version,
            "profile_used": validation_result.profile_used,
            "mode": validation_result.mode.value if validation_result.mode else None,
            "started_at": validation_result.started_at.isoformat()
            if validation_result.started_at
            else None,
            "completed_at": validation_result.completed_at.isoformat()
            if validation_result.completed_at
            else None,
            "duration_ms": validation_result.duration_ms,
            "errors": validation_result.errors,
            "schema_results": [
                {
                    "schema_id": sr.schema_id,
                    "schema_name": sr.schema_name,
                    "is_valid": sr.is_valid,
                    "check_results": [
                        {
                            "check_name": cr.check_name,
                            "status": cr.status.value,
                            "severity": cr.severity.value if cr.severity else None,
                            "metric_value": cr.metric_value,
                            "threshold": cr.threshold,
                            "error_message": cr.error_message,
                            "execution_time_ms": cr.execution_time_ms,
                        }
                        for cr in sr.check_results
                    ],
                }
                for sr in validation_result.schema_results
            ],
        }

    async def report_result(self, result: WorkerResult) -> bool:
        """
        Report results back to the registry.

        For local worker, this is a no-op by default.
        Override or configure for custom reporting.

        Args:
            result: Worker result to report

        Returns:
            True (always succeeds for local)
        """
        # Local worker stores results in memory
        # In production, this would POST to callback_url
        return True

    async def health_check(self) -> Dict[str, Any]:
        """
        Check worker health.

        Returns:
            Health status dictionary
        """
        return {
            "status": "healthy",
            "worker_id": self.config.worker_id,
            "worker_type": "local",
            "jobs_completed": len(self._results),
            "engine_capabilities": self._engine.get_runtime_capabilities(),
        }

    def get_result(self, job_id: str) -> Optional[WorkerResult]:
        """
        Get a stored result by job ID.

        Args:
            job_id: Job identifier

        Returns:
            WorkerResult if found, None otherwise
        """
        return self._results.get(job_id)

    def list_results(self) -> Dict[str, WorkerResult]:
        """
        List all stored results.

        Returns:
            Dictionary of job_id to WorkerResult
        """
        return self._results.copy()

    def clear_results(self) -> None:
        """Clear all stored results."""
        self._results.clear()


async def run_local_validation(
    contract: Contract,
    profile: str = "default",
    arrow_data: Optional[Dict[str, bytes]] = None,
    options: Optional[Dict[str, Any]] = None,
) -> WorkerResult:
    """
    Convenience function to run validation locally.

    Args:
        contract: Contract to validate
        profile: Execution profile
        arrow_data: Arrow IPC data keyed by schema ID
        options: Additional validation options

    Returns:
        WorkerResult with validation results

    Example:
        >>> result = await run_local_validation(
        ...     contract=my_contract,
        ...     profile="data_engineering",
        ...     arrow_data={"schema-1": arrow_bytes},
        ... )
        >>> print(f"Valid: {result.is_valid}")
    """
    worker = LocalWorker()
    worker.contract_fetcher.add_contract(contract)

    payload = JobPayload(
        job_id=f"local-{uuid.uuid4().hex[:8]}",
        contract_id=contract.id,
        profile=profile,
        arrow_data=arrow_data,
        options=options or {},
    )

    return await worker.execute(payload)
