"""
Validation orchestrator - main entry point for orchestrated validation.

The ValidationOrchestrator coordinates the execution of validation jobs
by splitting them into WASM and container checks, dispatching them in
parallel, and aggregating the results.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from griot_core.orchestration.aggregator import ResultAggregator
from griot_core.orchestration.dispatcher import (
    ComputeDispatcher,
    DispatcherConfig,
    create_dispatcher,
)
from griot_core.orchestration.splitter import JobSplitter
from griot_core.orchestration.types import (
    AggregatedResult,
    CheckSpec,
    DispatchResult,
    SplitJob,
)

logger = logging.getLogger(__name__)


class ValidationOrchestrator:
    """
    Orchestrates validation job execution across compute backends.

    The orchestrator:
    1. Splits jobs into WASM and container checks using JobSplitter
    2. Dispatches all jobs in parallel to the compute backend
    3. Collects results via callbacks (async) or polling
    4. Aggregates results into a single AggregatedResult

    This approach avoids Docker-in-Docker by:
    - Running WASM checks in a griot-core worker container with embedded WASM runtime
    - Running container checks as native K8s pods spawned by the orchestrator

    Example:
        # Create orchestrator
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
            contract=contract,
            profile="data_engineering",
            data_reference={"s3": "s3://bucket/data.parquet"},
        )

        print(f"Valid: {result.is_valid}")
        print(f"Passed: {result.passed_checks}/{result.total_checks}")
    """

    def __init__(
        self,
        dispatcher_config: DispatcherConfig | None = None,
        dispatcher: ComputeDispatcher | None = None,
        splitter: JobSplitter | None = None,
        callback_base_url: str | None = None,
        **dispatcher_kwargs: Any,
    ):
        """
        Initialize the orchestrator.

        Args:
            dispatcher_config: Configuration for the dispatcher
            dispatcher: Pre-configured dispatcher (alternative to config)
            splitter: Pre-configured job splitter
            callback_base_url: Base URL for job callbacks
            **dispatcher_kwargs: Additional kwargs passed to dispatcher factory
        """
        self._dispatcher_config = dispatcher_config
        self._dispatcher = dispatcher
        self._dispatcher_kwargs = dispatcher_kwargs
        self._splitter = splitter or JobSplitter()
        self._callback_base_url = callback_base_url

        # Track active aggregators for async result collection
        self._aggregators: dict[str, ResultAggregator] = {}

    @property
    def dispatcher(self) -> ComputeDispatcher:
        """Get or create the compute dispatcher."""
        if self._dispatcher is None:
            if self._dispatcher_config is None:
                raise ValueError("Either dispatcher or dispatcher_config must be provided")
            self._dispatcher = create_dispatcher(self._dispatcher_config, **self._dispatcher_kwargs)
        return self._dispatcher

    async def validate(
        self,
        contract: Any,
        profile: str,
        data_reference: dict[str, Any],
        callback_url: str | None = None,
        metadata: dict[str, Any] | None = None,
        wait_for_completion: bool = False,
        timeout_seconds: int = 1800,
    ) -> AggregatedResult | str:
        """
        Execute validation for a contract.

        This is the main entry point for validation. It:
        1. Generates a job ID
        2. Splits the job into WASM and container checks
        3. Dispatches all jobs in parallel
        4. Optionally waits for completion

        Args:
            contract: Contract to validate
            profile: Validation profile
            data_reference: Reference to data (S3 URL, etc.) - NOT the data itself
            callback_url: Optional override for callback URL
            metadata: Additional job metadata
            wait_for_completion: If True, wait for all results synchronously
            timeout_seconds: Max wait time when waiting for completion

        Returns:
            If wait_for_completion: AggregatedResult
            Otherwise: job_id (results come via callbacks)
        """
        job_id = str(uuid.uuid4())

        # Build callback URL
        cb_url = callback_url or self._build_callback_url(job_id)

        # Split the job
        split_job = self._splitter.split_from_contract(
            job_id=job_id,
            contract=contract,
            profile=profile,
            data_reference=data_reference,
            callback_url=cb_url,
            metadata=metadata,
        )

        logger.info(
            "Orchestrating validation job %s: %d WASM checks, %d container checks",
            job_id,
            split_job.wasm_check_count,
            split_job.container_check_count,
        )

        # Create aggregator
        aggregator = ResultAggregator(split_job)
        aggregator.start()
        self._aggregators[job_id] = aggregator

        # Dispatch all jobs in parallel
        dispatch_results = await self._dispatch_all(split_job)

        # Check for dispatch failures
        failures = [r for r in dispatch_results if not r.success]
        if failures:
            for failure in failures:
                logger.error("Dispatch failed for %s: %s", failure.job_id, failure.error)

        if wait_for_completion:
            # Wait for results via polling
            return await self._wait_for_completion(job_id, timeout_seconds)

        # Return job ID - results will come via callbacks
        return job_id

    async def validate_checks(
        self,
        job_id: str,
        contract_id: str,
        contract_version: str,
        profile: str,
        checks: list[CheckSpec],
        data_reference: dict[str, Any],
        callback_url: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[SplitJob, list[DispatchResult]]:
        """
        Execute validation with explicit checks (no contract object).

        Lower-level API that accepts CheckSpecs directly instead of
        extracting them from a contract.

        Args:
            job_id: Job ID to use
            contract_id: Contract identifier
            contract_version: Contract version
            profile: Validation profile
            checks: List of checks to execute
            data_reference: Reference to data
            callback_url: URL to POST results back to
            metadata: Additional job metadata

        Returns:
            Tuple of (SplitJob, list of DispatchResults)
        """
        cb_url = callback_url or self._build_callback_url(job_id)

        # Split the job
        split_job = self._splitter.split(
            job_id=job_id,
            contract_id=contract_id,
            contract_version=contract_version,
            profile=profile,
            checks=checks,
            data_reference=data_reference,
            callback_url=cb_url,
            metadata=metadata,
        )

        # Create aggregator
        aggregator = ResultAggregator(split_job)
        aggregator.start()
        self._aggregators[job_id] = aggregator

        # Dispatch all jobs
        dispatch_results = await self._dispatch_all(split_job)

        return split_job, dispatch_results

    async def _dispatch_all(self, split_job: SplitJob) -> list[DispatchResult]:
        """
        Dispatch all jobs in parallel.

        Args:
            split_job: Split job with WASM and container jobs

        Returns:
            List of dispatch results
        """
        tasks = []

        # Dispatch WASM worker if present
        if split_job.wasm_job:
            tasks.append(self.dispatcher.dispatch_wasm_worker(split_job.wasm_job))

        # Dispatch container jobs
        for container_job in split_job.container_jobs:
            tasks.append(self.dispatcher.dispatch_container(container_job))

        # Execute all dispatches in parallel
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Convert exceptions to failed DispatchResults
            processed_results: list[DispatchResult] = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    # Determine which job this was for
                    if i == 0 and split_job.wasm_job:
                        job_id = split_job.wasm_job.job_id
                        job_type = "wasm_worker"
                    else:
                        # Adjust index for container jobs
                        container_idx = i - (1 if split_job.wasm_job else 0)
                        job_id = split_job.container_jobs[container_idx].job_id
                        job_type = "container"

                    processed_results.append(
                        DispatchResult(
                            success=False,
                            job_id=job_id,
                            job_type=job_type,
                            backend=self.dispatcher.backend.value,
                            error=str(result),
                        )
                    )
                else:
                    processed_results.append(result)  # type: ignore[arg-type]

            return processed_results

        return []

    async def _wait_for_completion(self, job_id: str, timeout_seconds: int) -> AggregatedResult:
        """
        Wait for all job results via polling.

        Args:
            job_id: Parent job ID
            timeout_seconds: Max wait time

        Returns:
            AggregatedResult when all jobs complete
        """
        aggregator = self._aggregators.get(job_id)
        if not aggregator:
            raise ValueError(f"No aggregator found for job {job_id}")

        poll_interval = 2.0  # seconds
        elapsed = 0.0

        while elapsed < timeout_seconds:
            if aggregator.is_complete:
                result = aggregator.aggregate()
                del self._aggregators[job_id]
                return result

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            # Log progress periodically
            if int(elapsed) % 10 == 0:
                progress = aggregator.get_progress()
                logger.info(
                    "Job %s progress: %d/%d complete",
                    job_id,
                    progress["completed_jobs"],
                    progress["total_jobs"],
                )

        # Timeout - return partial result
        logger.warning("Job %s timed out after %d seconds", job_id, timeout_seconds)
        result = aggregator.aggregate()
        result.errors.append(f"Timeout after {timeout_seconds} seconds")
        del self._aggregators[job_id]
        return result

    def receive_callback(self, job_id: str, result: dict[str, Any]) -> bool:
        """
        Receive a callback result from a completed job.

        Called by the callback endpoint to deliver results to the
        appropriate aggregator.

        Args:
            job_id: Job ID from the callback
            result: Result data from the worker

        Returns:
            True if callback was processed
        """
        # Find parent job ID
        parent_job_id = result.get("parent_job_id") or result.get("metadata", {}).get(
            "parent_job_id"
        )

        # If this is the parent job ID itself, extract from result
        if parent_job_id is None:
            # Check if it's a WASM worker result (job_id ends with -wasm)
            if job_id.endswith("-wasm"):
                parent_job_id = job_id[:-5]  # Remove -wasm suffix
            else:
                # Try to find by iterating aggregators
                for pid, agg in self._aggregators.items():
                    if job_id in [agg._wasm_job_id] + list(agg._container_job_ids):
                        parent_job_id = pid
                        break

        if parent_job_id is None:
            logger.warning("Could not find parent job for callback %s", job_id)
            return False

        aggregator = self._aggregators.get(parent_job_id)
        if aggregator is None:
            logger.warning(
                "No aggregator found for parent job %s (callback %s)",
                parent_job_id,
                job_id,
            )
            return False

        # Add result to aggregator
        aggregator.add_result(result)
        return True

    def get_aggregator(self, job_id: str) -> ResultAggregator | None:
        """
        Get the aggregator for a job.

        Args:
            job_id: Parent job ID

        Returns:
            ResultAggregator or None if not found
        """
        return self._aggregators.get(job_id)

    def _build_callback_url(self, job_id: str) -> str | None:
        """
        Build callback URL for a job.

        Args:
            job_id: Job ID

        Returns:
            Callback URL or None if base URL not configured
        """
        if not self._callback_base_url:
            return None
        return f"{self._callback_base_url.rstrip('/')}/api/v1/jobs/{job_id}/callback"

    async def health_check(self) -> dict[str, Any]:
        """
        Check orchestrator health.

        Returns:
            Health status information
        """
        dispatcher_healthy = await self.dispatcher.health_check()

        return {
            "healthy": dispatcher_healthy,
            "dispatcher_backend": self.dispatcher.backend.value,
            "active_jobs": len(self._aggregators),
        }
