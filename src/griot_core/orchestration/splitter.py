"""
Job splitter for separating WASM and container checks.

The JobSplitter analyzes a validation job's checks and separates them
by runtime type:
- WASM checks -> batched into a single WasmJobSpec
- Container checks -> each becomes a separate ContainerJobSpec

This enables parallel execution without Docker-in-Docker.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from griot_core.orchestration.types import (
    CheckRuntime,
    CheckSpec,
    ContainerJobSpec,
    SplitJob,
    WasmJobSpec,
)

logger = logging.getLogger(__name__)


class JobSplitter:
    """
    Splits validation jobs into WASM and container sub-jobs.

    The splitter categorizes checks by their runtime type and creates
    appropriate job specifications for each:

    - WASM checks are batched into a single WasmJobSpec that will run
      in a griot-core worker container using the embedded WASM runtime.

    - Container checks each become a separate ContainerJobSpec that
      will run as a native K8s pod (avoiding Docker-in-Docker).

    Example:
        splitter = JobSplitter()

        # Contract has mixed checks
        checks = [
            CheckSpec(name="null_check", executor_uri="registry://null-check@1.0", runtime=CheckRuntime.WASM),
            CheckSpec(name="drift_check", executor_uri="oci://ghcr.io/griot/drift:1.0", runtime=CheckRuntime.CONTAINER),
        ]

        split = splitter.split(
            job_id="job-123",
            contract_id="orders",
            contract_version="1.0.0",
            profile="data_engineering",
            checks=checks,
            data_reference={"s3": "s3://bucket/data.parquet"},
            callback_url="https://registry/api/v1/jobs/job-123/callback",
        )

        # split.wasm_job contains null_check
        # split.container_jobs contains drift_check
    """

    def __init__(
        self,
        default_wasm_timeout: int = 600,
        default_container_timeout: int = 600,
    ):
        """
        Initialize the job splitter.

        Args:
            default_wasm_timeout: Default timeout for WASM worker (seconds)
            default_container_timeout: Default timeout for container checks (seconds)
        """
        self.default_wasm_timeout = default_wasm_timeout
        self.default_container_timeout = default_container_timeout

    def split(
        self,
        job_id: str,
        contract_id: str,
        contract_version: str,
        profile: str,
        checks: list[CheckSpec],
        data_reference: dict[str, Any],
        callback_url: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SplitJob:
        """
        Split a validation job into WASM and container sub-jobs.

        Args:
            job_id: Parent job ID
            contract_id: Contract being validated
            contract_version: Version of the contract
            profile: Validation profile
            checks: List of checks to execute
            data_reference: Reference to data (S3 URL, etc.)
            callback_url: URL to POST results back to
            metadata: Additional job metadata

        Returns:
            SplitJob with separated WASM and container jobs
        """
        metadata = metadata or {}

        # Categorize checks by runtime
        wasm_checks: list[CheckSpec] = []
        container_checks: list[CheckSpec] = []

        for check in checks:
            if check.runtime == CheckRuntime.WASM:
                wasm_checks.append(check)
            else:
                container_checks.append(check)

        logger.info(
            "Split job %s: %d WASM checks, %d container checks",
            job_id,
            len(wasm_checks),
            len(container_checks),
        )

        # Create WASM job if there are WASM checks
        wasm_job: WasmJobSpec | None = None
        if wasm_checks:
            wasm_job = WasmJobSpec(
                job_id=f"{job_id}-wasm",
                contract_id=contract_id,
                contract_version=contract_version,
                profile=profile,
                checks=wasm_checks,
                data_reference=data_reference,
                callback_url=callback_url,
                timeout_seconds=self._calculate_wasm_timeout(wasm_checks),
                metadata={
                    **metadata,
                    "parent_job_id": job_id,
                    "job_type": "wasm_worker",
                },
            )

        # Create container jobs (one per container check)
        container_jobs: list[ContainerJobSpec] = []
        for check in container_checks:
            container_job = ContainerJobSpec(
                job_id=f"{job_id}-{check.name}-{uuid.uuid4().hex[:6]}",
                parent_job_id=job_id,
                contract_id=contract_id,
                contract_version=contract_version,
                check=check,
                data_reference=data_reference,
                callback_url=callback_url,
                timeout_seconds=check.timeout_seconds or self.default_container_timeout,
                metadata={
                    **metadata,
                    "parent_job_id": job_id,
                    "job_type": "container_check",
                },
            )
            container_jobs.append(container_job)

        return SplitJob(
            parent_job_id=job_id,
            wasm_job=wasm_job,
            container_jobs=container_jobs,
            total_checks=len(checks),
        )

    def _calculate_wasm_timeout(self, checks: list[CheckSpec]) -> int:
        """
        Calculate appropriate timeout for WASM worker.

        Uses the sum of individual check timeouts with a minimum
        of the default timeout.

        Args:
            checks: List of WASM checks

        Returns:
            Timeout in seconds
        """
        total = sum(c.timeout_seconds for c in checks)
        return max(total, self.default_wasm_timeout)

    def split_from_contract(
        self,
        job_id: str,
        contract: Any,  # Contract type from griot_core
        profile: str,
        data_reference: dict[str, Any],
        callback_url: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SplitJob:
        """
        Split a job using a Contract object directly.

        Extracts checks from the contract and converts them to CheckSpecs.

        Args:
            job_id: Parent job ID
            contract: Contract object
            profile: Validation profile
            data_reference: Reference to data
            callback_url: URL to POST results back to
            metadata: Additional job metadata

        Returns:
            SplitJob with separated WASM and container jobs
        """
        # Convert contract checks to CheckSpecs
        checks = self._extract_checks_from_contract(contract, profile)

        return self.split(
            job_id=job_id,
            contract_id=contract.id,
            contract_version=contract.version,
            profile=profile,
            checks=checks,
            data_reference=data_reference,
            callback_url=callback_url,
            metadata=metadata,
        )

    def _extract_checks_from_contract(self, contract: Any, profile: str) -> list[CheckSpec]:
        """
        Extract checks from a contract for the given profile.

        Args:
            contract: Contract object
            profile: Validation profile to filter checks

        Returns:
            List of CheckSpecs
        """
        checks: list[CheckSpec] = []

        # Get checks from contract (adapt based on your Contract model)
        contract_checks = getattr(contract, "checks", [])
        if hasattr(contract, "quality") and hasattr(contract.quality, "checks"):
            contract_checks = contract.quality.checks

        for check in contract_checks:
            # Filter by profile if check has conditions
            if hasattr(check, "when") and check.when:
                if check.when.profile and profile not in check.when.profile:
                    continue

            # Create CheckSpec from contract check
            check_spec = CheckSpec.from_executor_uri(
                name=check.name,
                executor_uri=check.executor,
                parameters=getattr(check, "parameters", {}),
                severity=getattr(check, "severity", "warning"),
                timeout_seconds=getattr(check, "timeout_seconds", 300),
            )
            checks.append(check_spec)

        return checks
