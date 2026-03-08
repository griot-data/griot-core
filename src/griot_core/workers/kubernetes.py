"""
Kubernetes worker for container-based validation.

Provides the worker implementation for running validation jobs
as Kubernetes Jobs or standalone pods.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from griot_core.executors import ExecutorRegistry, ExecutorRuntime
from griot_core.models import Contract
from griot_core.parsing import parse_contract_json
from griot_core.validation import ValidationEngine, ValidationOptions

from .base import (
    JobPayload,
    Worker,
    WorkerConfig,
    WorkerResult,
    WorkerStatus,
)


class KubernetesWorker(Worker):
    """
    Kubernetes worker for container-based validation.

    Designed to run as a Kubernetes Job or standalone pod.
    Reads job configuration from environment variables or
    mounted ConfigMaps/Secrets.

    Environment Variables:
        GRIOT_JOB_ID: Job identifier
        GRIOT_CONTRACT_ID: Contract to validate
        GRIOT_CONTRACT_VERSION: Contract version (optional)
        GRIOT_PROFILE: Execution profile (default: "default")
        GRIOT_ENVIRONMENT: Environment (default: "production")
        GRIOT_CALLBACK_URL: URL to report results
        GRIOT_TIMEOUT: Timeout in seconds
        GRIOT_REGISTRY_URL: URL of the Griot registry
        GRIOT_WORKER_ID: Worker identifier
        GRIOT_ARROW_DATA_PATH: Path to mounted Arrow data directory
        KUBERNETES_POD_NAME: Pod name (set by K8s downward API)
        KUBERNETES_NAMESPACE: Namespace (set by K8s downward API)

    Example Kubernetes Job:
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: griot-validation-job
        spec:
          template:
            spec:
              containers:
              - name: validator
                image: griot/worker:latest
                env:
                - name: GRIOT_JOB_ID
                  value: "job-123"
                - name: GRIOT_CONTRACT_ID
                  value: "my-contract"
                volumeMounts:
                - name: arrow-data
                  mountPath: /data
              restartPolicy: Never
          backoffLimit: 2
    """

    def __init__(
        self,
        config: Optional[WorkerConfig] = None,
        validation_engine: Optional[ValidationEngine] = None,
    ):
        """
        Initialize the Kubernetes worker.

        Args:
            config: Worker configuration (defaults to env-based config)
            validation_engine: Optional pre-configured validation engine
        """
        if config is None:
            config = WorkerConfig(
                worker_id=os.environ.get(
                    "GRIOT_WORKER_ID",
                    os.environ.get("KUBERNETES_POD_NAME", f"k8s-{uuid.uuid4().hex[:8]}"),
                ),
                worker_type="kubernetes",
                default_timeout=int(os.environ.get("GRIOT_TIMEOUT", "300")),
                registry_url=os.environ.get("GRIOT_REGISTRY_URL"),
                callback_enabled=True,
                extra_options={
                    "namespace": os.environ.get("KUBERNETES_NAMESPACE", "default"),
                    "pod_name": os.environ.get("KUBERNETES_POD_NAME"),
                },
            )

        super().__init__(config)

        if validation_engine:
            self._engine = validation_engine
        else:
            self._engine = ValidationEngine(
                executor_runtime=ExecutorRuntime(),
                executor_registry=ExecutorRegistry(),
            )

        self._contracts_cache: Dict[str, Contract] = {}

    def get_job_payload_from_env(self) -> JobPayload:
        """
        Build JobPayload from environment variables.

        Returns:
            JobPayload configured from environment

        Raises:
            ValueError: If required environment variables are missing
        """
        contract_id = os.environ.get("GRIOT_CONTRACT_ID")
        if not contract_id:
            raise ValueError("GRIOT_CONTRACT_ID environment variable is required")

        # Load Arrow data from mounted path if available
        arrow_data = None
        arrow_data_path = os.environ.get("GRIOT_ARROW_DATA_PATH", "/data")
        if os.path.isdir(arrow_data_path):
            arrow_data = self._load_arrow_data(arrow_data_path)

        # Parse options from JSON env var
        options = {}
        options_json = os.environ.get("GRIOT_OPTIONS")
        if options_json:
            options = json.loads(options_json)

        return JobPayload(
            job_id=os.environ.get("GRIOT_JOB_ID", f"k8s-{uuid.uuid4().hex[:8]}"),
            contract_id=contract_id,
            contract_version=os.environ.get("GRIOT_CONTRACT_VERSION"),
            profile=os.environ.get("GRIOT_PROFILE", "default"),
            environment=os.environ.get("GRIOT_ENVIRONMENT", "production"),
            arrow_data=arrow_data,
            options=options,
            callback_url=os.environ.get("GRIOT_CALLBACK_URL"),
            timeout_seconds=int(os.environ.get("GRIOT_TIMEOUT", "300")),
            metadata={
                "namespace": self.config.extra_options.get("namespace"),
                "pod_name": self.config.extra_options.get("pod_name"),
            },
        )

    def _load_arrow_data(self, data_path: str) -> Dict[str, bytes]:
        """
        Load Arrow data from mounted directory.

        Expects files named <schema_id>.arrow

        Args:
            data_path: Path to data directory

        Returns:
            Dictionary of schema_id to Arrow bytes
        """
        arrow_data = {}

        for filename in os.listdir(data_path):
            if filename.endswith(".arrow"):
                schema_id = filename[:-6]  # Remove .arrow extension
                file_path = os.path.join(data_path, filename)
                with open(file_path, "rb") as f:
                    arrow_data[schema_id] = f.read()

        return arrow_data

    async def execute(self, payload: JobPayload) -> WorkerResult:
        """
        Execute a validation job.

        Args:
            payload: Job payload

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
            worker_type="kubernetes",
            metadata={
                "namespace": self.config.extra_options.get("namespace"),
                "pod_name": self.config.extra_options.get("pod_name"),
            },
        )

        try:
            # Fetch contract
            contract = await self._fetch_contract(
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

            # Run validation
            validation_result = await asyncio.wait_for(
                self._engine.validate(
                    contract=contract,
                    profile=payload.profile,
                    environment=payload.environment,
                    options=options,
                    arrow_data=payload.arrow_data,
                ),
                timeout=payload.timeout_seconds,
            )

            result.status = WorkerStatus.COMPLETED
            result.is_valid = validation_result.is_valid
            result.validation_result = self._serialize_result(validation_result)

        except asyncio.TimeoutError:
            result.status = WorkerStatus.TIMEOUT
            result.errors.append(f"Job timed out after {payload.timeout_seconds} seconds")

        except Exception as e:
            result.status = WorkerStatus.FAILED
            result.errors.append(str(e))

        # Complete timing
        result.completed_at = datetime.now()
        result.duration_ms = (time.perf_counter() - start_time) * 1000

        # Report result
        if self.config.callback_enabled and payload.callback_url:
            await self.report_result(result)

        return result

    async def _fetch_contract(
        self,
        contract_id: str,
        version: Optional[str] = None,
    ) -> Contract:
        """
        Fetch a contract from registry or cache.

        Args:
            contract_id: Contract identifier
            version: Optional version

        Returns:
            Contract object
        """
        cache_key = f"{contract_id}@{version or 'latest'}"

        if cache_key in self._contracts_cache:
            return self._contracts_cache[cache_key]

        # Try to load from mounted ConfigMap
        contract_path = os.environ.get("GRIOT_CONTRACT_PATH")
        if contract_path and os.path.isfile(contract_path):
            with open(contract_path, "r") as f:
                data = json.load(f)
                contract = parse_contract_json(json.dumps(data))
                self._contracts_cache[cache_key] = contract
                return contract

        # Fetch from registry
        if self.config.registry_url:
            import urllib.request

            url = f"{self.config.registry_url}/api/v1/contracts/{contract_id}"
            if version:
                url += f"?version={version}"

            with urllib.request.urlopen(url, timeout=10) as response:
                data = json.loads(response.read().decode())
                contract = parse_contract_json(json.dumps(data))
                self._contracts_cache[cache_key] = contract
                return contract

        raise ValueError(
            f"Contract {contract_id} not found. Set GRIOT_CONTRACT_PATH or GRIOT_REGISTRY_URL"
        )

    def _serialize_result(self, validation_result: Any) -> Dict[str, Any]:
        """Serialize validation result to dictionary."""
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

        Args:
            result: Worker result to report

        Returns:
            True if successful
        """
        callback_url = result.metadata.get("callback_url")
        if not callback_url:
            return True

        try:
            import urllib.request

            data = result.to_json().encode("utf-8")

            req = urllib.request.Request(
                callback_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status == 200

        except Exception:
            return False

    async def health_check(self) -> Dict[str, Any]:
        """
        Check worker health.

        Returns:
            Health status dictionary
        """
        return {
            "status": "healthy",
            "worker_id": self.config.worker_id,
            "worker_type": "kubernetes",
            "namespace": self.config.extra_options.get("namespace"),
            "pod_name": self.config.extra_options.get("pod_name"),
            "registry_url": self.config.registry_url,
            "engine_capabilities": self._engine.get_runtime_capabilities(),
        }


def main() -> int:
    """
    Main entry point for Kubernetes worker.

    Reads configuration from environment variables and runs validation.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    worker = KubernetesWorker()

    try:
        # Get job payload from environment
        payload = worker.get_job_payload_from_env()

        # Run validation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(worker.execute(payload))
        finally:
            loop.close()

        # Output result
        print(result.to_json())

        # Exit with appropriate code
        if result.status == WorkerStatus.COMPLETED and result.is_valid:
            return 0
        elif result.status == WorkerStatus.COMPLETED:
            return 0  # Validation completed but failed - still success for job
        else:
            return 1

    except Exception as e:
        print(json.dumps({"error": str(e), "status": "failed"}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
