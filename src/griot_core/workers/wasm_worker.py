"""
Dedicated WASM worker for running WASM checks inside containers.

This worker is designed to run inside a griot-core worker container
(e.g., K8s Job). It receives a WasmJobSpec and executes all WASM
checks sequentially using the embedded WASM runtime.

No Docker/Podman needed - WASM modules run in-process.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass
class WasmCheckResult:
    """Result of a single WASM check execution."""

    check_name: str
    passed: bool
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
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "operator": self.operator,
            "severity": self.severity,
            "execution_time_ms": self.execution_time_ms,
            "error": self.error,
            "details": self.details,
        }


@dataclass
class WasmWorkerResult:
    """Result from WASM worker execution."""

    job_id: str
    success: bool
    check_results: list[WasmCheckResult]
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: float | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for callback."""
        return {
            "job_id": self.job_id,
            "job_type": "wasm_worker",
            "success": self.success,
            "check_results": [r.to_dict() for r in self.check_results],
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "metadata": self.metadata,
        }


class WasmWorker:
    """
    Dedicated WASM worker for executing WASM checks.

    This worker is designed to run inside a container orchestrated
    by the ValidationOrchestrator. It:

    1. Receives a WasmJobSpec via environment variable
    2. Fetches data from the data_reference
    3. Executes all WASM checks sequentially
    4. POSTs results to the callback URL

    Example usage in container entrypoint:
        worker = WasmWorker()
        await worker.run()

    Or execute specific checks:
        spec = WasmJobSpec(...)
        result = await worker.execute_wasm_checks(spec)
    """

    def __init__(
        self,
        wasm_cache_dir: Path | None = None,
        callback_timeout: float = 30.0,
    ):
        """
        Initialize the WASM worker.

        Args:
            wasm_cache_dir: Directory for caching WASM modules
            callback_timeout: Timeout for callback POST requests
        """
        self.wasm_cache_dir = wasm_cache_dir or Path("/tmp/wasm-cache")
        self.callback_timeout = callback_timeout
        self._wasm_runtime = None

    def _get_wasm_runtime(self):
        """Get or create WASM runtime."""
        if self._wasm_runtime is None:
            from griot_core.executors.wasm_runtime import WasmRuntime

            self._wasm_runtime = WasmRuntime(cache_dir=self.wasm_cache_dir)  # type: ignore[assignment]
        return self._wasm_runtime

    async def run(self) -> WasmWorkerResult:
        """
        Run the worker using spec from environment.

        Reads GRIOT_JOB_SPEC from environment and executes checks.
        POSTs results to callback URL if provided.

        Returns:
            WasmWorkerResult
        """
        # Read spec from environment
        spec_json = os.environ.get("GRIOT_JOB_SPEC")
        if not spec_json:
            error = "GRIOT_JOB_SPEC environment variable not set"
            logger.error(error)
            return WasmWorkerResult(
                job_id=os.environ.get("GRIOT_JOB_ID", "unknown"),
                success=False,
                check_results=[],
                error=error,
            )

        try:
            spec_dict = json.loads(spec_json)
        except json.JSONDecodeError as e:
            error = f"Failed to parse GRIOT_JOB_SPEC: {e}"
            logger.error(error)
            return WasmWorkerResult(
                job_id=os.environ.get("GRIOT_JOB_ID", "unknown"),
                success=False,
                check_results=[],
                error=error,
            )

        # Execute checks
        result = await self.execute_wasm_checks_from_dict(spec_dict)

        # POST results to callback URL
        callback_url = spec_dict.get("callback_url") or os.environ.get("GRIOT_CALLBACK_URL")
        if callback_url:
            await self._post_callback(callback_url, result)

        return result

    async def execute_wasm_checks(self, spec: Any) -> dict[str, Any]:
        """
        Execute WASM checks from a WasmJobSpec.

        Args:
            spec: WasmJobSpec instance

        Returns:
            Result dictionary
        """
        return await self.execute_wasm_checks_from_dict(spec.to_dict())  # type: ignore[return-value]

    async def execute_wasm_checks_from_dict(self, spec_dict: dict[str, Any]) -> WasmWorkerResult:
        """
        Execute WASM checks from a spec dictionary.

        Args:
            spec_dict: WasmJobSpec as dictionary

        Returns:
            WasmWorkerResult
        """
        job_id = spec_dict.get("job_id", "unknown")
        started_at = datetime.utcnow()
        start_time = time.perf_counter()

        check_results: list[WasmCheckResult] = []
        overall_success = True

        try:
            # Fetch data from reference
            data_reference = spec_dict.get("data_reference", {})
            arrow_data = await self._fetch_data(data_reference)

            # Get WASM runtime
            wasm_runtime = self._get_wasm_runtime()

            # Execute each check
            checks = spec_dict.get("checks", [])
            for check_dict in checks:
                check_result = await self._execute_single_check(
                    wasm_runtime,
                    check_dict,
                    arrow_data,
                )
                check_results.append(check_result)

                if not check_result.passed:
                    # Check severity to determine overall success
                    if check_result.severity == "critical":
                        overall_success = False

        except Exception as e:
            logger.exception("Error executing WASM checks for job %s", job_id)
            overall_success = False
            return WasmWorkerResult(
                job_id=job_id,
                success=False,
                check_results=check_results,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                duration_ms=(time.perf_counter() - start_time) * 1000,
                error=str(e),
                metadata=spec_dict.get("metadata", {}),
            )

        completed_at = datetime.utcnow()
        duration_ms = (time.perf_counter() - start_time) * 1000

        return WasmWorkerResult(
            job_id=job_id,
            success=overall_success,
            check_results=check_results,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=duration_ms,
            metadata=spec_dict.get("metadata", {}),
        )

    async def _execute_single_check(
        self,
        wasm_runtime: Any,
        check_dict: dict[str, Any],
        arrow_data: bytes,
    ) -> WasmCheckResult:
        """
        Execute a single WASM check.

        Args:
            wasm_runtime: WasmRuntime instance
            check_dict: Check specification dictionary
            arrow_data: Arrow IPC data

        Returns:
            WasmCheckResult
        """
        check_name = check_dict.get("name", "unknown")
        start_time = time.perf_counter()

        try:
            # Create executor spec from check
            from griot_core.executors.types import ExecutorSpec
            from griot_core.models.enums import Runtime

            executor_uri = check_dict.get("executor_uri", "")
            spec = ExecutorSpec(
                id=check_name,
                version="1.0",
                runtime=Runtime.WASM,
                artifact_url=executor_uri,
            )

            # Create check model
            from griot_core.models import Check
            from griot_core.models.enums import CheckType, Severity

            check = Check(
                name=check_name,
                description=check_dict.get("description", ""),
                type=CheckType.DATA_QUALITY,
                executor=executor_uri,
                parameters=check_dict.get("parameters", {}),
                severity=Severity(check_dict.get("severity", "warning")),
            )

            # Execute via WASM runtime
            result = await wasm_runtime.execute(
                spec,
                check,
                arrow_data,
                timeout=check_dict.get("timeout_seconds"),
            )

            execution_time_ms = (time.perf_counter() - start_time) * 1000

            return WasmCheckResult(
                check_name=check_name,
                passed=result.check_result.passed,
                metric_value=result.check_result.metric_value,
                threshold=result.check_result.threshold,
                operator=result.check_result.operator,
                severity=check_dict.get("severity", "warning"),
                execution_time_ms=execution_time_ms,
                error=result.check_result.error,
                details=result.check_result.details,
            )

        except Exception as e:
            logger.exception("Error executing check %s", check_name)
            execution_time_ms = (time.perf_counter() - start_time) * 1000

            return WasmCheckResult(
                check_name=check_name,
                passed=False,
                severity=check_dict.get("severity", "warning"),
                execution_time_ms=execution_time_ms,
                error=str(e),
            )

    async def _fetch_data(self, data_reference: dict[str, Any]) -> bytes:
        """
        Fetch data from reference.

        Supports:
        - s3: S3 URLs
        - gcs: GCS URLs
        - http/https: HTTP URLs
        - file: Local file paths
        - inline: Inline Arrow data (base64 encoded)

        Args:
            data_reference: Data reference dictionary

        Returns:
            Arrow IPC data as bytes
        """
        if "inline" in data_reference:
            import base64

            return base64.b64decode(data_reference["inline"])

        if "s3" in data_reference:
            return await self._fetch_from_s3(data_reference["s3"])

        if "gcs" in data_reference:
            return await self._fetch_from_gcs(data_reference["gcs"])

        if "http" in data_reference or "https" in data_reference:
            url = data_reference.get("http") or data_reference.get("https")
            return await self._fetch_from_http(url)  # type: ignore[arg-type]

        if "file" in data_reference:
            path = Path(data_reference["file"])
            return path.read_bytes()

        raise ValueError(f"Unsupported data reference type: {data_reference}")

    async def _fetch_from_s3(self, s3_url: str) -> bytes:
        """Fetch data from S3."""
        try:
            import boto3

            # Parse S3 URL
            if s3_url.startswith("s3://"):
                path = s3_url[5:]
            else:
                path = s3_url

            parts = path.split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""

            s3 = boto3.client("s3")
            response = s3.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()

        except ImportError:
            raise RuntimeError("boto3 required for S3 access")

    async def _fetch_from_gcs(self, gcs_url: str) -> bytes:
        """Fetch data from GCS."""
        try:
            from google.cloud import storage

            # Parse GCS URL
            if gcs_url.startswith("gs://"):
                path = gcs_url[5:]
            else:
                path = gcs_url

            parts = path.split("/", 1)
            bucket_name = parts[0]
            blob_name = parts[1] if len(parts) > 1 else ""

            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            return blob.download_as_bytes()

        except ImportError:
            raise RuntimeError("google-cloud-storage required for GCS access")

    async def _fetch_from_http(self, url: str) -> bytes:
        """Fetch data from HTTP URL."""
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content

    async def _post_callback(self, callback_url: str, result: WasmWorkerResult) -> None:
        """
        POST results to callback URL.

        Args:
            callback_url: URL to POST to
            result: Worker result
        """
        try:
            async with httpx.AsyncClient(timeout=self.callback_timeout) as client:
                response = await client.post(
                    callback_url,
                    json=result.to_dict(),
                    headers={
                        "Content-Type": "application/json",
                        "X-Griot-Job-Id": result.job_id,
                        "X-Griot-Job-Type": "wasm_worker",
                    },
                )
                if response.status_code >= 400:
                    logger.error(
                        "Callback failed for job %s: %s %s",
                        result.job_id,
                        response.status_code,
                        response.text[:100],
                    )
        except Exception as e:
            logger.error("Error posting callback for job %s: %s", result.job_id, e)


async def main():
    """Entry point for WASM worker container."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    worker = WasmWorker()
    result = await worker.run()

    if result.success:
        logger.info(
            "WASM worker completed successfully: %d/%d checks passed",
            sum(1 for r in result.check_results if r.passed),
            len(result.check_results),
        )
    else:
        logger.error("WASM worker failed: %s", result.error)
        exit(1)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
