"""
Local dispatcher for orchestrated validation jobs (testing/development).

Executes validation jobs locally for development and testing.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

from griot_core.orchestration.dispatcher.base import (
    ComputeBackend,
    ComputeDispatcher,
    DispatcherConfig,
)
from griot_core.orchestration.types import (
    ContainerJobSpec,
    DispatchResult,
    WasmJobSpec,
)

logger = logging.getLogger(__name__)


class LocalDispatcher(ComputeDispatcher):
    """
    Local dispatcher for development and testing.

    Executes validation jobs in-process using griot-core.
    Results are posted to the callback URL after completion.

    WARNING: This is for development only. In production, use
    Kubernetes, Lambda, or Cloud Run.

    Example:
        config = DispatcherConfig(
            backend=ComputeBackend.LOCAL,
            timeout_seconds=60,
        )
        dispatcher = LocalDispatcher(config)
        result = await dispatcher.dispatch_wasm_worker(wasm_spec)
    """

    def __init__(
        self,
        config: DispatcherConfig,
        max_workers: int = 4,
        callback_timeout: float = 30.0,
    ):
        """
        Initialize local dispatcher.

        Args:
            config: Dispatcher configuration
            max_workers: Max concurrent job threads
            callback_timeout: Timeout for callback POST requests
        """
        super().__init__(config)
        self.max_workers = max_workers
        self.callback_timeout = callback_timeout
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._running_jobs: dict[str, asyncio.Task] = {}

    @property
    def backend(self) -> ComputeBackend:
        """Return local backend type."""
        return ComputeBackend.LOCAL

    async def dispatch_wasm_worker(self, spec: WasmJobSpec) -> DispatchResult:
        """
        Dispatch WASM checks for local execution.

        Runs WASM checks in a background task using griot-core's
        WASM runtime.

        Args:
            spec: WASM job specification

        Returns:
            DispatchResult with local execution ID
        """
        try:
            invocation_id = f"local-wasm-{uuid.uuid4().hex[:12]}"

            task = asyncio.create_task(
                self._execute_wasm_job(spec, invocation_id),
                name=f"griot-wasm-{spec.job_id}",
            )
            self._running_jobs[invocation_id] = task

            logger.info(
                "Dispatched WASM job %s for local execution (id=%s)",
                spec.job_id,
                invocation_id,
            )

            return DispatchResult(
                success=True,
                job_id=spec.job_id,
                job_type="wasm_worker",
                backend=self.backend.value,
                invocation_id=invocation_id,
            )

        except Exception as e:
            logger.exception("Error dispatching local WASM job %s", spec.job_id)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="wasm_worker",
                backend=self.backend.value,
                error=str(e),
            )

    async def dispatch_container(self, spec: ContainerJobSpec) -> DispatchResult:
        """
        Dispatch container check for local execution.

        For local testing, runs container checks using the container
        runtime if available (podman/docker).

        Args:
            spec: Container job specification

        Returns:
            DispatchResult with local execution ID
        """
        try:
            invocation_id = f"local-container-{uuid.uuid4().hex[:12]}"

            task = asyncio.create_task(
                self._execute_container_job(spec, invocation_id),
                name=f"griot-container-{spec.job_id}",
            )
            self._running_jobs[invocation_id] = task

            logger.info(
                "Dispatched container check %s for local execution (id=%s)",
                spec.check.name,
                invocation_id,
            )

            return DispatchResult(
                success=True,
                job_id=spec.job_id,
                job_type="container",
                backend=self.backend.value,
                invocation_id=invocation_id,
            )

        except Exception as e:
            logger.exception("Error dispatching local container check %s", spec.check.name)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="container",
                backend=self.backend.value,
                error=str(e),
            )

    async def _execute_wasm_job(self, spec: WasmJobSpec, invocation_id: str) -> dict[str, Any]:
        """
        Execute WASM job locally.

        Args:
            spec: WASM job specification
            invocation_id: Local invocation ID

        Returns:
            Execution result
        """

        result = {
            "job_id": spec.job_id,
            "invocation_id": invocation_id,
            "job_type": "wasm_worker",
            "success": False,
            "error": None,
            "check_results": [],
        }

        try:
            from griot_core.workers.wasm_worker import WasmWorker

            worker = WasmWorker()
            execution_result = await worker.execute_wasm_checks(spec)

            result["success"] = execution_result.get("success", False)
            result["check_results"] = execution_result.get("check_results", [])

        except ImportError:
            logger.warning(
                "WasmWorker not available, returning mock result for job %s",
                spec.job_id,
            )
            # Return mock results for each check
            result["success"] = True
            result["check_results"] = [
                {
                    "check_name": check.name,
                    "passed": True,
                    "mock": True,
                    "note": "WasmWorker not available",
                }
                for check in spec.checks
            ]

        except Exception as e:
            logger.exception("Error executing local WASM job %s", spec.job_id)
            result["error"] = str(e)

        finally:
            self._running_jobs.pop(invocation_id, None)

        # POST results to callback URL
        if spec.callback_url:
            await self._post_callback(spec.callback_url, result, spec.job_id)

        return result

    async def _execute_container_job(
        self, spec: ContainerJobSpec, invocation_id: str
    ) -> dict[str, Any]:
        """
        Execute container job locally.

        Args:
            spec: Container job specification
            invocation_id: Local invocation ID

        Returns:
            Execution result
        """

        result = {
            "job_id": spec.job_id,
            "parent_job_id": spec.parent_job_id,
            "invocation_id": invocation_id,
            "job_type": "container_check",
            "check_name": spec.check.name,
            "success": False,
            "error": None,
            "check_result": None,
        }

        try:
            from griot_core.executors.container_runtime import (
                ContainerRuntime,
            )

            runtime = ContainerRuntime()

            if not runtime.is_available():
                logger.warning(
                    "Container runtime not available for check %s, returning mock result",
                    spec.check.name,
                )
                result["success"] = True
                result["check_result"] = {
                    "passed": True,
                    "mock": True,
                    "note": "Container runtime not available",
                }
            else:
                # Run the container
                container_result = await runtime.run_check_container(  # type: ignore[attr-defined]
                    image=spec.image,
                    check_name=spec.check.name,
                    parameters=spec.check.parameters,
                    data_reference=spec.data_reference,
                    timeout=spec.timeout_seconds,
                )
                result["success"] = container_result.get("success", False)
                result["check_result"] = container_result

        except ImportError:
            logger.warning(
                "ContainerRuntime not available for check %s, returning mock result",
                spec.check.name,
            )
            result["success"] = True
            result["check_result"] = {
                "passed": True,
                "mock": True,
                "note": "ContainerRuntime not available",
            }

        except Exception as e:
            logger.exception("Error executing local container check %s", spec.check.name)
            result["error"] = str(e)

        finally:
            self._running_jobs.pop(invocation_id, None)

        # POST results to callback URL
        if spec.callback_url:
            await self._post_callback(spec.callback_url, result, spec.job_id)

        return result

    async def _post_callback(self, callback_url: str, result: dict[str, Any], job_id: str) -> None:
        """
        POST results to callback URL.

        Args:
            callback_url: URL to POST to
            result: Result data
            job_id: Job ID for headers
        """
        try:
            async with httpx.AsyncClient(timeout=self.callback_timeout) as client:
                response = await client.post(
                    callback_url,
                    json=result,
                    headers={
                        "Content-Type": "application/json",
                        "X-Griot-Job-Id": job_id,
                    },
                )
                if response.status_code >= 400:
                    logger.error(
                        "Callback failed for job %s: %s %s",
                        job_id,
                        response.status_code,
                        response.text[:100],
                    )
        except Exception as e:
            logger.error("Error posting callback for job %s: %s", job_id, e)

    async def check_status(self, invocation_id: str) -> dict[str, Any]:
        """
        Check local job status.

        Args:
            invocation_id: The local invocation ID

        Returns:
            Job status information
        """
        task = self._running_jobs.get(invocation_id)

        if task is None:
            return {
                "invocation_id": invocation_id,
                "status": "completed",
                "note": "Job not in running list (completed or not found)",
            }

        if task.done():
            try:
                task.result()
                status = "completed"
            except asyncio.CancelledError:
                status = "cancelled"
            except Exception as e:
                status = f"failed: {e}"

            return {
                "invocation_id": invocation_id,
                "status": status,
            }

        return {
            "invocation_id": invocation_id,
            "status": "running",
        }

    async def cancel(self, invocation_id: str) -> bool:
        """
        Cancel a local job.

        Args:
            invocation_id: The invocation ID to cancel

        Returns:
            True if cancellation was successful
        """
        task = self._running_jobs.get(invocation_id)

        if task is None:
            logger.warning("Job %s not found for cancellation", invocation_id)
            return False

        if task.done():
            logger.info("Job %s already completed", invocation_id)
            return False

        task.cancel()
        logger.info("Cancelled local job %s", invocation_id)
        return True

    async def health_check(self) -> bool:
        """Local dispatcher is always healthy."""
        return True

    async def shutdown(self) -> None:
        """Shutdown the dispatcher and cancel running jobs."""
        for invocation_id, task in list(self._running_jobs.items()):
            if not task.done():
                task.cancel()
                logger.info("Cancelled job %s during shutdown", invocation_id)

        self._executor.shutdown(wait=False)
