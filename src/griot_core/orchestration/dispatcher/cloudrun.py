"""
Google Cloud Run dispatcher for orchestrated validation jobs.

Dispatches WASM worker jobs and container checks to Cloud Run services.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import httpx

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


class CloudRunDispatcher(ComputeDispatcher):
    """
    Google Cloud Run dispatcher for orchestrated validation jobs.

    Invokes Cloud Run services via HTTP. Services process asynchronously
    and POST results back to callback URLs.

    WASM checks:
        Invokes a Cloud Run service running griot-core WASM worker.
        All WASM checks are executed in one service invocation.

    Container checks:
        Invokes Cloud Run Jobs for container checks.
        Each check runs as a separate Cloud Run Job.

    Example:
        config = DispatcherConfig(
            backend=ComputeBackend.CLOUD_RUN,
        )
        dispatcher = CloudRunDispatcher(
            config,
            project_id="my-project",
            region="us-central1",
            wasm_service_url="https://griot-wasm-worker-xxx.run.app",
        )
    """

    def __init__(
        self,
        config: DispatcherConfig,
        project_id: str | None = None,
        region: str = "us-central1",
        wasm_service_url: str | None = None,
        use_auth: bool = True,
    ):
        """
        Initialize Cloud Run dispatcher.

        Args:
            config: Dispatcher configuration
            project_id: GCP project ID
            region: Cloud Run region
            wasm_service_url: URL of the WASM worker Cloud Run service
            use_auth: Whether to use GCP authentication
        """
        super().__init__(config)
        self.project_id = project_id
        self.region = region
        self.wasm_service_url = wasm_service_url
        self.use_auth = use_auth
        self._client: httpx.AsyncClient | None = None
        self._credentials = None

    @property
    def backend(self) -> ComputeBackend:
        """Return Cloud Run backend type."""
        return ComputeBackend.CLOUD_RUN

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=float(self.config.timeout_seconds),
                    write=30.0,
                    pool=10.0,
                )
            )
        return self._client

    def _get_auth_token(self, audience: str) -> str | None:
        """
        Get GCP identity token for Cloud Run auth.

        Args:
            audience: The target service URL

        Returns:
            Identity token string or None if auth disabled
        """
        if not self.use_auth:
            return None

        try:
            import google.auth
            import google.oauth2.id_token
            from google.auth.transport.requests import Request

            if self._credentials is None:
                self._credentials, _ = google.auth.default()

            token = google.oauth2.id_token.fetch_id_token(Request(), audience)
            return token

        except ImportError:
            logger.warning(
                "google-auth not installed. Install with: "
                "pip install google-auth google-auth-httplib2"
            )
            return None
        except Exception as e:
            logger.error("Failed to get GCP auth token: %s", e)
            return None

    async def dispatch_wasm_worker(self, spec: WasmJobSpec) -> DispatchResult:
        """
        Dispatch WASM checks to Cloud Run service.

        Sends HTTP POST to the WASM worker service. The service
        processes asynchronously and POSTs results to callback URL.

        Args:
            spec: WASM job specification

        Returns:
            DispatchResult with invocation status
        """
        if not self.wasm_service_url:
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="wasm_worker",
                backend=self.backend.value,
                error="wasm_service_url not configured",
            )

        try:
            client = await self._get_client()

            request_id = str(uuid.uuid4())
            headers = {
                "Content-Type": "application/json",
                "X-Griot-Job-Id": spec.job_id,
                "X-Griot-Job-Type": "wasm_worker",
                "X-Request-Id": request_id,
            }

            token = self._get_auth_token(self.wasm_service_url)
            if token:
                headers["Authorization"] = f"Bearer {token}"

            payload = {
                "job_type": "wasm_worker",
                "spec": spec.to_dict(),
            }

            response = await client.post(
                self.wasm_service_url,
                json=payload,
                headers=headers,
            )

            if response.status_code in (200, 202):
                logger.info(
                    "Dispatched WASM job %s to Cloud Run (request_id=%s)",
                    spec.job_id,
                    request_id,
                )
                return DispatchResult(
                    success=True,
                    job_id=spec.job_id,
                    job_type="wasm_worker",
                    backend=self.backend.value,
                    invocation_id=request_id,
                )
            else:
                error_msg = f"Cloud Run returned {response.status_code}: {response.text[:200]}"
                logger.error("Failed to dispatch WASM job %s: %s", spec.job_id, error_msg)
                return DispatchResult(
                    success=False,
                    job_id=spec.job_id,
                    job_type="wasm_worker",
                    backend=self.backend.value,
                    error=error_msg,
                )

        except httpx.TimeoutException as e:
            error_msg = f"Request timeout: {e}"
            logger.error("Timeout dispatching WASM job %s: %s", spec.job_id, error_msg)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="wasm_worker",
                backend=self.backend.value,
                error=error_msg,
            )
        except Exception as e:
            logger.exception("Error dispatching WASM job %s to Cloud Run", spec.job_id)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="wasm_worker",
                backend=self.backend.value,
                error=str(e),
            )

    async def dispatch_container(self, spec: ContainerJobSpec) -> DispatchResult:
        """
        Dispatch container check as Cloud Run Job.

        Creates a Cloud Run Job that runs the check's container image.
        The job executes asynchronously.

        Args:
            spec: Container job specification

        Returns:
            DispatchResult with invocation status
        """
        try:
            # Use Cloud Run Jobs API
            job_name = f"griot-check-{spec.job_id[:8]}-{uuid.uuid4().hex[:6]}"

            from google.cloud import run_v2

            client = run_v2.JobsClient()

            # Build the job configuration
            job = run_v2.Job(
                template=run_v2.ExecutionTemplate(
                    template=run_v2.TaskTemplate(
                        containers=[
                            run_v2.Container(
                                image=spec.image,
                                env=[
                                    run_v2.EnvVar(
                                        name="GRIOT_JOB_SPEC",
                                        value=json.dumps(spec.to_dict()),
                                    ),
                                    run_v2.EnvVar(
                                        name="GRIOT_JOB_ID",
                                        value=spec.job_id,
                                    ),
                                    run_v2.EnvVar(
                                        name="GRIOT_JOB_TYPE",
                                        value="container_check",
                                    ),
                                    run_v2.EnvVar(
                                        name="GRIOT_CHECK_NAME",
                                        value=spec.check.name,
                                    ),
                                ],
                                resources=run_v2.ResourceRequirements(
                                    limits={
                                        "memory": f"{self.config.memory_mb}Mi",
                                        "cpu": str(self.config.cpu_millicores / 1000),
                                    }
                                ),
                            )
                        ],
                        timeout=f"{spec.timeout_seconds}s",
                        max_retries=self.config.retry_count,
                    )
                )
            )

            parent = f"projects/{self.project_id}/locations/{self.region}"

            # Create the job
            operation = client.create_job(
                parent=parent,
                job=job,
                job_id=job_name,
            )
            created_job = operation.result()

            # Run the job
            execution_operation = client.run_job(name=created_job.name)
            execution = execution_operation.result()

            logger.info(
                "Created Cloud Run Job %s for check %s",
                job_name,
                spec.check.name,
            )

            return DispatchResult(
                success=True,
                job_id=spec.job_id,
                job_type="container",
                backend=self.backend.value,
                invocation_id=execution.name,
            )

        except ImportError:
            logger.error(
                "google-cloud-run not installed. Install with: pip install google-cloud-run"
            )
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="container",
                backend=self.backend.value,
                error="google-cloud-run not installed",
            )
        except Exception as e:
            logger.exception("Error dispatching container check %s to Cloud Run", spec.check.name)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="container",
                backend=self.backend.value,
                error=str(e),
            )

    async def check_status(self, invocation_id: str) -> dict[str, Any]:
        """
        Check Cloud Run invocation status.

        Args:
            invocation_id: The request ID or job name from dispatch

        Returns:
            Status information
        """
        # For WASM worker (HTTP request), status comes from callback
        if not invocation_id.startswith("projects/"):
            return {
                "invocation_id": invocation_id,
                "status": "unknown",
                "note": "HTTP invocations don't provide status. Check callback results.",
            }

        # For Cloud Run Jobs, check execution status
        try:
            from google.cloud import run_v2

            client = run_v2.ExecutionsClient()
            execution = client.get_execution(name=invocation_id)

            return {
                "invocation_id": invocation_id,
                "status": execution.reconciling,
                "succeeded": execution.succeeded_count,
                "failed": execution.failed_count,
                "running": execution.running_count,
                "completion_time": (
                    execution.completion_time.isoformat() if execution.completion_time else None
                ),
            }
        except Exception as e:
            logger.error("Error checking Cloud Run execution status: %s", e)
            return {"error": str(e)}

    async def cancel(self, invocation_id: str) -> bool:
        """
        Cancel a Cloud Run execution.

        Args:
            invocation_id: The execution name to cancel

        Returns:
            True if cancellation was successful
        """
        if not invocation_id.startswith("projects/"):
            logger.warning(
                "Cannot cancel HTTP invocation %s - not supported",
                invocation_id,
            )
            return False

        try:
            from google.cloud import run_v2

            client = run_v2.ExecutionsClient()
            client.cancel_execution(name=invocation_id)
            logger.info("Cancelled Cloud Run execution %s", invocation_id)
            return True
        except Exception as e:
            logger.error("Error cancelling Cloud Run execution: %s", e)
            return False

    async def health_check(self) -> bool:
        """
        Check if Cloud Run service is healthy.

        Returns:
            True if WASM worker service responds
        """
        if not self.wasm_service_url:
            return False

        try:
            client = await self._get_client()

            headers = {}
            token = self._get_auth_token(self.wasm_service_url)
            if token:
                headers["Authorization"] = f"Bearer {token}"

            for path in ["/health", "/", ""]:
                try:
                    url = self.wasm_service_url.rstrip("/") + path
                    response = await client.get(url, headers=headers, timeout=10.0)
                    if response.status_code < 500:
                        return True
                except httpx.HTTPError:
                    continue

            return False

        except Exception as e:
            logger.error("Cloud Run health check failed: %s", e)
            return False

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
