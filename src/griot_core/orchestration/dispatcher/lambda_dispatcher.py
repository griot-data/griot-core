"""
AWS Lambda dispatcher for orchestrated validation jobs.

Dispatches WASM worker jobs and container checks to Lambda functions.
Container checks use Lambda container image support.
"""

from __future__ import annotations

import json
import logging
from typing import Any

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


class LambdaDispatcher(ComputeDispatcher):
    """
    AWS Lambda dispatcher for orchestrated validation jobs.

    Uses Lambda's async invocation for fire-and-forget execution.
    Results are posted back to the callback URL.

    WASM checks:
        Invokes a Lambda function running griot-core WASM worker.
        All WASM checks are executed sequentially in one invocation.

    Container checks:
        Invokes Lambda functions deployed with container images.
        Each check is a separate Lambda invocation.

    Example:
        config = DispatcherConfig(
            backend=ComputeBackend.AWS_LAMBDA,
            environment={"GRIOT_REGISTRY_URL": "https://..."},
        )
        dispatcher = LambdaDispatcher(
            config,
            wasm_function_name="griot-wasm-worker",
        )
    """

    def __init__(
        self,
        config: DispatcherConfig,
        wasm_function_name: str = "griot-wasm-worker",
        container_function_prefix: str = "griot-check-",
    ):
        """
        Initialize Lambda dispatcher.

        Args:
            config: Dispatcher configuration
            wasm_function_name: Lambda function name for WASM worker
            container_function_prefix: Prefix for container check function names
        """
        super().__init__(config)
        self.wasm_function_name = wasm_function_name
        self.container_function_prefix = container_function_prefix
        self._client = None

    @property
    def backend(self) -> ComputeBackend:
        """Return AWS Lambda backend type."""
        return ComputeBackend.AWS_LAMBDA

    def _get_client(self):
        """Get or create boto3 Lambda client."""
        if self._client is None:
            try:
                import boto3

                self._client = boto3.client("lambda")
            except ImportError:
                raise RuntimeError(
                    "boto3 is required for Lambda dispatcher. Install with: pip install boto3"
                )
        return self._client

    async def dispatch_wasm_worker(self, spec: WasmJobSpec) -> DispatchResult:
        """
        Dispatch WASM checks to Lambda function.

        Uses async invocation so the registry doesn't wait for completion.
        The worker POSTs results to callback_url.

        Args:
            spec: WASM job specification

        Returns:
            DispatchResult with invocation status
        """
        try:
            client = self._get_client()

            payload = {
                "job_type": "wasm_worker",
                "spec": spec.to_dict(),
            }

            response = client.invoke(
                FunctionName=self.wasm_function_name,
                InvocationType="Event",  # Async
                Payload=json.dumps(payload),
            )

            status_code = response.get("StatusCode", 0)
            request_id = response.get("ResponseMetadata", {}).get("RequestId")

            if status_code == 202:
                logger.info(
                    "Dispatched WASM job %s to Lambda %s (request_id=%s)",
                    spec.job_id,
                    self.wasm_function_name,
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
                error_msg = f"Unexpected status code: {status_code}"
                logger.error("Failed to dispatch WASM job %s: %s", spec.job_id, error_msg)
                return DispatchResult(
                    success=False,
                    job_id=spec.job_id,
                    job_type="wasm_worker",
                    backend=self.backend.value,
                    error=error_msg,
                )

        except Exception as e:
            logger.exception("Error dispatching WASM job %s to Lambda", spec.job_id)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="wasm_worker",
                backend=self.backend.value,
                error=str(e),
            )

    async def dispatch_container(self, spec: ContainerJobSpec) -> DispatchResult:
        """
        Dispatch container check to Lambda function.

        The Lambda function should be deployed with the check's
        container image using Lambda's container image support.

        Args:
            spec: Container job specification

        Returns:
            DispatchResult with invocation status
        """
        try:
            client = self._get_client()

            # Derive Lambda function name from check executor URI
            # Convention: oci://griot/checks/null-check:1.0 -> griot-check-null-check
            function_name = self._derive_function_name(spec)

            payload = {
                "job_type": "container_check",
                "spec": spec.to_dict(),
            }

            response = client.invoke(
                FunctionName=function_name,
                InvocationType="Event",  # Async
                Payload=json.dumps(payload),
            )

            status_code = response.get("StatusCode", 0)
            request_id = response.get("ResponseMetadata", {}).get("RequestId")

            if status_code == 202:
                logger.info(
                    "Dispatched container check %s to Lambda %s (request_id=%s)",
                    spec.check.name,
                    function_name,
                    request_id,
                )
                return DispatchResult(
                    success=True,
                    job_id=spec.job_id,
                    job_type="container",
                    backend=self.backend.value,
                    invocation_id=request_id,
                )
            else:
                error_msg = f"Unexpected status code: {status_code}"
                logger.error(
                    "Failed to dispatch container check %s: %s",
                    spec.check.name,
                    error_msg,
                )
                return DispatchResult(
                    success=False,
                    job_id=spec.job_id,
                    job_type="container",
                    backend=self.backend.value,
                    error=error_msg,
                )

        except Exception as e:
            logger.exception("Error dispatching container check %s to Lambda", spec.check.name)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="container",
                backend=self.backend.value,
                error=str(e),
            )

    def _derive_function_name(self, spec: ContainerJobSpec) -> str:
        """
        Derive Lambda function name from container spec.

        Convention: The function name is the container_function_prefix
        plus the sanitized check name.

        Args:
            spec: Container job specification

        Returns:
            Lambda function name
        """
        # Sanitize check name for Lambda
        safe_name = spec.check.name.replace("_", "-").replace(".", "-").lower()
        return f"{self.container_function_prefix}{safe_name}"

    async def check_status(self, invocation_id: str) -> dict[str, Any]:
        """
        Check Lambda invocation status.

        Lambda async invocations don't provide status tracking.
        Status comes from the worker callback.

        Args:
            invocation_id: The request ID from dispatch

        Returns:
            Status information (limited for async Lambda)
        """
        return {
            "invocation_id": invocation_id,
            "status": "unknown",
            "note": "Lambda async invocations don't provide status. "
            "Check validation_jobs table for callback results.",
        }

    async def cancel(self, invocation_id: str) -> bool:
        """
        Cancel a Lambda invocation.

        Lambda async invocations cannot be cancelled once started.

        Args:
            invocation_id: The request ID to cancel

        Returns:
            False - Lambda async invocations cannot be cancelled
        """
        logger.warning(
            "Cannot cancel Lambda async invocation %s - not supported",
            invocation_id,
        )
        return False

    async def health_check(self) -> bool:
        """
        Check if Lambda function is available.

        Returns:
            True if WASM worker function exists and is active
        """
        try:
            client = self._get_client()
            response = client.get_function(FunctionName=self.wasm_function_name)
            state = response.get("Configuration", {}).get("State", "Unknown")
            return state == "Active"
        except Exception as e:
            logger.error("Lambda health check failed: %s", e)
            return False
