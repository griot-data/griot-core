"""
AWS Lambda worker for serverless validation.

Provides the Lambda handler function and worker implementation
for running validation jobs on AWS Lambda.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
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


class LambdaWorker(Worker):
    """
    AWS Lambda worker for serverless validation.

    Designed to be invoked by AWS Lambda runtime. Handles:
    - Event parsing from various invocation sources
    - Contract fetching from registry or S3
    - Validation execution
    - Result reporting via callback or response

    Environment Variables:
        GRIOT_REGISTRY_URL: URL of the Griot registry
        GRIOT_WORKER_ID: Worker identifier (defaults to Lambda function name)
        GRIOT_DEFAULT_TIMEOUT: Default timeout in seconds
        AWS_LAMBDA_FUNCTION_NAME: Lambda function name (set by Lambda)

    Example Lambda handler:
        >>> def handler(event, context):
        ...     worker = LambdaWorker()
        ...     return asyncio.get_event_loop().run_until_complete(
        ...         worker.handle_event(event, context)
        ...     )
    """

    def __init__(
        self,
        config: Optional[WorkerConfig] = None,
        validation_engine: Optional[ValidationEngine] = None,
    ):
        """
        Initialize the Lambda worker.

        Args:
            config: Worker configuration (defaults to env-based config)
            validation_engine: Optional pre-configured validation engine
        """
        if config is None:
            config = WorkerConfig(
                worker_id=os.environ.get(
                    "GRIOT_WORKER_ID",
                    os.environ.get("AWS_LAMBDA_FUNCTION_NAME", f"lambda-{uuid.uuid4().hex[:8]}"),
                ),
                worker_type="lambda",
                default_timeout=int(os.environ.get("GRIOT_DEFAULT_TIMEOUT", "300")),
                registry_url=os.environ.get("GRIOT_REGISTRY_URL"),
                callback_enabled=True,
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

    async def handle_event(
        self,
        event: Dict[str, Any],
        context: Any = None,
    ) -> Dict[str, Any]:
        """
        Handle a Lambda invocation event.

        Supports multiple event sources:
        - Direct invocation with JobPayload
        - API Gateway (HTTP POST with JSON body)
        - SQS messages
        - EventBridge events

        Args:
            event: Lambda event
            context: Lambda context

        Returns:
            Response dictionary with results
        """
        try:
            # Parse payload from various event sources
            payload = self._parse_event(event)

            # Get remaining time if context available
            if context and hasattr(context, "get_remaining_time_in_millis"):
                remaining_ms = context.get_remaining_time_in_millis()
                # Leave 5 seconds buffer for cleanup
                max_timeout = (remaining_ms - 5000) / 1000
                if payload.timeout_seconds > max_timeout:
                    payload.timeout_seconds = int(max_timeout)

            # Execute validation
            result = await self.execute(payload)

            # Return response
            return {
                "statusCode": 200 if result.status == WorkerStatus.COMPLETED else 500,
                "body": result.to_json(),
                "headers": {
                    "Content-Type": "application/json",
                },
            }

        except Exception as e:
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {
                        "error": str(e),
                        "error_type": type(e).__name__,
                    }
                ),
                "headers": {
                    "Content-Type": "application/json",
                },
            }

    def _parse_event(self, event: Dict[str, Any]) -> JobPayload:
        """
        Parse JobPayload from Lambda event.

        Handles different event source formats.
        """
        # Direct invocation
        if "job_id" in event:
            return self._parse_direct_event(event)

        # API Gateway
        if "body" in event:
            body = event["body"]
            if isinstance(body, str):
                body = json.loads(body)
            return self._parse_direct_event(body)

        # SQS
        if "Records" in event and event["Records"]:
            record = event["Records"][0]
            if "body" in record:
                body = json.loads(record["body"])
                return self._parse_direct_event(body)

        # EventBridge
        if "detail" in event:
            return self._parse_direct_event(event["detail"])

        raise ValueError("Unable to parse event format")

    def _parse_direct_event(self, data: Dict[str, Any]) -> JobPayload:
        """Parse JobPayload from direct event data."""
        # Handle base64-encoded Arrow data
        arrow_data = None
        if "arrow_data" in data:
            arrow_data = {}
            for key, value in data["arrow_data"].items():
                if isinstance(value, str):
                    arrow_data[key] = base64.b64decode(value)
                else:
                    arrow_data[key] = value

        return JobPayload(
            job_id=data.get("job_id", f"lambda-{uuid.uuid4().hex[:8]}"),
            contract_id=data["contract_id"],
            contract_version=data.get("contract_version"),
            profile=data.get("profile", "default"),
            environment=data.get("environment", "production"),
            arrow_data=arrow_data,
            options=data.get("options", {}),
            callback_url=data.get("callback_url"),
            timeout_seconds=data.get("timeout_seconds", self.config.default_timeout),
            metadata=data.get("metadata", {}),
        )

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
            worker_type="lambda",
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

        # Report result if callback configured
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

        # Check cache
        if cache_key in self._contracts_cache:
            return self._contracts_cache[cache_key]

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

        raise ValueError(f"Contract {contract_id} not found and GRIOT_REGISTRY_URL not configured")

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
        Report results back to the registry via callback URL.

        Args:
            result: Worker result to report

        Returns:
            True if successful
        """
        if not result.metadata.get("callback_url"):
            return True

        try:
            import urllib.request

            url = result.metadata["callback_url"]
            data = result.to_json().encode("utf-8")

            req = urllib.request.Request(
                url,
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
            "worker_type": "lambda",
            "registry_url": self.config.registry_url,
            "engine_capabilities": self._engine.get_runtime_capabilities(),
        }


# Lambda handler function
def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function.

    This is the entry point for Lambda invocations.

    Args:
        event: Lambda event
        context: Lambda context

    Returns:
        Response dictionary
    """
    worker = LambdaWorker()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(worker.handle_event(event, context))
    finally:
        loop.close()
