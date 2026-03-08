"""
Google Cloud Run worker for serverless container validation.

Provides the HTTP handler and worker implementation for running
validation jobs on Google Cloud Run.
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


class CloudRunWorker(Worker):
    """
    Google Cloud Run worker for serverless container validation.

    Designed to run as a Cloud Run service that receives HTTP requests.
    Supports both synchronous and asynchronous (Pub/Sub) invocation.

    Environment Variables:
        GRIOT_REGISTRY_URL: URL of the Griot registry
        GRIOT_WORKER_ID: Worker identifier (defaults to K_REVISION)
        GRIOT_DEFAULT_TIMEOUT: Default timeout in seconds
        K_REVISION: Cloud Run revision name (set by Cloud Run)
        K_SERVICE: Cloud Run service name (set by Cloud Run)
        PORT: HTTP port (set by Cloud Run, default 8080)

    Example Cloud Run Service:
        gcloud run deploy griot-worker \\
            --image gcr.io/project/griot-worker:latest \\
            --set-env-vars GRIOT_REGISTRY_URL=https://registry.example.com \\
            --memory 1Gi \\
            --timeout 300s
    """

    def __init__(
        self,
        config: Optional[WorkerConfig] = None,
        validation_engine: Optional[ValidationEngine] = None,
    ):
        """
        Initialize the Cloud Run worker.

        Args:
            config: Worker configuration (defaults to env-based config)
            validation_engine: Optional pre-configured validation engine
        """
        if config is None:
            config = WorkerConfig(
                worker_id=os.environ.get(
                    "GRIOT_WORKER_ID",
                    os.environ.get("K_REVISION", f"cloudrun-{uuid.uuid4().hex[:8]}"),
                ),
                worker_type="cloudrun",
                default_timeout=int(os.environ.get("GRIOT_DEFAULT_TIMEOUT", "300")),
                registry_url=os.environ.get("GRIOT_REGISTRY_URL"),
                callback_enabled=True,
                extra_options={
                    "service": os.environ.get("K_SERVICE"),
                    "revision": os.environ.get("K_REVISION"),
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

    async def handle_request(
        self,
        request_data: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Handle an HTTP request.

        Supports:
        - Direct HTTP POST with JSON body
        - Cloud Pub/Sub push messages
        - Cloud Tasks HTTP requests

        Args:
            request_data: Request body as dictionary
            headers: Optional request headers

        Returns:
            Response dictionary
        """
        try:
            # Parse payload from request
            payload = self._parse_request(request_data)

            # Execute validation
            result = await self.execute(payload)

            # Return response
            return {
                "status": "success" if result.status == WorkerStatus.COMPLETED else "error",
                "result": result.to_dict(),
            }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
            }

    def _parse_request(self, data: Dict[str, Any]) -> JobPayload:
        """
        Parse JobPayload from request data.

        Handles different request formats.
        """
        # Pub/Sub push message
        if "message" in data:
            message = data["message"]
            if "data" in message:
                decoded = base64.b64decode(message["data"]).decode("utf-8")
                return self._parse_direct_request(json.loads(decoded))

        # Cloud Tasks
        if "taskName" in data:
            return self._parse_direct_request(data.get("payload", data))

        # Direct request
        return self._parse_direct_request(data)

    def _parse_direct_request(self, data: Dict[str, Any]) -> JobPayload:
        """Parse JobPayload from direct request data."""
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
            job_id=data.get("job_id", f"cloudrun-{uuid.uuid4().hex[:8]}"),
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
            worker_type="cloudrun",
            metadata={
                "service": self.config.extra_options.get("service"),
                "revision": self.config.extra_options.get("revision"),
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
            "worker_type": "cloudrun",
            "service": self.config.extra_options.get("service"),
            "revision": self.config.extra_options.get("revision"),
            "registry_url": self.config.registry_url,
            "engine_capabilities": self._engine.get_runtime_capabilities(),
        }


# Flask/WSGI application for Cloud Run
def create_app():
    """
    Create Flask application for Cloud Run.

    Returns:
        Flask application
    """
    try:
        from flask import Flask, jsonify, request
    except ImportError:
        raise ImportError(
            "Flask is required for Cloud Run HTTP handler. Install with: pip install flask"
        )

    app = Flask(__name__)
    worker = CloudRunWorker()

    @app.route("/", methods=["POST"])
    def handle_validation():
        """Handle validation request."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                worker.handle_request(
                    request.get_json(),
                    dict(request.headers),
                )
            )
            status_code = 200 if result.get("status") == "success" else 500
            return jsonify(result), status_code
        finally:
            loop.close()

    @app.route("/health", methods=["GET"])
    def health():
        """Health check endpoint."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            health_status = loop.run_until_complete(worker.health_check())
            return jsonify(health_status), 200
        finally:
            loop.close()

    return app


def main():
    """
    Main entry point for Cloud Run.

    Starts the Flask server on the configured port.
    """
    app = create_app()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
