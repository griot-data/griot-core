"""
Registry reporter -- pushes ToolValidationReport to the Griot Registry.

Uses the single-call /contracts/{id}/report endpoint so the full
Run + per-schema Validation lifecycle is handled server-side.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .base import ToolValidationReport

logger = logging.getLogger(__name__)


@dataclass
class ReportResult:
    """Result of reporting to the registry.

    Attributes:
        run_id: ID of the created run.
        status: Final run status ("completed" or "failed").
        schemas_reported: Number of schemas that had validation records posted.
        validations: List of validation IDs created.
    """

    run_id: str
    status: str
    schemas_reported: int = 0
    validations: List[str] = field(default_factory=list)


class RegistryReporter:
    """Report tool validation results to the Griot Registry.

    Uses a single POST to /contracts/{id}/report which creates the
    Run and per-schema Validation records server-side.

    Authenticates via JWT token or API key.

    Example:
        reporter = RegistryReporter(
            registry_url="http://localhost:8000/api/v1",
            token="eyJ...",
        )
        result = reporter.report(
            report=validation_report,
            contract_id="my-contract",
            environment="production",
        )
        print(f"Run {result.run_id}: {result.status}")
    """

    def __init__(
        self,
        registry_url: str,
        token: Optional[str] = None,
        api_key: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        """Initialize the reporter.

        Args:
            registry_url: Base URL of the Griot Registry API.
            token: JWT for Bearer auth.
            api_key: API key for X-API-Key auth.
            client_id: Service account client_id (exchanges for JWT automatically).
            client_secret: Service account client_secret.
        """
        self._registry_url = registry_url
        self._token = token
        self._api_key = api_key
        self._client_id = client_id
        self._client_secret = client_secret
        self._client = None

    def _get_client(self):
        """Lazily create SyncRegistryClient, exchanging SA credentials if needed."""
        if self._client is None:
            from griot_registry.client import SyncRegistryClient

            token = self._token

            # Auto-exchange client_id + client_secret for a JWT
            if not token and self._client_id and self._client_secret:
                token = self._exchange_token()

            self._client = SyncRegistryClient(
                base_url=self._registry_url,
                token=token,
                api_key=self._api_key,
            )
        return self._client

    def _exchange_token(self) -> str:
        """Exchange SA client_id + client_secret for a JWT."""
        import httpx

        response = httpx.post(
            f"{self._registry_url}/api/v1/auth/sa/token",
            json={
                "clientId": self._client_id,
                "clientSecret": self._client_secret,
                "grantType": "client_credentials",
            },
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        token = data.get("accessToken") or data.get("access_token")
        if not token:
            raise ValueError("Token exchange failed: no accessToken in response")
        logger.info("Exchanged SA credentials for JWT")
        return token

    def report(
        self,
        report: ToolValidationReport,
        contract_id: str,
        contract_version: Optional[str] = None,
        environment: str = "development",
        pipeline_id: Optional[str] = None,
        trigger: str = "scheduled",
    ) -> ReportResult:
        """Report results to the registry in a single call.

        Args:
            report: Parsed tool validation report.
            contract_id: ID of the contract being validated.
            contract_version: Version of the contract.
            environment: Environment name (development, staging, production).
            pipeline_id: Pipeline identifier for traceability.
            trigger: How the run was triggered (scheduled, manual, etc.).

        Returns:
            ReportResult with run_id, status, and validation IDs.
        """
        client = self._get_client()

        # Build per-schema results for the /report endpoint
        schemas: List[Dict[str, Any]] = []
        for schema_name, schema_result in report.schemas.items():
            failed = schema_result.failed + schema_result.errored
            schema_payload: Dict[str, Any] = {
                "name": schema_name,
                "passed": failed == 0,
                "total": schema_result.total,
                "failed": schema_result.failed,
                "warned": schema_result.warned,
                "errored": schema_result.errored,
                "skipped": schema_result.skipped,
                "duration_ms": round(schema_result.duration_ms, 2),
                "errors": schema_result.errors[:100],
            }
            # Include individual check results when available
            if schema_result.check_results:
                schema_payload["validation_results"] = [
                    cr.to_dict() for cr in schema_result.check_results
                ]
            schemas.append(schema_payload)

        # Merge parser metadata with reporting metadata (stop dropping dbt_version etc.)
        from datetime import datetime, timezone

        merged_metadata = dict(report.metadata)
        merged_metadata["total_tests"] = report.overall.total
        merged_metadata["reported_at"] = datetime.now(timezone.utc).isoformat()

        # Single call to the registry
        logger.info("Reporting to Griot Registry...")
        result = client.report(
            contract_id=contract_id,
            tool=report.tool,
            environment=environment,
            contract_version=contract_version,
            pipeline_id=pipeline_id,
            trigger=trigger,
            duration_ms=round(report.overall.duration_ms, 2),
            schemas=schemas,
            metadata=merged_metadata,
        )

        run_id = result["run_id"]
        status = result["status"]
        validation_ids = [v["id"] for v in result.get("validations", [])]

        # Log per-schema results
        for v in result.get("validations", []):
            icon = "PASS" if v["passed"] else "FAIL"
            schema = v.get("schema_name") or "(contract-level)"
            logger.info(f"[{icon}] {schema} (validation: {v['id']})")

        logger.info(f"Run {run_id}: {status} ({len(validation_ids)} validations)")

        return ReportResult(
            run_id=run_id,
            status=status,
            schemas_reported=result.get("schemas_reported", len(validation_ids)),
            validations=validation_ids,
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> "RegistryReporter":
        return self

    def __exit__(self, *args) -> None:
        self.close()
