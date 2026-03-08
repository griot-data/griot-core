"""
Prefect integration for Griot validation.

Provides Prefect tasks for running Griot validation as part of Prefect flows.

Requirements:
    pip install prefect

Example Flow:
    from prefect import flow
    from griot_core.integrations.prefect import griot_validate

    @flow
    def data_pipeline():
        # Run validation
        result = griot_validate(
            contract_id="sales-contract",
            profile="data_engineering",
        )

        if result.is_valid:
            # Continue with pipeline
            ...
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:
    from prefect import get_run_logger, task
    from prefect.tasks import task_input_hash

    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False

    # Create stub decorators when Prefect not installed
    def task(*args, **kwargs):
        def decorator(func):
            return func

        return decorator if not args else decorator(args[0])

    def get_run_logger():
        import logging

        return logging.getLogger("griot")

    def task_input_hash(*args, **kwargs):
        return None


@dataclass
class ValidationResult:
    """
    Result from Griot validation in Prefect context.

    Attributes:
        is_valid: Whether validation passed
        contract_id: Contract that was validated
        profile: Profile that was used
        duration_ms: Validation duration in milliseconds
        check_results: Detailed check results
        errors: List of error messages
    """

    is_valid: bool
    contract_id: str
    profile: str
    duration_ms: Optional[float] = None
    check_results: Optional[Dict[str, Any]] = None
    errors: Optional[list] = None

    def raise_on_invalid(self, message: Optional[str] = None) -> None:
        """
        Raise an exception if validation failed.

        Args:
            message: Optional custom error message

        Raises:
            ValueError: If validation failed
        """
        if not self.is_valid:
            msg = message or f"Validation failed for contract {self.contract_id}"
            if self.errors:
                msg += f": {', '.join(self.errors)}"
            raise ValueError(msg)


@task(
    name="griot_validate",
    description="Validate data against a Griot contract",
    retries=1,
    retry_delay_seconds=30,
)
def griot_validate(
    contract_id: str,
    profile: str = "default",
    environment: str = "production",
    registry_url: Optional[str] = None,
    arrow_data: Optional[Dict[str, bytes]] = None,
    arrow_data_path: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    timeout: int = 300,
    fail_on_invalid: bool = False,
) -> ValidationResult:
    """
    Prefect task for running Griot validation.

    Validates data against a contract and returns the result.
    Can optionally fail the task on validation failure.

    Args:
        contract_id: ID of the contract to validate
        profile: Execution profile
        environment: Environment (production, staging, dev)
        registry_url: URL of the Griot registry
        arrow_data: Arrow IPC data keyed by schema ID
        arrow_data_path: Path to Arrow data file (alternative to arrow_data)
        options: Additional validation options
        timeout: Timeout in seconds
        fail_on_invalid: Whether to fail the task if validation fails

    Returns:
        ValidationResult with validation details

    Raises:
        ValueError: If fail_on_invalid is True and validation fails

    Example:
        >>> @flow
        ... def my_pipeline():
        ...     result = griot_validate(
        ...         contract_id="users-contract",
        ...         profile="data_engineering",
        ...         fail_on_invalid=True,
        ...     )
        ...     print(f"Valid: {result.is_valid}")
    """
    if not PREFECT_AVAILABLE:
        raise ImportError("Prefect is required for this task. Install with: pip install prefect")

    from griot_core.workers import JobPayload, LocalWorker

    logger = get_run_logger()
    logger.info(f"Validating contract: {contract_id}")
    logger.info(f"Profile: {profile}, Environment: {environment}")

    # Load Arrow data if path provided
    if arrow_data_path and not arrow_data:
        logger.info(f"Loading Arrow data from: {arrow_data_path}")
        with open(arrow_data_path, "rb") as f:
            arrow_data = {"default": f.read()}

    # Create worker and payload
    worker = LocalWorker()

    if registry_url:
        worker.config.registry_url = registry_url

    payload = JobPayload(
        job_id=f"prefect-{contract_id}",
        contract_id=contract_id,
        profile=profile,
        environment=environment,
        arrow_data=arrow_data,
        options=options or {},
        timeout_seconds=timeout,
    )

    # Run validation
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(worker.execute(payload))
    finally:
        loop.close()

    # Log results
    logger.info(f"Validation completed: {result.status.value}")
    logger.info(f"Is valid: {result.is_valid}")

    if result.validation_result:
        schema_results = result.validation_result.get("schema_results", [])
        for sr in schema_results:
            logger.info(f"Schema {sr['schema_id']}: valid={sr['is_valid']}")
            for cr in sr.get("check_results", []):
                status = cr["status"]
                severity = cr.get("severity", "info")
                logger.info(f"  - {cr['check_name']}: {status} ({severity})")

    # Build result
    validation_result = ValidationResult(
        is_valid=result.is_valid,
        contract_id=contract_id,
        profile=profile,
        duration_ms=result.duration_ms,
        check_results=result.validation_result,
        errors=result.errors if result.errors else None,
    )

    # Handle failure
    if fail_on_invalid and not result.is_valid:
        validation_result.raise_on_invalid()

    return validation_result


class GriotValidationTask:
    """
    Class-based Prefect task for Griot validation.

    Provides a reusable task instance with pre-configured settings.

    Example:
        >>> validate_users = GriotValidationTask(
        ...     contract_id="users-contract",
        ...     profile="data_engineering",
        ... )
        >>>
        >>> @flow
        ... def my_pipeline():
        ...     result = validate_users()
        ...     if result.is_valid:
        ...         ...
    """

    def __init__(
        self,
        contract_id: str,
        profile: str = "default",
        environment: str = "production",
        registry_url: Optional[str] = None,
        fail_on_invalid: bool = False,
        timeout: int = 300,
    ):
        """
        Initialize the validation task.

        Args:
            contract_id: ID of the contract to validate
            profile: Execution profile
            environment: Environment
            registry_url: URL of the Griot registry
            fail_on_invalid: Whether to fail on validation failure
            timeout: Timeout in seconds
        """
        self.contract_id = contract_id
        self.profile = profile
        self.environment = environment
        self.registry_url = registry_url
        self.fail_on_invalid = fail_on_invalid
        self.timeout = timeout

    def __call__(
        self,
        arrow_data: Optional[Dict[str, bytes]] = None,
        arrow_data_path: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> ValidationResult:
        """
        Run the validation task.

        Args:
            arrow_data: Arrow IPC data
            arrow_data_path: Path to Arrow data file
            options: Additional validation options
            **kwargs: Override any init parameters

        Returns:
            ValidationResult
        """
        return griot_validate(
            contract_id=kwargs.get("contract_id", self.contract_id),
            profile=kwargs.get("profile", self.profile),
            environment=kwargs.get("environment", self.environment),
            registry_url=kwargs.get("registry_url", self.registry_url),
            arrow_data=arrow_data,
            arrow_data_path=arrow_data_path,
            options=options,
            timeout=kwargs.get("timeout", self.timeout),
            fail_on_invalid=kwargs.get("fail_on_invalid", self.fail_on_invalid),
        )


@task(
    name="griot_validate_async",
    description="Async validation - returns job ID for polling",
)
def griot_validate_async(
    contract_id: str,
    profile: str = "default",
    registry_url: Optional[str] = None,
    callback_url: Optional[str] = None,
) -> str:
    """
    Submit async validation job to registry.

    For long-running validations, submits job to registry and returns
    job ID for later polling.

    Args:
        contract_id: Contract to validate
        profile: Execution profile
        registry_url: URL of the Griot registry
        callback_url: URL to receive results

    Returns:
        Job ID for polling status
    """
    import json
    import urllib.request
    import uuid

    if not registry_url:
        raise ValueError("registry_url is required for async validation")

    logger = get_run_logger()
    logger.info(f"Submitting async validation for {contract_id}")

    job_id = f"prefect-async-{uuid.uuid4().hex[:8]}"

    # Submit job to registry
    url = f"{registry_url}/api/v1/jobs"
    data = json.dumps(
        {
            "job_id": job_id,
            "contract_id": contract_id,
            "profile": profile,
            "callback_url": callback_url,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=10) as response:
        result = json.loads(response.read().decode())
        return result.get("job_id", job_id)
