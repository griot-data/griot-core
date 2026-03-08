"""
Dagster integration for Griot validation.

Provides Dagster resources, ops, and asset decorators for running
Griot validation as part of Dagster pipelines.

Requirements:
    pip install dagster

Example:
    from dagster import job, op
    from griot_core.integrations.dagster import griot_validation_resource

    @op(required_resource_keys={"griot"})
    def validate_sales_data(context):
        result = context.resources.griot.validate(
            contract_id="sales-contract",
            profile="data_engineering",
        )
        if not result.is_valid:
            raise Exception("Validation failed")

    @job(resource_defs={"griot": griot_validation_resource})
    def my_pipeline():
        validate_sales_data()
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

try:
    from dagster import (
        AssetCheckResult,
        AssetCheckSeverity,
        AssetExecutionContext,
        ConfigurableResource,
        OpExecutionContext,
        Output,
        asset,
        op,
    )

    DAGSTER_AVAILABLE = True
except ImportError:
    DAGSTER_AVAILABLE = False

    # Create stubs when Dagster not installed
    class ConfigurableResource:
        pass

    def asset(*args, **kwargs):
        def decorator(func):
            return func

        return decorator if not args else decorator(args[0])

    def op(*args, **kwargs):
        def decorator(func):
            return func

        return decorator if not args else decorator(args[0])

    class Output:
        pass

    class AssetExecutionContext:
        pass

    class OpExecutionContext:
        pass

    class AssetCheckResult:
        pass

    class AssetCheckSeverity:
        pass


@dataclass
class GriotValidationResult:
    """
    Result from Griot validation in Dagster context.

    Attributes:
        is_valid: Whether validation passed
        contract_id: Contract that was validated
        profile: Profile used
        duration_ms: Validation duration
        check_results: Detailed check results
        errors: Error messages if any
    """

    is_valid: bool
    contract_id: str
    profile: str
    duration_ms: Optional[float] = None
    check_results: Optional[Dict[str, Any]] = None
    errors: Optional[list] = None


class GriotResource(ConfigurableResource):
    """
    Dagster resource for Griot validation.

    Provides validation capabilities to ops and assets.

    Configuration:
        registry_url: URL of the Griot registry
        default_profile: Default execution profile
        default_timeout: Default timeout in seconds

    Example:
        >>> griot = GriotResource(
        ...     registry_url="https://registry.example.com",
        ...     default_profile="data_engineering",
        ... )
        >>> result = griot.validate(contract_id="my-contract")
    """

    registry_url: Optional[str] = None
    default_profile: str = "default"
    default_timeout: int = 300

    def validate(
        self,
        contract_id: str,
        profile: Optional[str] = None,
        environment: str = "production",
        arrow_data: Optional[Dict[str, bytes]] = None,
        arrow_data_path: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> GriotValidationResult:
        """
        Run validation against a contract.

        Args:
            contract_id: Contract to validate
            profile: Execution profile (uses default if not specified)
            environment: Environment
            arrow_data: Arrow IPC data
            arrow_data_path: Path to Arrow data file
            options: Additional options
            timeout: Timeout in seconds

        Returns:
            GriotValidationResult
        """
        if not DAGSTER_AVAILABLE:
            raise ImportError(
                "Dagster is required for this resource. Install with: pip install dagster"
            )

        from griot_core.workers import JobPayload, LocalWorker

        profile = profile or self.default_profile
        timeout = timeout or self.default_timeout

        # Load Arrow data if path provided
        if arrow_data_path and not arrow_data:
            with open(arrow_data_path, "rb") as f:
                arrow_data = {"default": f.read()}

        # Create worker
        worker = LocalWorker()
        if self.registry_url:
            worker.config.registry_url = self.registry_url

        payload = JobPayload(
            job_id=f"dagster-{contract_id}",
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

        return GriotValidationResult(
            is_valid=result.is_valid,
            contract_id=contract_id,
            profile=profile,
            duration_ms=result.duration_ms,
            check_results=result.validation_result,
            errors=result.errors if result.errors else None,
        )


# Create a default resource for simple use cases
griot_validation_resource = GriotResource


def griot_validated_asset(
    contract_id: str,
    profile: str = "default",
    fail_on_invalid: bool = True,
    **asset_kwargs: Any,
) -> Callable:
    """
    Decorator for creating a validated Dagster asset.

    Wraps an asset function with automatic validation before execution.

    Args:
        contract_id: Contract to validate against
        profile: Execution profile
        fail_on_invalid: Whether to fail if validation fails
        **asset_kwargs: Additional asset decorator arguments

    Returns:
        Decorated asset function

    Example:
        >>> @griot_validated_asset(
        ...     contract_id="users-contract",
        ...     profile="data_engineering",
        ... )
        ... def users_data(context):
        ...     # This only runs if validation passes
        ...     return load_users()
    """

    def decorator(func: Callable) -> Callable:
        @asset(**asset_kwargs)
        def wrapper(context: AssetExecutionContext, **kwargs: Any) -> Any:
            # Get or create griot resource
            if hasattr(context.resources, "griot"):
                griot = context.resources.griot
            else:
                griot = GriotResource()

            # Run validation
            context.log.info(f"Validating against contract: {contract_id}")
            result = griot.validate(
                contract_id=contract_id,
                profile=profile,
            )

            context.log.info(f"Validation result: valid={result.is_valid}")

            if not result.is_valid and fail_on_invalid:
                errors = ", ".join(result.errors) if result.errors else "Unknown error"
                raise Exception(f"Validation failed for {contract_id}: {errors}")

            # Run the actual asset function
            return func(context, **kwargs)

        return wrapper

    return decorator


def create_validation_op(
    contract_id: str,
    profile: str = "default",
    fail_on_invalid: bool = True,
) -> Callable:
    """
    Create a Dagster op for validation.

    Creates a reusable op that validates against a specific contract.

    Args:
        contract_id: Contract to validate
        profile: Execution profile
        fail_on_invalid: Whether to fail on invalid

    Returns:
        Dagster op function

    Example:
        >>> validate_users = create_validation_op(
        ...     contract_id="users-contract",
        ...     profile="data_engineering",
        ... )
        >>>
        >>> @job
        ... def my_pipeline():
        ...     validate_users()
    """

    @op(
        name=f"validate_{contract_id.replace('-', '_')}",
        required_resource_keys={"griot"},
    )
    def validation_op(context: OpExecutionContext) -> GriotValidationResult:
        result = context.resources.griot.validate(
            contract_id=contract_id,
            profile=profile,
        )

        context.log.info(f"Validation for {contract_id}: valid={result.is_valid}")

        if not result.is_valid:
            if result.errors:
                for error in result.errors:
                    context.log.error(f"  Error: {error}")

            if fail_on_invalid:
                raise Exception(f"Validation failed for {contract_id}")

        return result

    return validation_op


def create_asset_check(
    asset_key: str,
    contract_id: str,
    profile: str = "default",
) -> Callable:
    """
    Create a Dagster asset check for validation.

    Creates an asset check that validates the asset's data against
    a Griot contract.

    Args:
        asset_key: Key of the asset to check
        contract_id: Contract to validate against
        profile: Execution profile

    Returns:
        Asset check function

    Example:
        >>> check_users = create_asset_check(
        ...     asset_key="users",
        ...     contract_id="users-contract",
        ... )
    """
    if not DAGSTER_AVAILABLE:
        raise ImportError("Dagster is required")

    from dagster import asset_check

    @asset_check(asset=asset_key)
    def griot_check(context: AssetExecutionContext) -> AssetCheckResult:
        # Get griot resource
        if hasattr(context.resources, "griot"):
            griot = context.resources.griot
        else:
            griot = GriotResource()

        result = griot.validate(
            contract_id=contract_id,
            profile=profile,
        )

        if result.is_valid:
            return AssetCheckResult(
                passed=True,
                metadata={
                    "contract_id": contract_id,
                    "profile": profile,
                    "duration_ms": result.duration_ms,
                },
            )
        else:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={
                    "contract_id": contract_id,
                    "profile": profile,
                    "errors": result.errors,
                },
            )

    return griot_check
