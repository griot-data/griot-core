"""
Validation engine for orchestrating check execution.

The ValidationEngine is the main entry point for running validations.
It orchestrates the entire validation process:
1. Resolve contract inheritance
2. Resolve profile and checks
3. Connect to data sources
4. Execute checks via executor runtime
5. Build and return results
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol

from griot_core.connectors.base import DataConnector
from griot_core.executors.registry import ExecutorRegistry
from griot_core.executors.runtime import ExecutorRuntime
from griot_core.models import Contract, Schema
from griot_core.models.enums import Runtime
from griot_core.resolution import ContractResolver

from .profile import ProfileResolver, ResolvedCheck, ResolvedProfile
from .result import (
    CheckExecutionResult,
    CheckStatus,
    SchemaValidationResult,
    ValidationMode,
    ValidationResult,
)
from .types import ValidationOptions


class RegistryClient(Protocol):
    """Protocol for registry client."""

    def fetch_contract(self, contract_id: str) -> Contract:
        """Fetch a contract by ID."""
        ...

    def fetch_schema(self, schema_ref: str) -> Schema:
        """Fetch a schema by reference."""
        ...


class ValidationEngine:
    """
    Main validation engine that orchestrates check execution.

    The engine is responsible for:
    - Resolving contract inheritance
    - Resolving profiles to determine which checks to run
    - Fetching data from connectors
    - Executing checks via executor runtime
    - Building comprehensive validation results

    IMPORTANT: This class does NOT import any DataFrame libraries.
    All data processing happens inside executors (WASM/Container).

    Example:
        >>> engine = ValidationEngine(
        ...     registry=registry_client,
        ...     executor_runtime=ExecutorRuntime(),
        ...     executor_registry=ExecutorRegistry(),
        ... )
        >>> result = await engine.validate(
        ...     contract_id="my-contract",
        ...     profile="data_engineering",
        ... )
        >>> print(f"Valid: {result.is_valid}")
    """

    def __init__(
        self,
        registry: Optional[RegistryClient] = None,
        executor_runtime: Optional[ExecutorRuntime] = None,
        executor_registry: Optional[ExecutorRegistry] = None,
        connector_registry: Optional[Dict[str, DataConnector]] = None,
        contract_resolver: Optional[ContractResolver] = None,
        profile_resolver: Optional[ProfileResolver] = None,
    ):
        """
        Initialize the validation engine.

        Args:
            registry: Client for fetching contracts and schemas
            executor_runtime: Runtime for executing checks
            executor_registry: Registry for fetching executor specs
            connector_registry: Registry of data connectors
            contract_resolver: Resolver for contract inheritance
            profile_resolver: Resolver for execution profiles
        """
        self._registry = registry
        self._executor_runtime = executor_runtime or ExecutorRuntime()
        self._executor_registry = executor_registry or ExecutorRegistry()
        self._connector_registry = connector_registry or {}
        self._contract_resolver = contract_resolver
        self._profile_resolver = profile_resolver or ProfileResolver()

    async def validate(
        self,
        contract: Contract,
        profile: str = "default",
        environment: str = "production",
        options: Optional[ValidationOptions] = None,
        arrow_data: Optional[Dict[str, bytes]] = None,
    ) -> ValidationResult:
        """
        Run validation for a contract.

        Args:
            contract: The contract to validate
            profile: Execution profile name
            environment: Environment (prod, staging, dev)
            options: Validation options
            arrow_data: Pre-fetched Arrow data keyed by schema ID

        Returns:
            ValidationResult with check results
        """
        opts = options or ValidationOptions(profile=profile, environment=environment)
        started_at = datetime.now()
        start_time = time.perf_counter()

        # Create result
        result = ValidationResult(
            is_valid=True,
            contract_id=contract.id,
            contract_version=contract.version,
            profile_used=profile,
            mode=ValidationMode.FULL if not opts.sample_size else ValidationMode.SAMPLE,
            started_at=started_at,
        )

        try:
            # Resolve profile
            resolved_profile = self._profile_resolver.resolve(
                contract,
                profile_name=profile,
                environment=environment,
            )

            # Validate each inline schema
            for schema in contract.inline_schemas:
                # Get Arrow data for this schema
                schema_arrow_data = None
                if arrow_data:
                    schema_arrow_data = arrow_data.get(schema.id)

                schema_result = await self._validate_schema(
                    schema=schema,
                    profile=resolved_profile,
                    arrow_data=schema_arrow_data,
                    options=opts,
                )
                result.add_schema_result(schema_result)

                # Fail fast if configured
                if opts.fail_fast and not schema_result.is_valid:
                    break

        except Exception as e:
            result.errors.append(str(e))
            result.is_valid = False

        # Complete timing
        result.completed_at = datetime.now()
        result.duration_ms = (time.perf_counter() - start_time) * 1000

        return result

    async def validate_with_data(
        self,
        contract: Contract,
        data: Dict[str, bytes],
        profile: str = "default",
        options: Optional[ValidationOptions] = None,
    ) -> ValidationResult:
        """
        Validate with pre-provided Arrow data.

        Args:
            contract: The contract to validate
            data: Arrow IPC data keyed by schema ID
            profile: Execution profile name
            options: Validation options

        Returns:
            ValidationResult
        """
        return await self.validate(
            contract=contract,
            profile=profile,
            options=options,
            arrow_data=data,
        )

    async def _validate_schema(
        self,
        schema: Schema,
        profile: ResolvedProfile,
        arrow_data: Optional[bytes],
        options: ValidationOptions,
    ) -> SchemaValidationResult:
        """Validate a single schema."""
        result = SchemaValidationResult(
            schema_id=schema.id,
            schema_name=schema.name,
            is_valid=True,
        )

        # Filter checks for this schema
        schema_checks = [
            rc for rc in profile.checks if rc.source == schema.id or rc.source == "contract"
        ]

        # Execute checks
        if options.parallel_checks:
            # Run checks in parallel
            tasks = [
                self._execute_check(rc, arrow_data, profile.runtime_preference, options)
                for rc in schema_checks
            ]
            check_results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, check_result in enumerate(check_results):
                if isinstance(check_result, Exception):
                    result.add_result(
                        CheckExecutionResult(
                            check_name=schema_checks[i].check.name,
                            status=CheckStatus.ERROR,
                            severity=schema_checks[i].check.severity,
                            error_message=str(check_result),
                        )
                    )
                else:
                    result.add_result(check_result)  # type: ignore[arg-type]
        else:
            # Run checks sequentially
            for resolved_check in schema_checks:
                check_result = await self._execute_check(
                    resolved_check,
                    arrow_data,
                    profile.runtime_preference,
                    options,
                )
                result.add_result(check_result)

                # Fail fast if configured
                if options.fail_fast and check_result.status == CheckStatus.FAILED:
                    if check_result.is_critical:
                        break

        return result

    async def _execute_check(
        self,
        resolved_check: ResolvedCheck,
        arrow_data: Optional[bytes],
        runtime_preference: List[Runtime],
        options: ValidationOptions,
    ) -> CheckExecutionResult:
        """Execute a single check."""
        check = resolved_check.check
        start_time = time.perf_counter()

        # Handle dry run
        if options.dry_run:
            return CheckExecutionResult(
                check_name=check.name,
                status=CheckStatus.SKIPPED,
                severity=check.severity,
                details={"reason": "dry_run"},
            )

        # Handle missing data
        if arrow_data is None:
            return CheckExecutionResult(
                check_name=check.name,
                status=CheckStatus.SKIPPED,
                severity=check.severity,
                details={"reason": "no_data"},
            )

        try:
            # Get executor spec
            executor_spec = await self._executor_registry.get_executor(check.executor)

            # Execute via runtime
            executor_result = await self._executor_runtime.execute(
                spec=executor_spec,
                check=check,
                arrow_data=arrow_data,
                runtime_preference=runtime_preference,
                timeout=options.timeout_seconds,
            )

            execution_time = (time.perf_counter() - start_time) * 1000
            check_result = executor_result.check_result

            # Build result
            status = CheckStatus.PASSED if check_result.passed else CheckStatus.FAILED
            if check_result.error:
                status = CheckStatus.ERROR

            return CheckExecutionResult(
                check_name=check.name,
                status=status,
                severity=check.severity,
                metric_value=check_result.metric_value,
                threshold=check_result.threshold,
                operator=check_result.operator,
                details=check_result.details,
                samples=check_result.samples[: options.max_samples]
                if options.include_samples
                else [],
                error_message=check_result.error,
                execution_time_ms=execution_time,
                executor_id=executor_result.executor_id,
                executor_version=executor_result.executor_version,
            )

        except Exception as e:
            execution_time = (time.perf_counter() - start_time) * 1000
            return CheckExecutionResult(
                check_name=check.name,
                status=CheckStatus.ERROR,
                severity=check.severity,
                error_message=str(e),
                execution_time_ms=execution_time,
            )

    async def validate_contract_id(
        self,
        contract_id: str,
        profile: str = "default",
        environment: str = "production",
        sample_size: Optional[int] = None,
    ) -> ValidationResult:
        """
        Validate by contract ID (fetches from registry).

        Args:
            contract_id: Contract ID to validate
            profile: Execution profile
            environment: Environment
            sample_size: Optional sample size

        Returns:
            ValidationResult
        """
        if not self._registry:
            return ValidationResult(
                is_valid=False,
                contract_id=contract_id,
                errors=["Registry client not configured"],
            )

        try:
            contract = self._registry.fetch_contract(contract_id)
            return await self.validate(
                contract=contract,
                profile=profile,
                environment=environment,
                options=ValidationOptions(
                    profile=profile,
                    environment=environment,
                    sample_size=sample_size,
                ),
            )
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                contract_id=contract_id,
                errors=[str(e)],
            )

    def get_available_profiles(self, contract: Contract) -> List[str]:
        """Get available profiles for a contract."""
        return self._profile_resolver.list_profiles(contract)

    def get_runtime_capabilities(self) -> Dict[str, Any]:
        """Get information about available runtimes."""
        caps = self._executor_runtime.get_capabilities()
        return {
            "wasm_available": caps.wasm_available,
            "container_available": caps.container_available,
            "container_runtime": caps.container_runtime,
        }
