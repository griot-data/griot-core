"""
Validation types and configuration.

Defines types used by the validation engine for configuration
and context.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from griot_core.models.enums import Runtime


@dataclass
class ProfileConfig:
    """
    Configuration for an execution profile.

    Profiles allow different teams to run different subsets of
    checks with different runtime preferences.

    Attributes:
        name: Profile name (e.g., "data_engineering")
        description: Human-readable description
        checks_include: Checks to include (list or "all")
        checks_exclude: Checks to exclude
        runtime_preference: Preferred runtimes in order
        timeout_override: Override default timeout
        sample_size_override: Override default sample size
    """

    name: str
    description: str = ""
    checks_include: List[str] = field(default_factory=lambda: ["all"])
    checks_exclude: List[str] = field(default_factory=list)
    runtime_preference: List[Runtime] = field(
        default_factory=lambda: [Runtime.WASM, Runtime.CONTAINER]
    )
    timeout_override: Optional[int] = None
    sample_size_override: Optional[int] = None

    def should_run_check(self, check_name: str, check_type: str, check_tags: List[str]) -> bool:
        """
        Determine if a check should run under this profile.

        Args:
            check_name: Name of the check
            check_type: Type of the check (data_quality, privacy, etc.)
            check_tags: Tags on the check

        Returns:
            True if the check should run
        """
        # Check exclusions first
        if check_name in self.checks_exclude:
            return False
        if f"type:{check_type}" in self.checks_exclude:
            return False
        for tag in check_tags:
            if f"tag:{tag}" in self.checks_exclude:
                return False

        # Check inclusions
        if "all" in self.checks_include:
            return True
        if check_name in self.checks_include:
            return True
        if f"type:{check_type}" in self.checks_include:
            return True
        for tag in check_tags:
            if f"tag:{tag}" in self.checks_include:
                return True

        # Check for auto: patterns
        if "auto:constraints" in self.checks_include:
            # Include auto-generated constraint checks
            if check_name.startswith("auto_"):
                return True

        return False


@dataclass
class ValidationOptions:
    """
    Options for a validation run.

    Attributes:
        profile: Profile to use for validation
        environment: Environment to validate in (prod, staging, dev)
        sample_size: Optional sample size for sampling validation
        timeout_seconds: Maximum time for validation
        fail_fast: Stop on first critical failure
        include_samples: Include sample failed rows in results
        max_samples: Maximum number of sample rows to include
        dry_run: If True, don't actually run checks (just validate config)
        parallel_checks: Run checks in parallel where possible
    """

    profile: str = "default"
    environment: str = "production"
    sample_size: Optional[int] = None
    timeout_seconds: int = 300
    fail_fast: bool = False
    include_samples: bool = True
    max_samples: int = 10
    dry_run: bool = False
    parallel_checks: bool = True


@dataclass
class ValidationContext:
    """
    Context for a validation run.

    Contains all the information needed to run validation,
    including the resolved contract, options, and runtime state.

    Attributes:
        contract_id: ID of the contract being validated
        contract_version: Version of the contract
        resolved_contract: The fully resolved contract (inheritance applied)
        options: Validation options
        profile_config: Resolved profile configuration
        environment: Current environment
        metadata: Additional metadata for the run
    """

    contract_id: str
    contract_version: str
    resolved_contract: Dict[str, Any]
    options: ValidationOptions = field(default_factory=ValidationOptions)
    profile_config: Optional[ProfileConfig] = None
    environment: str = "production"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_checks_for_schema(self, schema_id: str) -> List[Dict[str, Any]]:
        """
        Get the checks that should run for a schema.

        Applies profile filtering to determine which checks to run.

        Args:
            schema_id: ID of the schema

        Returns:
            List of check configurations to run
        """
        # Get schema checks from resolved contract
        all_checks = []

        # Get contract-level checks
        contract_checks = self.resolved_contract.get("checks", [])
        all_checks.extend(contract_checks)

        # Get schema-specific checks
        schemas = self.resolved_contract.get("schema", [])
        for schema in schemas:
            if isinstance(schema, dict):
                if schema.get("id") == schema_id:
                    schema_checks = schema.get("checks", [])
                    all_checks.extend(schema_checks)

        # Filter by profile if configured
        if self.profile_config:
            filtered = []
            for check in all_checks:
                check_name = check.get("name", "")
                check_type = check.get("type", "data_quality")
                check_tags = check.get("tags", [])
                if self.profile_config.should_run_check(check_name, check_type, check_tags):
                    filtered.append(check)
            return filtered

        return all_checks
