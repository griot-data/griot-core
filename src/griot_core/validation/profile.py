"""
Profile resolver for validation execution.

Resolves execution profiles to determine which checks to run
and with what configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from griot_core.models import Check, Contract, Schema
from griot_core.models.enums import CheckType, Runtime, Severity


@dataclass
class ResolvedCheck:
    """
    A check resolved for execution.

    Contains all information needed to execute the check.
    """

    check: Check
    source: str  # "contract" or schema ID
    auto_generated: bool = False
    priority: int = 0  # Lower is higher priority


@dataclass
class ResolvedProfile:
    """
    A resolved execution profile.

    Contains the checks to run and runtime preferences.
    """

    name: str
    description: str
    checks: List[ResolvedCheck]
    runtime_preference: List[Runtime]
    timeout_seconds: int = 300
    sample_size: Optional[int] = None
    fail_fast: bool = False

    def get_checks_by_type(self, check_type: CheckType) -> List[ResolvedCheck]:
        """Get checks of a specific type."""
        return [c for c in self.checks if c.check.type == check_type]

    def get_critical_checks(self) -> List[ResolvedCheck]:
        """Get all critical checks."""
        return [c for c in self.checks if c.check.severity == Severity.CRITICAL]


class ProfileResolver:
    """
    Resolves execution profiles from contracts.

    Determines which checks to run based on the profile configuration,
    including auto-generated checks from property constraints.

    Example:
        >>> resolver = ProfileResolver()
        >>> profile = resolver.resolve(contract, "data_engineering")
        >>> for check in profile.checks:
        ...     print(f"{check.check.name}: {check.check.executor}")
    """

    # Default profiles if none specified in contract
    DEFAULT_PROFILES = {
        "default": {
            "description": "Default profile - runs all checks",
            "checks_include": ["all"],
            "checks_exclude": [],
            "runtime_preference": ["wasm", "container"],
        },
        "data_engineering": {
            "description": "Full validation for data pipelines",
            "checks_include": ["all"],
            "checks_exclude": [],
            "runtime_preference": ["container"],
        },
        "software_engineering": {
            "description": "Schema validation for APIs",
            "checks_include": ["auto:constraints"],
            "checks_exclude": [],
            "runtime_preference": ["wasm"],
        },
        "data_science": {
            "description": "Distribution and drift analysis",
            "checks_include": ["type:data_quality"],
            "checks_exclude": ["type:privacy"],
            "runtime_preference": ["container"],
        },
        "privacy_audit": {
            "description": "Privacy and compliance validation",
            "checks_include": ["type:privacy", "auto:pii_masking"],
            "checks_exclude": [],
            "runtime_preference": ["container"],
        },
        "quick": {
            "description": "Quick validation - critical checks only",
            "checks_include": ["severity:critical"],
            "checks_exclude": [],
            "runtime_preference": ["wasm"],
        },
    }

    def __init__(self, auto_checks_enabled: bool = True):
        """
        Initialize the profile resolver.

        Args:
            auto_checks_enabled: Whether to generate auto checks from constraints
        """
        self._auto_checks_enabled = auto_checks_enabled

    def resolve(
        self,
        contract: Contract,
        profile_name: str = "default",
        environment: str = "production",
    ) -> ResolvedProfile:
        """
        Resolve a profile for a contract.

        Args:
            contract: The contract to resolve profile for
            profile_name: Name of the profile to use
            environment: Environment (for conditional checks)

        Returns:
            ResolvedProfile with checks to run
        """
        # Get profile configuration
        profile_config = self._get_profile_config(contract, profile_name)

        # Collect all checks
        all_checks: List[ResolvedCheck] = []

        # Add contract-level checks
        for check in contract.checks:
            if self._should_include_check(check, profile_config, environment):
                all_checks.append(
                    ResolvedCheck(
                        check=check,
                        source="contract",
                        auto_generated=False,
                    )
                )

        # Add schema-level checks
        for schema in contract.inline_schemas:
            for check in schema.checks:
                if self._should_include_check(check, profile_config, environment):
                    all_checks.append(
                        ResolvedCheck(
                            check=check,
                            source=schema.id,
                            auto_generated=False,
                        )
                    )

            # Generate auto checks from constraints
            if self._auto_checks_enabled and self._should_include_auto_checks(profile_config):
                auto_checks = self._generate_auto_checks(schema, profile_config)
                all_checks.extend(auto_checks)

        # Resolve runtime preference
        runtime_pref = self._resolve_runtime_preference(profile_config)

        return ResolvedProfile(
            name=profile_name,
            description=profile_config.get("description", ""),
            checks=all_checks,
            runtime_preference=runtime_pref,
            timeout_seconds=profile_config.get("timeout_seconds", 300),
            sample_size=profile_config.get("sample_size"),
            fail_fast=profile_config.get("fail_fast", False),
        )

    def _get_profile_config(self, contract: Contract, profile_name: str) -> Dict[str, Any]:
        """Get profile configuration from contract or defaults."""
        # Check contract executors config
        if contract.executors and contract.executors.profiles:
            if profile_name in contract.executors.profiles:
                profile = contract.executors.profiles[profile_name]
                return {
                    "description": profile.description,
                    "checks_include": profile.checks_include,
                    "checks_exclude": profile.checks_exclude,
                    "runtime_preference": profile.runtime_preference,
                }

        # Fall back to defaults
        if profile_name in self.DEFAULT_PROFILES:
            return self.DEFAULT_PROFILES[profile_name].copy()

        # Unknown profile - use default
        return self.DEFAULT_PROFILES["default"].copy()

    def _should_include_check(
        self, check: Check, profile_config: Dict[str, Any], environment: str
    ) -> bool:
        """Determine if a check should be included based on profile and conditions."""
        includes = profile_config.get("checks_include", ["all"])
        excludes = profile_config.get("checks_exclude", [])

        # Check exclusions first
        if check.name in excludes:
            return False
        if f"type:{check.type.value}" in excludes:
            return False
        for tag in check.tags:
            if f"tag:{tag}" in excludes:
                return False
        if f"severity:{check.severity.value}" in excludes:
            return False

        # Check environment condition
        if check.when and check.when.environment:
            if environment not in check.when.environment:
                return False

        # Check inclusions
        if "all" in includes:
            return True
        if check.name in includes:
            return True
        if f"type:{check.type.value}" in includes:
            return True
        for tag in check.tags:
            if f"tag:{tag}" in includes:
                return True
        if f"severity:{check.severity.value}" in includes:
            return True

        return False

    def _should_include_auto_checks(self, profile_config: Dict[str, Any]) -> bool:
        """Check if auto checks should be included."""
        includes = profile_config.get("checks_include", [])
        return (
            "all" in includes
            or "auto:constraints" in includes
            or "auto:pii_masking" in includes
            or "auto:nullable" in includes
            or "auto:unique" in includes
        )

    def _generate_auto_checks(
        self, schema: Schema, profile_config: Dict[str, Any]
    ) -> List[ResolvedCheck]:
        """Generate auto checks from schema property constraints."""
        auto_checks: List[ResolvedCheck] = []
        includes = set(profile_config.get("checks_include", []))

        for prop in schema.properties:
            constraints = prop.constraints

            # Nullable check (required or not nullable)
            if constraints.required or not constraints.nullable:
                if (
                    "all" in includes
                    or "auto:constraints" in includes
                    or "auto:nullable" in includes
                ):
                    auto_checks.append(
                        self._create_auto_check(
                            schema_id=schema.id,
                            check_name=f"auto_null_check_{prop.name}",
                            check_type=CheckType.DATA_QUALITY,
                            executor="registry://executors/null-check@1.0",
                            parameters={"column": prop.name, "threshold": 0, "operator": "eq"},
                            description=f"Auto-generated: {prop.name} must not be null",
                        )
                    )

            # Unique check
            if constraints.unique:
                if "all" in includes or "auto:constraints" in includes or "auto:unique" in includes:
                    auto_checks.append(
                        self._create_auto_check(
                            schema_id=schema.id,
                            check_name=f"auto_unique_check_{prop.name}",
                            check_type=CheckType.DATA_QUALITY,
                            executor="registry://executors/unique-check@1.0",
                            parameters={"column": prop.name, "threshold": 0},
                            description=f"Auto-generated: {prop.name} must be unique",
                        )
                    )

            # Primary key check (combines null + unique)
            if constraints.primary_key:
                if "all" in includes or "auto:constraints" in includes:
                    auto_checks.append(
                        self._create_auto_check(
                            schema_id=schema.id,
                            check_name=f"auto_pk_null_{prop.name}",
                            check_type=CheckType.DATA_QUALITY,
                            executor="registry://executors/null-check@1.0",
                            parameters={"column": prop.name, "threshold": 0, "operator": "eq"},
                            description=f"Auto-generated: Primary key {prop.name} must not be null",
                            severity=Severity.CRITICAL,
                        )
                    )
                    auto_checks.append(
                        self._create_auto_check(
                            schema_id=schema.id,
                            check_name=f"auto_pk_unique_{prop.name}",
                            check_type=CheckType.DATA_QUALITY,
                            executor="registry://executors/unique-check@1.0",
                            parameters={"column": prop.name, "threshold": 0},
                            description=f"Auto-generated: Primary key {prop.name} must be unique",
                            severity=Severity.CRITICAL,
                        )
                    )

            # PII masking check
            if constraints.is_pii:
                if "all" in includes or "auto:pii_masking" in includes:
                    pii_type = constraints.pii_type.value if constraints.pii_type else "unknown"
                    auto_checks.append(
                        self._create_auto_check(
                            schema_id=schema.id,
                            check_name=f"auto_pii_masking_{prop.name}",
                            check_type=CheckType.PRIVACY,
                            executor="registry://executors/masking-check@1.0",
                            parameters={"column": prop.name, "pii_type": pii_type},
                            description=f"Auto-generated: PII field {prop.name} should be masked",
                            severity=Severity.WARNING,
                        )
                    )

        return auto_checks

    def _create_auto_check(
        self,
        schema_id: str,
        check_name: str,
        check_type: CheckType,
        executor: str,
        parameters: Dict[str, Any],
        description: str,
        severity: Severity = Severity.CRITICAL,
    ) -> ResolvedCheck:
        """Create an auto-generated check."""
        check = Check(
            name=check_name,
            description=description,
            type=check_type,
            executor=executor,
            parameters=parameters,
            severity=severity,
            tags=["auto-generated"],
        )
        return ResolvedCheck(
            check=check,
            source=schema_id,
            auto_generated=True,
        )

    def _resolve_runtime_preference(self, profile_config: Dict[str, Any]) -> List[Runtime]:
        """Resolve runtime preference from profile config."""
        prefs = profile_config.get("runtime_preference", ["wasm", "container"])
        result = []
        for pref in prefs:
            if isinstance(pref, Runtime):
                result.append(pref)
            elif isinstance(pref, str):
                try:
                    result.append(Runtime(pref))
                except ValueError:
                    pass
        return result or [Runtime.WASM, Runtime.CONTAINER]

    def list_profiles(self, contract: Optional[Contract] = None) -> List[str]:
        """List available profiles."""
        profiles = set(self.DEFAULT_PROFILES.keys())
        if contract and contract.executors and contract.executors.profiles:
            profiles.update(contract.executors.profiles.keys())
        return sorted(profiles)
