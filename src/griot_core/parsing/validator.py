"""
Structure validation for contracts and schemas.

Validates that contracts and schemas have the required fields
and correct structure without validating data content.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List

from griot_core.models import Contract, Schema


class IssueSeverity(str, Enum):
    """Severity levels for validation issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """
    A validation issue found during structure validation.

    Attributes:
        path: JSON path to the issue location (e.g., "$.schema[0].properties[1].name")
        message: Human-readable description of the issue
        severity: How severe the issue is
        code: Machine-readable error code
    """

    path: str
    message: str
    severity: IssueSeverity = IssueSeverity.ERROR
    code: str = ""


@dataclass
class StructureValidationResult:
    """
    Result of structure validation.

    Attributes:
        is_valid: True if no errors (warnings/info allowed)
        issues: List of validation issues found
        error_count: Number of error-level issues
        warning_count: Number of warning-level issues
    """

    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    error_count: int = 0
    warning_count: int = 0

    @classmethod
    def valid(cls) -> "StructureValidationResult":
        """Create a valid result with no issues."""
        return cls(is_valid=True)

    def add_issue(self, issue: ValidationIssue) -> None:
        """Add an issue to the result."""
        self.issues.append(issue)
        if issue.severity == IssueSeverity.ERROR:
            self.error_count += 1
            self.is_valid = False
        elif issue.severity == IssueSeverity.WARNING:
            self.warning_count += 1


def validate_contract_structure(contract: Contract) -> StructureValidationResult:
    """
    Validate the structure of a Contract.

    Checks:
    - Required fields are present
    - Status is valid
    - At least one schema (ref or inline)
    - Schema properties are valid
    - Check definitions are complete

    Args:
        contract: The Contract to validate

    Returns:
        StructureValidationResult with any issues found
    """
    result = StructureValidationResult(is_valid=True)

    # Check required fields
    if not contract.id:
        result.add_issue(
            ValidationIssue(
                path="$.id",
                message="Contract ID is required",
                severity=IssueSeverity.ERROR,
                code="MISSING_ID",
            )
        )

    if not contract.name:
        result.add_issue(
            ValidationIssue(
                path="$.name",
                message="Contract name is required",
                severity=IssueSeverity.ERROR,
                code="MISSING_NAME",
            )
        )

    # Check schemas
    if not contract.schema_refs and not contract.inline_schemas:
        result.add_issue(
            ValidationIssue(
                path="$.schema",
                message="Contract must have at least one schema (ref or inline)",
                severity=IssueSeverity.ERROR,
                code="NO_SCHEMAS",
            )
        )

    # Validate schema refs
    for idx, schema_ref in enumerate(contract.schema_refs):
        if not schema_ref.ref:  # type: ignore[attr-defined]
            result.add_issue(
                ValidationIssue(
                    path=f"$.schema[{idx}].$ref",
                    message="Schema reference URI is required",
                    severity=IssueSeverity.ERROR,
                    code="MISSING_SCHEMA_REF",
                )
            )

    # Validate inline schemas
    for idx, schema in enumerate(contract.inline_schemas):
        schema_result = validate_schema_structure(schema, base_path=f"$.schema[{idx}]")
        for issue in schema_result.issues:
            result.add_issue(issue)

    # Validate checks
    for idx, check in enumerate(contract.checks):
        if not check.name:
            result.add_issue(
                ValidationIssue(
                    path=f"$.checks[{idx}].name",
                    message="Check name is required",
                    severity=IssueSeverity.ERROR,
                    code="MISSING_CHECK_NAME",
                )
            )
        if not check.executor:
            result.add_issue(
                ValidationIssue(
                    path=f"$.checks[{idx}].executor",
                    message="Check executor URI is required",
                    severity=IssueSeverity.ERROR,
                    code="MISSING_CHECK_EXECUTOR",
                )
            )

    # Validate servers
    for idx, server in enumerate(contract.servers):
        if not server.name:
            result.add_issue(
                ValidationIssue(
                    path=f"$.servers[{idx}].name",
                    message="Server name is required",
                    severity=IssueSeverity.WARNING,
                    code="MISSING_SERVER_NAME",
                )
            )
        if not server.type:
            result.add_issue(
                ValidationIssue(
                    path=f"$.servers[{idx}].type",
                    message="Server type is required",
                    severity=IssueSeverity.WARNING,
                    code="MISSING_SERVER_TYPE",
                )
            )

    # Info: Check for recommended fields
    if not contract.owner:
        result.add_issue(
            ValidationIssue(
                path="$.owner",
                message="Consider specifying a contract owner",
                severity=IssueSeverity.INFO,
                code="MISSING_OWNER",
            )
        )

    if not contract.description:
        result.add_issue(
            ValidationIssue(
                path="$.description",
                message="Consider adding a contract description",
                severity=IssueSeverity.INFO,
                code="MISSING_DESCRIPTION",
            )
        )

    return result


def validate_schema_structure(schema: Schema, base_path: str = "$") -> StructureValidationResult:
    """
    Validate the structure of a Schema.

    Checks:
    - Required fields are present
    - Properties are valid
    - Checks are complete

    Args:
        schema: The Schema to validate
        base_path: Base JSON path for issue locations

    Returns:
        StructureValidationResult with any issues found
    """
    result = StructureValidationResult(is_valid=True)

    # Check required fields
    if not schema.id:
        result.add_issue(
            ValidationIssue(
                path=f"{base_path}.id",
                message="Schema ID is required",
                severity=IssueSeverity.ERROR,
                code="MISSING_ID",
            )
        )

    if not schema.name:
        result.add_issue(
            ValidationIssue(
                path=f"{base_path}.name",
                message="Schema name is required",
                severity=IssueSeverity.ERROR,
                code="MISSING_NAME",
            )
        )

    if not schema.physical_name:
        result.add_issue(
            ValidationIssue(
                path=f"{base_path}.physicalName",
                message="Schema physical name is required",
                severity=IssueSeverity.ERROR,
                code="MISSING_PHYSICAL_NAME",
            )
        )

    # Validate properties
    if not schema.properties:
        result.add_issue(
            ValidationIssue(
                path=f"{base_path}.properties",
                message="Schema must have at least one property",
                severity=IssueSeverity.WARNING,
                code="NO_PROPERTIES",
            )
        )

    primary_key_count = 0
    for idx, prop in enumerate(schema.properties):
        prop_path = f"{base_path}.properties[{idx}]"

        if not prop.name:
            result.add_issue(
                ValidationIssue(
                    path=f"{prop_path}.name",
                    message="Property name is required",
                    severity=IssueSeverity.ERROR,
                    code="MISSING_PROPERTY_NAME",
                )
            )

        if not prop.physical_type:
            result.add_issue(
                ValidationIssue(
                    path=f"{prop_path}.physicalType",
                    message="Property physical type is recommended",
                    severity=IssueSeverity.WARNING,
                    code="MISSING_PHYSICAL_TYPE",
                )
            )

        if prop.constraints.primary_key:
            primary_key_count += 1

        # Check PII consistency
        if prop.constraints.pii_type and not prop.constraints.is_pii:
            result.add_issue(
                ValidationIssue(
                    path=f"{prop_path}.constraints",
                    message="Property has pii_type but is_pii is False",
                    severity=IssueSeverity.WARNING,
                    code="INCONSISTENT_PII",
                )
            )

        # Validate relationships
        for rel_idx, rel in enumerate(prop.relationships):
            if not rel.to:
                result.add_issue(
                    ValidationIssue(
                        path=f"{prop_path}.relationships[{rel_idx}].to",
                        message="Relationship target is required",
                        severity=IssueSeverity.ERROR,
                        code="MISSING_RELATIONSHIP_TARGET",
                    )
                )

    # Info: Check for primary key
    if primary_key_count == 0 and schema.properties:
        result.add_issue(
            ValidationIssue(
                path=f"{base_path}.properties",
                message="Consider defining a primary key",
                severity=IssueSeverity.INFO,
                code="NO_PRIMARY_KEY",
            )
        )

    # Validate checks
    for idx, check in enumerate(schema.checks):
        if not check.name:
            result.add_issue(
                ValidationIssue(
                    path=f"{base_path}.checks[{idx}].name",
                    message="Check name is required",
                    severity=IssueSeverity.ERROR,
                    code="MISSING_CHECK_NAME",
                )
            )
        if not check.executor:
            result.add_issue(
                ValidationIssue(
                    path=f"{base_path}.checks[{idx}].executor",
                    message="Check executor URI is required",
                    severity=IssueSeverity.ERROR,
                    code="MISSING_CHECK_EXECUTOR",
                )
            )

    return result
