"""
Griot Core Reports

Report generation for ODCS data contracts.
Uses Python stdlib only (no external dependencies).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from griot_core.schema import Schema

__all__ = [
    "ContractReport",
    "generate_contract_report",
]


def _get_schema_fields(schema: Any) -> dict[str, Any]:
    """Get fields from Schema class or instance."""
    # Check if it's a class with _schema_fields (class-based schema)
    if hasattr(schema, "_schema_fields") and schema._schema_fields:
        return schema._schema_fields
    # Check if it's an instance with a fields property
    if hasattr(schema, "fields"):
        fields_attr = getattr(schema, "fields", None)
        if isinstance(fields_attr, dict):
            return fields_attr
    raise TypeError(f"Expected Schema, got {type(schema).__name__}")


def _get_schema_name(schema: Any) -> str:
    """Get name from Schema class or instance."""
    # For class-based schemas, check _name class variable first
    if hasattr(schema, "_name") and schema._name:
        return schema._name
    # For classes, use __name__
    if isinstance(schema, type):
        return schema.__name__
    # For instances, check instance name attribute
    if hasattr(schema, "name") and isinstance(getattr(schema, "name"), str) and schema.name:
        return schema.name
    # Fall back to class name
    return schema.__class__.__name__ if hasattr(schema, "__class__") else str(schema)


def _get_primary_key(schema: Any) -> str | None:
    """Get primary key from Schema class or instance."""
    if hasattr(schema, "_schema_primary_key"):
        return schema._schema_primary_key
    return None


@dataclass
class ContractReport:
    """
    Report for a data contract.

    Provides statistics about the contract structure and fields.
    """

    contract_name: str
    generated_at: str
    version: str = "1.0"

    # Contract metadata
    contract_id: str | None = None
    contract_version: str | None = None
    contract_status: str | None = None

    # Field statistics
    total_fields: int = 0
    field_types: dict[str, int] = dataclass_field(default_factory=dict)
    nullable_fields: list[str] = dataclass_field(default_factory=list)
    required_fields: list[str] = dataclass_field(default_factory=list)

    # Key information
    primary_key: str | None = None
    unique_fields: list[str] = dataclass_field(default_factory=list)
    partitioned_fields: list[str] = dataclass_field(default_factory=list)

    # Critical data elements
    critical_data_elements: list[str] = dataclass_field(default_factory=list)

    # Documentation quality
    documented_fields: int = 0
    fields_with_quality_rules: list[str] = dataclass_field(default_factory=list)

    # Recommendations
    recommendations: list[str] = dataclass_field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "report_type": "contract",
            "contract_name": self.contract_name,
            "generated_at": self.generated_at,
            "version": self.version,
            "contract_metadata": {
                "id": self.contract_id,
                "version": self.contract_version,
                "status": self.contract_status,
            },
            "summary": {
                "total_fields": self.total_fields,
                "nullable_count": len(self.nullable_fields),
                "required_count": len(self.required_fields),
                "documentation_coverage": (
                    self.documented_fields / self.total_fields * 100 if self.total_fields > 0 else 0
                ),
            },
            "field_types": self.field_types,
            "keys": {
                "primary_key": self.primary_key,
                "unique_fields": self.unique_fields,
                "partitioned_fields": self.partitioned_fields,
            },
            "critical_data_elements": self.critical_data_elements,
            "quality": {
                "fields_with_rules": self.fields_with_quality_rules,
            },
            "recommendations": self.recommendations,
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert report to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def to_markdown(self) -> str:
        """Convert report to Markdown format."""
        lines = [
            f"# Contract Report: {self.contract_name}",
            "",
            f"*Generated: {self.generated_at}*",
            "",
            "## Contract Metadata",
            "",
            "| Property | Value |",
            "|----------|-------|",
            f"| ID | {self.contract_id or 'Not set'} |",
            f"| Version | {self.contract_version or 'Not set'} |",
            f"| Status | {self.contract_status or 'Not set'} |",
            "",
            "## Summary",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Total Fields | {self.total_fields} |",
            f"| Required Fields | {len(self.required_fields)} |",
            f"| Nullable Fields | {len(self.nullable_fields)} |",
            f"| Documentation Coverage | {self.documented_fields}/{self.total_fields} |",
            "",
            "## Field Types",
            "",
        ]

        for ftype, count in sorted(self.field_types.items()):
            lines.append(f"- **{ftype}**: {count}")

        lines.extend(
            [
                "",
                "## Keys",
                "",
                f"- **Primary Key**: {self.primary_key or 'Not defined'}",
                f"- **Unique Fields**: {', '.join(self.unique_fields) or 'None'}",
                f"- **Partitioned Fields**: {', '.join(self.partitioned_fields) or 'None'}",
                "",
            ]
        )

        if self.critical_data_elements:
            lines.extend(
                [
                    "## Critical Data Elements",
                    "",
                ]
            )
            for cde in self.critical_data_elements:
                lines.append(f"- {cde}")
            lines.append("")

        if self.recommendations:
            lines.extend(
                [
                    "## Recommendations",
                    "",
                ]
            )
            for rec in self.recommendations:
                lines.append(f"- {rec}")
            lines.append("")

        return "\n".join(lines)


def generate_contract_report(schema: type[Schema] | Schema) -> ContractReport:
    """
    Generate a report for a Schema.

    Args:
        schema: The Schema class or instance to analyze.

    Returns:
        ContractReport with contract statistics.
    """
    fields = _get_schema_fields(schema)
    name = _get_schema_name(schema)

    report = ContractReport(
        contract_name=name,
        generated_at=datetime.now().isoformat(),
        total_fields=len(fields),
        primary_key=_get_primary_key(schema),
        contract_id=getattr(schema, "id", None) or getattr(schema, "_id", None),
        contract_version=getattr(schema, "version", None),
        contract_status=None,  # Schema doesn't have status
    )

    # Analyze each field
    for field_name, field_info in fields.items():
        # Field type distribution
        type_name = field_info.logical_type
        report.field_types[type_name] = report.field_types.get(type_name, 0) + 1

        # Nullable vs required
        if field_info.nullable:
            report.nullable_fields.append(field_name)
        if field_info.required:
            report.required_fields.append(field_name)

        # Unique fields
        if field_info.unique:
            report.unique_fields.append(field_name)

        # Partitioned fields
        if field_info.partitioned:
            report.partitioned_fields.append(field_name)

        # Critical data elements
        if field_info.critical_data_element:
            report.critical_data_elements.append(field_name)

        # Documentation quality
        if field_info.description and len(field_info.description) > 10:
            report.documented_fields += 1

        # Quality rules
        if field_info.quality:
            report.fields_with_quality_rules.append(field_name)

    # Generate recommendations
    if report.primary_key is None:
        report.recommendations.append(
            "Consider defining a primary key for unique row identification"
        )

    undocumented = report.total_fields - report.documented_fields
    if undocumented > 0:
        report.recommendations.append(f"Add descriptions to {undocumented} undocumented fields")

    if not report.fields_with_quality_rules and report.total_fields > 0:
        report.recommendations.append("Consider adding quality rules to validate data integrity")

    return report
