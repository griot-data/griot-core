"""
Griot Core Manifest Export

Export contract metadata for AI/LLM consumption.
Supports JSON-LD, Markdown, and LLM context formats.
Uses Python stdlib only (no external dependencies).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from griot_core.types import DataType

if TYPE_CHECKING:
    from griot_core.schema import Schema

__all__ = ["export_manifest"]


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


def _get_primary_key(schema: Any) -> str | None:
    """Get primary key from Schema class or instance."""
    if hasattr(schema, "_schema_primary_key"):
        return schema._schema_primary_key
    return None


def export_manifest(
    schema: type[Schema] | Schema,
    format: str = "json_ld",
) -> str:
    """
    Export schema metadata for AI/LLM consumption.

    Args:
        schema: The Schema class or instance to export.
        format: Output format - 'json_ld', 'markdown', or 'llm_context'.

    Returns:
        String representation in the requested format.
    """
    if format == "json_ld":
        return _export_json_ld(schema)
    elif format == "markdown":
        return _export_markdown(schema)
    elif format == "llm_context":
        return _export_llm_context(schema)
    else:
        raise ValueError(f"Unknown format: {format}. Use 'json_ld', 'markdown', or 'llm_context'")


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


def _export_json_ld(schema: type[Schema] | Schema) -> str:
    """Export as JSON-LD (Linked Data format)."""
    properties: dict[str, Any] = {}
    fields = _get_schema_fields(schema)

    for field_name, field_info in fields.items():
        prop: dict[str, Any] = {
            "@type": _datatype_to_schema_org(field_info.type),
            "description": field_info.description,
        }

        if field_info.nullable:
            prop["nullable"] = True
        if field_info.physical_type:
            prop["physicalType"] = field_info.physical_type

        properties[field_name] = prop

    name = _get_schema_name(schema)
    json_ld = {
        "@context": {
            "@vocab": "https://schema.org/",
            "griot": "https://griot.dev/schema/",
        },
        "@type": "DataContract",
        "@id": f"griot:{name}",
        "name": name,
        "description": getattr(schema, "__doc__", "") or "",
        "properties": properties,
    }

    primary_key = _get_primary_key(schema)
    if primary_key:
        json_ld["identifier"] = primary_key

    return json.dumps(json_ld, indent=2, default=str)


def _export_markdown(schema: type[Schema] | Schema) -> str:
    """Export as Markdown documentation."""
    name = _get_schema_name(schema)
    fields = _get_schema_fields(schema)

    lines = [
        f"# {name}",
        "",
    ]

    doc = getattr(schema, "__doc__", None)
    if doc:
        lines.append(doc.strip())
        lines.append("")

    lines.append("## Fields")
    lines.append("")
    lines.append("| Field | Type | Description | Properties |")
    lines.append("|-------|------|-------------|------------|")

    for field_name, field_info in fields.items():
        type_str = field_info.logical_type
        if field_info.nullable:
            type_str += "?"

        props: list[str] = []
        if field_info.primary_key:
            props.append("PK")
        if field_info.unique:
            props.append("unique")
        if field_info.required:
            props.append("required")
        if field_info.partitioned:
            props.append("partitioned")
        if field_info.critical_data_element:
            props.append("CDE")

        props_str = ", ".join(props) if props else "-"
        lines.append(f"| `{field_name}` | {type_str} | {field_info.description} | {props_str} |")

    lines.append("")

    # Add detailed field information
    lines.append("## Field Details")
    lines.append("")

    for field_name, field_info in fields.items():
        lines.append(f"### {field_name}")
        lines.append("")
        lines.append(f"**Logical Type:** `{field_info.logical_type}`")
        if field_info.physical_type:
            lines.append(f"**Physical Type:** `{field_info.physical_type}`")
        lines.append("")
        lines.append(f"**Description:** {field_info.description}")
        lines.append("")

        if field_info.tags:
            lines.append(f"**Tags:** {', '.join(field_info.tags)}")
            lines.append("")

        if field_info.relationships:
            lines.append("**Relationships:**")
            for rel in field_info.relationships:
                lines.append(f"- {rel.get('type', 'unknown')}: {rel.get('to', '')}")
            lines.append("")

    return "\n".join(lines)


def _export_llm_context(schema: type[Schema] | Schema) -> str:
    """Export as optimized context for LLM consumption."""
    name = _get_schema_name(schema)
    fields = _get_schema_fields(schema)
    primary_key = _get_primary_key(schema)

    lines = [
        f"DATA CONTRACT: {name}",
        "",
    ]

    doc = getattr(schema, "__doc__", None)
    if doc:
        lines.append(f"PURPOSE: {doc.strip()}")
        lines.append("")

    if primary_key:
        lines.append(f"PRIMARY KEY: {primary_key}")
        lines.append("")

    lines.append("FIELDS:")
    lines.append("")

    for field_name, field_info in fields.items():
        # Build a concise field description
        parts = [f"- {field_name} ({field_info.logical_type})"]

        if field_info.nullable:
            parts.append(" [nullable]")
        if field_info.primary_key:
            parts.append(" [PK]")
        if field_info.unique:
            parts.append(" [unique]")
        if field_info.required:
            parts.append(" [required]")

        lines.append("".join(parts))
        lines.append(f"  Description: {field_info.description}")

        # Add quality rules from ODCS specification
        quality_rules = field_info.get_dq_checks()
        if quality_rules:
            rule_strs = []
            for rule in quality_rules:
                metric = rule.get("metric", "unknown")
                # Extract operator (mustBe, mustNotBe, etc.)
                op_str = ""
                for op_name in [
                    "mustBe",
                    "mustNotBe",
                    "mustBeGreaterThan",
                    "mustBeLessThan",
                    "mustBeBetween",
                ]:
                    if op_name in rule:
                        op_str = f" {op_name}={rule[op_name]}"
                        break
                rule_strs.append(f"{metric}{op_str}")
            lines.append(f"  Quality Rules: {', '.join(rule_strs)}")

        lines.append("")

    # Add usage guidance for LLM
    lines.append("VALIDATION RULES:")
    lines.append("- All non-nullable fields are required")
    lines.append("- Type constraints must be strictly followed")
    lines.append("- Check field-level quality rules for additional constraints")
    lines.append("")

    return "\n".join(lines)


def _datatype_to_schema_org(dtype: DataType) -> str:
    """Convert DataType to schema.org type."""
    mapping = {
        DataType.STRING: "Text",
        DataType.INTEGER: "Integer",
        DataType.FLOAT: "Number",
        DataType.BOOLEAN: "Boolean",
        DataType.DATE: "Date",
        DataType.DATETIME: "DateTime",
        DataType.ARRAY: "ItemList",
        DataType.OBJECT: "Thing",
        DataType.ANY: "DataType",
    }
    return mapping.get(dtype, "Text")
