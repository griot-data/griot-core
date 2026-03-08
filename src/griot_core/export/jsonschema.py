"""
JSON Schema export for Griot schemas.

Exports Griot Schema definitions to JSON Schema format for use
in API validation, form generation, and IDE support.
"""

from __future__ import annotations

from typing import Any, Dict, List

from griot_core.models import LogicalType, Property, Schema

# Mapping from Griot logical types to JSON Schema types
LOGICAL_TYPE_TO_JSON_SCHEMA = {
    LogicalType.STRING: {"type": "string"},
    LogicalType.INTEGER: {"type": "integer"},
    LogicalType.DECIMAL: {"type": "number"},
    LogicalType.BOOLEAN: {"type": "boolean"},
    LogicalType.DATE: {"type": "string", "format": "date"},
    LogicalType.TIMESTAMP: {"type": "string", "format": "date-time"},
    LogicalType.ARRAY: {"type": "array"},
    LogicalType.OBJECT: {"type": "object"},
}


def export_to_jsonschema(
    schema: Schema,
    draft: str = "2020-12",
    include_extensions: bool = True,
    include_descriptions: bool = True,
) -> Dict[str, Any]:
    """
    Export a Schema to JSON Schema format.

    Generates a JSON Schema that can be used for:
    - API request/response validation
    - Form generation (react-jsonschema-form, etc.)
    - IDE autocompletion and validation
    - OpenAPI integration

    Args:
        schema: The Griot Schema to export
        draft: JSON Schema draft version ("2020-12", "draft-07", etc.)
        include_extensions: Include x-* extensions for Griot metadata
        include_descriptions: Include description fields

    Returns:
        JSON Schema as dictionary

    Example:
        >>> from griot_core.models import Schema
        >>> from griot_core.export import export_to_jsonschema
        >>>
        >>> schema = Schema(
        ...     id="sch-001",
        ...     name="employees",
        ...     version="1.0.0",
        ...     status=SchemaStatus.ACTIVE,
        ...     physical_name="hr.employees",
        ...     description="Employee records",
        ...     owner_team="data-team",
        ...     properties=[...]
        ... )
        >>> json_schema = export_to_jsonschema(schema)
    """
    exporter = JSONSchemaExporter(
        draft=draft,
        include_extensions=include_extensions,
        include_descriptions=include_descriptions,
    )
    return exporter.export(schema)


class JSONSchemaExporter:
    """
    Exporter for converting Griot Schemas to JSON Schema format.

    Supports multiple JSON Schema draft versions and optional
    Griot-specific extensions.
    """

    DRAFT_SCHEMAS = {
        "2020-12": "https://json-schema.org/draft/2020-12/schema",
        "2019-09": "https://json-schema.org/draft/2019-09/schema",
        "draft-07": "http://json-schema.org/draft-07/schema#",
        "draft-06": "http://json-schema.org/draft-06/schema#",
    }

    def __init__(
        self,
        draft: str = "2020-12",
        include_extensions: bool = True,
        include_descriptions: bool = True,
    ):
        """
        Initialize the exporter.

        Args:
            draft: JSON Schema draft version
            include_extensions: Include x-* extensions
            include_descriptions: Include descriptions
        """
        self.draft = draft
        self.include_extensions = include_extensions
        self.include_descriptions = include_descriptions

    def export(self, schema: Schema) -> Dict[str, Any]:
        """
        Export a Schema to JSON Schema format.

        Args:
            schema: The Schema to export

        Returns:
            JSON Schema dictionary
        """
        json_schema: Dict[str, Any] = {
            "$schema": self.DRAFT_SCHEMAS.get(self.draft, self.DRAFT_SCHEMAS["2020-12"]),
            "$id": f"urn:griot:schema:{schema.id}",
            "title": schema.name,
            "type": "object",
        }

        if self.include_descriptions and schema.description:
            json_schema["description"] = schema.description

        # Build properties and required list
        properties: Dict[str, Any] = {}
        required: List[str] = []
        primary_keys: List[str] = []

        for prop in schema.properties:
            prop_schema = self._export_property(prop)
            properties[prop.name] = prop_schema

            # Track required properties
            if prop.constraints.required or not prop.constraints.nullable:
                required.append(prop.name)

            # Track primary keys
            if prop.constraints.primary_key:
                primary_keys.append(prop.name)

        json_schema["properties"] = properties

        if required:
            json_schema["required"] = required

        # Add Griot extensions
        if self.include_extensions:
            json_schema["x-griot-schema-id"] = schema.id
            json_schema["x-griot-schema-version"] = schema.version
            json_schema["x-griot-schema-status"] = schema.status.value
            json_schema["x-griot-physical-name"] = schema.physical_name
            json_schema["x-griot-owner-team"] = schema.owner_team

            if primary_keys:
                json_schema["x-griot-primary-key"] = primary_keys

            if schema.tags:
                json_schema["x-griot-tags"] = schema.tags

        return json_schema

    def _export_property(self, prop: Property) -> Dict[str, Any]:
        """
        Export a Property to JSON Schema property format.

        Args:
            prop: The Property to export

        Returns:
            JSON Schema property dictionary
        """
        # Get base type from logical type
        prop_schema: Dict[str, Any] = LOGICAL_TYPE_TO_JSON_SCHEMA.get(
            prop.logical_type, {"type": "string"}
        ).copy()

        # Add description
        if self.include_descriptions and prop.description:
            prop_schema["description"] = prop.description

        # Add constraints
        self._add_constraints(prop_schema, prop)

        # Add Griot extensions
        if self.include_extensions:
            prop_schema["x-griot-property-id"] = prop.id
            prop_schema["x-griot-physical-type"] = prop.physical_type
            prop_schema["x-griot-ordinal-position"] = prop.ordinal_position

            if prop.constraints.primary_key:
                prop_schema["x-griot-primary-key"] = True

            if prop.constraints.unique:
                prop_schema["x-griot-unique"] = True

            if prop.constraints.partitioned:
                prop_schema["x-griot-partitioned"] = True
                if prop.constraints.partition_key_position is not None:
                    prop_schema["x-griot-partition-position"] = (
                        prop.constraints.partition_key_position
                    )

            if prop.constraints.is_pii:
                prop_schema["x-griot-pii"] = True
                if prop.constraints.pii_type:
                    prop_schema["x-griot-pii-type"] = prop.constraints.pii_type.value

            # Add relationships
            if prop.relationships:
                prop_schema["x-griot-relationships"] = [
                    {
                        "to": rel.to,
                        "type": rel.type.value,
                        "cardinality": rel.cardinality.value,
                    }
                    for rel in prop.relationships
                ]

        return prop_schema

    def _add_constraints(self, prop_schema: Dict[str, Any], prop: Property) -> None:
        """
        Add JSON Schema constraints based on property constraints.

        Args:
            prop_schema: The property schema to modify
            prop: The Property with constraints
        """
        # For string types, we could add pattern, minLength, maxLength
        # based on physical type if available

        # Parse physical type for constraints (e.g., VARCHAR(255) -> maxLength: 255)
        physical_type = prop.physical_type.upper()

        if prop.logical_type == LogicalType.STRING:
            # Try to extract length from VARCHAR(n) or CHAR(n)
            if "VARCHAR" in physical_type or "CHAR" in physical_type:
                import re

                match = re.search(r"\((\d+)\)", physical_type)
                if match:
                    prop_schema["maxLength"] = int(match.group(1))

        elif prop.logical_type == LogicalType.DECIMAL:
            # Try to extract precision from DECIMAL(p,s)
            import re

            match = re.search(r"DECIMAL\s*\((\d+)\s*,\s*(\d+)\)", physical_type)
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2))
                # This is approximate - JSON Schema doesn't directly support decimal precision
                max_val = 10 ** (precision - scale)
                prop_schema["maximum"] = max_val
                prop_schema["minimum"] = -max_val


def schema_to_json_schema(schema: Schema, **kwargs) -> Dict[str, Any]:
    """
    Convenience function to export a Schema to JSON Schema.

    Args:
        schema: The Schema to export
        **kwargs: Additional options passed to export_to_jsonschema

    Returns:
        JSON Schema dictionary
    """
    return export_to_jsonschema(schema, **kwargs)
