"""
Griot Core Mock Data Generator

Generate synthetic data conforming to contract constraints.
Uses Python stdlib only (no external dependencies).
"""

from __future__ import annotations

import random
import string
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from griot_core.types import DataType

if TYPE_CHECKING:
    from griot_core.schema import FieldInfo, Schema

__all__ = ["generate_mock_data"]


def _extract_constraints_from_quality(quality_rules: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Extract mock-generation constraints from ODCS quality rules.

    Parses the quality rules list and extracts relevant constraints
    for generating mock data (min, max, min_length, max_length, validValues).

    Args:
        quality_rules: List of ODCS quality rule dictionaries.

    Returns:
        Dictionary of constraints for mock data generation.
    """
    constraints: dict[str, Any] = {}

    for rule in quality_rules:
        if not isinstance(rule, dict):
            continue

        arguments = rule.get("arguments", {})
        if not isinstance(arguments, dict):
            continue

        # Extract numeric range constraints
        if "min" in arguments:
            constraints["min"] = arguments["min"]
        if "max" in arguments:
            constraints["max"] = arguments["max"]

        # Extract string length constraints
        if "minLength" in arguments:
            constraints["min_length"] = arguments["minLength"]
        if "maxLength" in arguments:
            constraints["max_length"] = arguments["maxLength"]
        # Also support snake_case
        if "min_length" in arguments:
            constraints["min_length"] = arguments["min_length"]
        if "max_length" in arguments:
            constraints["max_length"] = arguments["max_length"]

        # Extract valid values for enum-like fields
        if "validValues" in arguments:
            constraints["valid_values"] = arguments["validValues"]

        # Extract pattern for strings
        if "pattern" in arguments:
            constraints["pattern"] = arguments["pattern"]

    return constraints


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


def generate_mock_data(
    schema: type[Schema] | Schema,
    rows: int = 100,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    """
    Generate synthetic data conforming to a schema.

    Args:
        schema: The Schema class or instance defining the contract.
        rows: Number of rows to generate.
        seed: Random seed for reproducibility.

    Returns:
        List of dictionaries containing mock data.
    """
    if seed is not None:
        random.seed(seed)

    fields = _get_schema_fields(schema)

    result: list[dict[str, Any]] = []
    seen_unique: dict[str, set[Any]] = {
        name: set() for name, info in fields.items() if info.unique or info.primary_key
    }

    for row_idx in range(rows):
        row: dict[str, Any] = {}
        for field_name, field_info in fields.items():
            # Handle nullable fields
            if field_info.nullable and random.random() < 0.1:
                row[field_name] = None
                continue

            # Generate value based on field type
            value = _generate_value(field_info, row_idx, seen_unique.get(field_name))

            # Track unique values
            if field_name in seen_unique:
                seen_unique[field_name].add(value)

            row[field_name] = value

        result.append(row)

    return result


def _generate_value(
    field_info: FieldInfo,
    row_idx: int,
    seen: set[Any] | None = None,
) -> Any:
    """Generate a mock value for a field."""
    # Handle default value
    if field_info.has_default and random.random() < 0.3:
        return field_info.default

    # Generate based on type
    dtype = field_info.type
    if dtype == DataType.STRING:
        return _generate_string(field_info, row_idx, seen)
    elif dtype == DataType.INTEGER:
        return _generate_integer(field_info, seen)
    elif dtype == DataType.FLOAT:
        return _generate_float(field_info)
    elif dtype == DataType.BOOLEAN:
        return random.choice([True, False])
    elif dtype == DataType.DATE:
        return _generate_date()
    elif dtype == DataType.DATETIME:
        return _generate_datetime()
    elif dtype == DataType.ARRAY:
        return _generate_array()
    elif dtype == DataType.OBJECT:
        return _generate_object()
    else:
        return _generate_string(field_info, row_idx, seen)


def _generate_string(
    field_info: FieldInfo,
    row_idx: int,
    seen: set[Any] | None = None,
) -> str:
    """Generate a mock string value."""
    # Extract constraints from ODCS quality rules
    quality_rules = field_info.get_dq_checks()
    constraints = _extract_constraints_from_quality(quality_rules)

    # Determine length from constraints
    min_len = constraints.get("min_length", 1)
    max_len = constraints.get("max_length", 50)

    # Check for valid values (enum-like constraint)
    valid_values = constraints.get("valid_values")
    if valid_values and isinstance(valid_values, list) and len(valid_values) > 0:
        return random.choice(valid_values)

    # Generate unique value if needed
    if seen is not None:
        base = f"{field_info.name}_{row_idx}"
        while base in seen:
            base = f"{field_info.name}_{row_idx}_{random.randint(1000, 9999)}"
        return base[:max_len]

    # Generate random string
    length = random.randint(min_len, min(max_len, 20))
    return "".join(random.choices(string.ascii_lowercase, k=length))


def _generate_integer(
    field_info: FieldInfo,
    seen: set[Any] | None = None,
) -> int:
    """Generate a mock integer value."""
    # Extract constraints from ODCS quality rules
    quality_rules = field_info.get_dq_checks()
    constraints = _extract_constraints_from_quality(quality_rules)

    # Determine range from constraints
    min_val = constraints.get("min", 0)
    max_val = constraints.get("max", 1000)

    min_val = int(min_val)
    max_val = int(max_val)

    # Generate unique value if needed
    if seen is not None:
        value = random.randint(min_val, max_val)
        attempts = 0
        while value in seen and attempts < 1000:
            value = random.randint(min_val, max_val)
            attempts += 1
        return value

    return random.randint(min_val, max_val)


def _generate_float(field_info: FieldInfo) -> float:
    """Generate a mock float value."""
    # Extract constraints from ODCS quality rules
    quality_rules = field_info.get_dq_checks()
    constraints = _extract_constraints_from_quality(quality_rules)

    min_val = constraints.get("min", 0.0)
    max_val = constraints.get("max", 1000.0)

    value = random.uniform(float(min_val), float(max_val))
    return round(value, 2)


def _generate_date() -> str:
    """Generate a random date in ISO format."""
    start = datetime(2020, 1, 1)
    end = datetime(2025, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    date = start + timedelta(days=random_days)
    return date.strftime("%Y-%m-%d")


def _generate_datetime() -> str:
    """Generate a random datetime in ISO format."""
    start = datetime(2020, 1, 1)
    end = datetime(2025, 12, 31)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    dt = start + timedelta(seconds=random_seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _generate_array() -> list[Any]:
    """Generate a random array."""
    length = random.randint(0, 5)
    return [random.randint(1, 100) for _ in range(length)]


def _generate_object() -> dict[str, Any]:
    """Generate a random object."""
    return {
        "key": f"value_{random.randint(1, 100)}",
        "count": random.randint(1, 10),
    }
