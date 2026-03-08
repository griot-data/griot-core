"""Logical-type to target-specific type mapping.

Maps ``DataType`` values from griot-core to concrete SQL column types
for each supported target warehouse / database, as well as Python type
hints for model code generation.
"""

from __future__ import annotations

from griot_core.types import DataType

# ── SQL type mappings per target ─────────────────────────────────────

TYPE_MAPPINGS: dict[str, dict[DataType, str]] = {
    "databricks": {
        DataType.STRING: "STRING",
        DataType.INTEGER: "BIGINT",
        DataType.NUMBER: "DOUBLE",
        DataType.FLOAT: "DOUBLE",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.ARRAY: "ARRAY<STRING>",
        DataType.OBJECT: "MAP<STRING, STRING>",
        DataType.ANY: "STRING",
    },
    "snowflake": {
        DataType.STRING: "VARCHAR",
        DataType.INTEGER: "NUMBER(38,0)",
        DataType.NUMBER: "NUMBER(38,4)",
        DataType.FLOAT: "FLOAT",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP_NTZ",
        DataType.TIMESTAMP: "TIMESTAMP_NTZ",
        DataType.ARRAY: "VARIANT",
        DataType.OBJECT: "VARIANT",
        DataType.ANY: "VARIANT",
    },
    "bigquery": {
        DataType.STRING: "STRING",
        DataType.INTEGER: "INT64",
        DataType.NUMBER: "NUMERIC",
        DataType.FLOAT: "FLOAT64",
        DataType.BOOLEAN: "BOOL",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.ARRAY: "ARRAY<STRING>",
        DataType.OBJECT: "JSON",
        DataType.ANY: "STRING",
    },
    "postgres": {
        DataType.STRING: "TEXT",
        DataType.INTEGER: "BIGINT",
        DataType.NUMBER: "NUMERIC",
        DataType.FLOAT: "DOUBLE PRECISION",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMPTZ",
        DataType.TIMESTAMP: "TIMESTAMPTZ",
        DataType.ARRAY: "JSONB",
        DataType.OBJECT: "JSONB",
        DataType.ANY: "TEXT",
    },
    "redshift": {
        DataType.STRING: "VARCHAR(65535)",
        DataType.INTEGER: "BIGINT",
        DataType.NUMBER: "NUMERIC(38,4)",
        DataType.FLOAT: "DOUBLE PRECISION",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMPTZ",
        DataType.TIMESTAMP: "TIMESTAMPTZ",
        DataType.ARRAY: "SUPER",
        DataType.OBJECT: "SUPER",
        DataType.ANY: "VARCHAR(65535)",
    },
    "trino": {
        DataType.STRING: "VARCHAR",
        DataType.INTEGER: "BIGINT",
        DataType.NUMBER: "DOUBLE",
        DataType.FLOAT: "DOUBLE",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.ARRAY: "JSON",
        DataType.OBJECT: "JSON",
        DataType.ANY: "VARCHAR",
    },
    "duckdb": {
        DataType.STRING: "VARCHAR",
        DataType.INTEGER: "BIGINT",
        DataType.NUMBER: "DOUBLE",
        DataType.FLOAT: "DOUBLE",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.ARRAY: "JSON",
        DataType.OBJECT: "JSON",
        DataType.ANY: "VARCHAR",
    },
    "mysql": {
        DataType.STRING: "VARCHAR(255)",
        DataType.INTEGER: "BIGINT",
        DataType.NUMBER: "DECIMAL(38,4)",
        DataType.FLOAT: "DOUBLE",
        DataType.BOOLEAN: "TINYINT(1)",
        DataType.DATE: "DATE",
        DataType.DATETIME: "DATETIME(6)",
        DataType.TIMESTAMP: "DATETIME(6)",
        DataType.ARRAY: "JSON",
        DataType.OBJECT: "JSON",
        DataType.ANY: "TEXT",
    },
    "dbt": {
        DataType.STRING: "{{ dbt.type_string() }}",
        DataType.INTEGER: "{{ dbt.type_int() }}",
        DataType.NUMBER: "{{ dbt.type_numeric() }}",
        DataType.FLOAT: "{{ dbt.type_float() }}",
        DataType.BOOLEAN: "{{ dbt.type_boolean() }}",
        DataType.DATE: "DATE",
        DataType.DATETIME: "{{ dbt.type_timestamp() }}",
        DataType.TIMESTAMP: "{{ dbt.type_timestamp() }}",
        DataType.ARRAY: "{{ dbt.type_string() }}",
        DataType.OBJECT: "{{ dbt.type_string() }}",
        DataType.ANY: "{{ dbt.type_string() }}",
    },
    "generic": {
        DataType.STRING: "VARCHAR",
        DataType.INTEGER: "INTEGER",
        DataType.NUMBER: "NUMERIC",
        DataType.FLOAT: "FLOAT",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.DATETIME: "TIMESTAMP",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.ARRAY: "TEXT",
        DataType.OBJECT: "TEXT",
        DataType.ANY: "TEXT",
    },
}

# ── Python type mappings ─────────────────────────────────────────────

PYTHON_TYPE_MAPPINGS: dict[DataType, str] = {
    DataType.STRING: "str",
    DataType.INTEGER: "int",
    DataType.NUMBER: "float",
    DataType.FLOAT: "float",
    DataType.BOOLEAN: "bool",
    DataType.DATE: "datetime.date",
    DataType.DATETIME: "datetime.datetime",
    DataType.TIMESTAMP: "datetime.datetime",
    DataType.ARRAY: "list[Any]",
    DataType.OBJECT: "dict[str, Any]",
    DataType.ANY: "Any",
}


# ── Public helpers ───────────────────────────────────────────────────


def map_logical_type(logical_type: str | DataType, target: str) -> str:
    """Map a logical type to a SQL column type for the given target.

    Args:
        logical_type: DataType enum or its string value.
        target: Target warehouse name (e.g. ``"databricks"``).

    Returns:
        SQL type string.

    Raises:
        ValueError: If the target is not supported.
    """
    target = target.lower()
    if target not in TYPE_MAPPINGS:
        raise ValueError(
            f"Unsupported target '{target}'. Supported: {', '.join(sorted(TYPE_MAPPINGS))}"
        )

    if isinstance(logical_type, str):
        try:
            logical_type = DataType(logical_type)
        except ValueError:
            return TYPE_MAPPINGS[target].get(DataType.ANY, "TEXT")

    return TYPE_MAPPINGS[target].get(logical_type, TYPE_MAPPINGS[target][DataType.ANY])


def map_to_python_type(logical_type: str | DataType) -> str:
    """Map a logical type to a Python type hint string.

    Args:
        logical_type: DataType enum or its string value.

    Returns:
        Python type hint string.
    """
    if isinstance(logical_type, str):
        try:
            logical_type = DataType(logical_type)
        except ValueError:
            return "Any"

    return PYTHON_TYPE_MAPPINGS.get(logical_type, "Any")


def get_supported_targets() -> list[str]:
    """Return list of supported target names."""
    return sorted(TYPE_MAPPINGS.keys())
