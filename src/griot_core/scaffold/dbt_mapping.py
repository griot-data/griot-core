"""Mapping between Griot check categories and dbt tests.

Provides constants and helpers for translating Griot executor function
names and check categories into dbt test configurations.
"""

from __future__ import annotations

from dataclasses import dataclass

from griot_core.models.enums import CheckCategory

# ── Function name → CheckCategory ────────────────────────────────────

FUNCTION_TO_CATEGORY: dict[str, CheckCategory] = {
    "check_not_null": CheckCategory.NOT_NULL,
    "check_unique": CheckCategory.UNIQUE,
    "check_allowed_values": CheckCategory.ACCEPTED_VALUES,
    "check_min_max_i64": CheckCategory.RANGE,
    "check_min_max_f64": CheckCategory.RANGE,
    "check_string_length": CheckCategory.STRING_LENGTH,
    "check_regex": CheckCategory.PATTERN,
    "get_row_count": CheckCategory.ROW_COUNT,
}


# ── CheckCategory → dbt test config ──────────────────────────────────


@dataclass
class DbtTestConfig:
    """Configuration for a dbt test mapping."""

    test_name: str
    is_builtin: bool
    param_mapping: dict[str, str]  # griot param → dbt param


CATEGORY_TO_DBT: dict[CheckCategory, DbtTestConfig] = {
    CheckCategory.NOT_NULL: DbtTestConfig(
        test_name="not_null",
        is_builtin=True,
        param_mapping={},
    ),
    CheckCategory.UNIQUE: DbtTestConfig(
        test_name="unique",
        is_builtin=True,
        param_mapping={},
    ),
    CheckCategory.ACCEPTED_VALUES: DbtTestConfig(
        test_name="accepted_values",
        is_builtin=True,
        param_mapping={"allowed_values": "values"},
    ),
    CheckCategory.RELATIONSHIPS: DbtTestConfig(
        test_name="relationships",
        is_builtin=True,
        param_mapping={"to": "to", "field": "field"},
    ),
    CheckCategory.RANGE: DbtTestConfig(
        test_name="griot_value_between",
        is_builtin=False,
        param_mapping={"min_val": "min_value", "max_val": "max_value"},
    ),
    CheckCategory.PATTERN: DbtTestConfig(
        test_name="griot_matches_pattern",
        is_builtin=False,
        param_mapping={"pattern": "pattern"},
    ),
    CheckCategory.STRING_LENGTH: DbtTestConfig(
        test_name="griot_string_length_between",
        is_builtin=False,
        param_mapping={"min_len": "min_length", "max_len": "max_length"},
    ),
    CheckCategory.ROW_COUNT: DbtTestConfig(
        test_name="griot_row_count_between",
        is_builtin=False,
        param_mapping={"min_count": "min_count", "max_count": "max_count"},
    ),
}


def suggest_category(check_function: str) -> CheckCategory:
    """Suggest a CheckCategory from an executor function name.

    Falls back to CUSTOM if no known mapping exists.
    """
    return FUNCTION_TO_CATEGORY.get(check_function, CheckCategory.CUSTOM)


def severity_to_dbt(severity: str) -> str:
    """Map Griot severity to dbt severity.

    dbt uses 'error' and 'warn'; Griot uses 'critical', 'warning', 'info'.
    """
    mapping = {
        "critical": "error",
        "warning": "warn",
        "info": "warn",
    }
    return mapping.get(severity.lower(), "warn")
