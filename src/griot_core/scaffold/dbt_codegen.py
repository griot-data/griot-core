"""dbt artifact generator for Griot data contracts.

Generates ``_sources.yml``, ``_models.yml``, staging SQL stubs, and
custom generic test macros from a Griot contract's schemas, field
constraints, and quality checks.
"""

from __future__ import annotations

import json
import re
from typing import Any

from griot_core.models.enums import CheckCategory
from griot_core.scaffold.codegen import to_snake_case
from griot_core.scaffold.dbt_mapping import (
    CATEGORY_TO_DBT,
    severity_to_dbt,
    suggest_category,
)
from griot_core.scaffold.type_mapping import map_logical_type

# ── Helpers ──────────────────────────────────────────────────────────


def _yaml_indent(text: str, spaces: int) -> str:
    """Indent every line of *text* by *spaces* spaces."""
    prefix = " " * spaces
    return "\n".join(prefix + line if line.strip() else line for line in text.splitlines())


def _quote(value: str) -> str:
    """YAML-safe single-quote a string."""
    return f"'{value}'"


def _parse_max_age(max_age: str) -> tuple[int, str]:
    """Parse an SLA max_age string like '24h' into (count, period).

    Returns (24, 'hour') for '24h', (7, 'day') for '7d', etc.
    """
    match = re.match(r"(\d+)\s*([hHdDmM])", str(max_age))
    if not match:
        return 24, "hour"
    count = int(match.group(1))
    unit = match.group(2).lower()
    period_map = {"h": "hour", "d": "day", "m": "minute"}
    return count, period_map.get(unit, "hour")


def _find_timestamp_field(fields_dict: dict[str, Any]) -> str | None:
    """Best-guess a loaded_at timestamp field from schema fields."""
    candidates = ["updated_at", "modified_at", "loaded_at", "created_at", "timestamp"]
    for name in candidates:
        if name in fields_dict:
            return name
    for name, info in fields_dict.items():
        lt = getattr(info, "logical_type", "")
        if lt in ("timestamp", "datetime"):
            return name
    return None


# ── Generator ────────────────────────────────────────────────────────


class DbtArtifactGenerator:
    """Generate dbt project artifacts from a Griot contract.

    Produces a ``dict[str, str]`` mapping relative file paths to their
    rendered content, suitable for writing to disk or displaying in a
    dry-run preview.
    """

    def __init__(self, contract: Any, *, database: str | None = None, target: str = "dbt") -> None:
        self.contract = contract
        self.database = database
        self.target = target
        self._contract_snake = to_snake_case(contract.id)
        # Track which custom macros are needed
        self._needed_macros: set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> dict[str, str]:
        """Generate all dbt artifacts and return as path→content dict."""
        rendered: dict[str, str] = {}
        schemas = self.contract.schemas
        if not schemas:
            return rendered

        base = f"models/contracts/{self._contract_snake}"

        # Collect per-schema data
        sources_tables: list[dict[str, Any]] = []
        models_entries: list[dict[str, Any]] = []

        for schema in schemas:
            physical = schema.physical_name or to_snake_case(schema.name)
            leaf = physical.rsplit(".", 1)[-1]

            # Build column tests (deduped) from property flags + quality checks
            column_tests = self._collect_column_tests(schema)

            sources_tables.append(
                {
                    "name": leaf,
                    "description": getattr(schema, "description", ""),
                    "tags": getattr(schema, "tags", []),
                    "columns": column_tests,
                }
            )

            models_entries.append(
                {
                    "name": f"stg_{leaf}",
                    "description": f"Staging model for {leaf}",
                    "columns": column_tests,
                }
            )

            # Staging SQL
            rendered[f"{base}/stg_{leaf}.sql"] = self._render_staging_sql(
                schema,
                leaf,
            )

        # _sources.yml
        rendered[f"{base}/_sources.yml"] = self._render_sources_yml(
            sources_tables,
        )

        # _models.yml
        rendered[f"{base}/_models.yml"] = self._render_models_yml(
            models_entries,
        )

        # Custom generic test macros (only if needed)
        macro_dir = "tests/generic/griot"
        for macro_name in sorted(self._needed_macros):
            content = _CUSTOM_MACROS.get(macro_name)
            if content:
                rendered[f"{macro_dir}/{macro_name}.sql"] = content

        # Helper macro for contract meta tags
        rendered["macros/griot/generate_contract_meta.sql"] = self._render_meta_macro()

        return rendered

    # ------------------------------------------------------------------
    # Test collection with deduplication
    # ------------------------------------------------------------------

    def _collect_column_tests(self, schema: Any) -> dict[str, list[Any]]:
        """Build ``column_name -> [test_config, ...]`` from field flags and checks.

        Returns an ordered dict preserving field order.
        """
        fields_dict = schema.fields
        column_tests: dict[str, list[Any]] = {}

        # Ensure every field has an entry (preserves order)
        for name in fields_dict:
            column_tests[name] = []

        # A set to track (column, test_name) for deduplication
        seen: set[tuple[str, str]] = set()

        def _add_test(col: str, test: Any) -> None:
            test_key = test if isinstance(test, str) else list(test.keys())[0]
            key = (col, test_key)
            if key not in seen:
                seen.add(key)
                column_tests.setdefault(col, []).append(test)

        # 1. Property flags (structural truth from FieldInfo)
        for name, info in fields_dict.items():
            if not info.nullable or info.required or info.primary_key:
                _add_test(name, "not_null")
            if info.unique or info.primary_key:
                _add_test(name, "unique")
            # Foreign key relationships
            for rel in info.relationships or []:
                if rel.get("type") == "foreignKey":
                    to_schema = rel.get("toSchema", "")
                    to_col = rel.get("to", "")
                    if to_schema and to_col:
                        ref_source = to_snake_case(to_schema)
                        _add_test(
                            name,
                            {
                                "relationships": {
                                    "to": f"source('{self._contract_snake}', '{ref_source}')",
                                    "field": to_col,
                                },
                            },
                        )

        # 2. Schema-level quality checks
        for check_dict in schema.quality or []:
            self._process_quality_check(check_dict, column_tests, seen, _add_test)

        return column_tests

    def _process_quality_check(
        self,
        check: dict[str, Any],
        column_tests: dict[str, list[Any]],
        seen: set[tuple[str, str]],
        add_fn: Any,
    ) -> None:
        """Process a single quality check dict into dbt test configs."""
        # Determine category
        category_str = check.get("category")
        check_fn = check.get("checkFunction", check.get("check_function", ""))

        if category_str:
            try:
                cat = CheckCategory(category_str)
            except ValueError:
                cat = CheckCategory.CUSTOM
        elif check_fn:
            cat = suggest_category(check_fn)
        else:
            cat = CheckCategory.CUSTOM

        if cat == CheckCategory.CUSTOM:
            return  # Can't map custom checks to dbt tests

        dbt_config = CATEGORY_TO_DBT.get(cat)
        if dbt_config is None:
            return

        # Get severity
        severity = check.get("severity", "warning")
        dbt_severity = severity_to_dbt(severity)

        # Get columns this check targets
        columns = check.get("columns", [])
        # Arguments can be under "arguments" or "parameters"
        args = check.get("arguments", check.get("parameters", {})) or {}

        # Map parameters
        mapped_params: dict[str, Any] = {}
        for griot_key, dbt_key in dbt_config.param_mapping.items():
            if griot_key in args:
                val = args[griot_key]
                # Parse JSON arrays for accepted_values
                if griot_key == "allowed_values" and isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        val = [val]
                mapped_params[dbt_key] = val

        # Track custom macros
        if not dbt_config.is_builtin:
            self._needed_macros.add(dbt_config.test_name)

        # Table-level tests (row_count) — no column
        if cat == CheckCategory.ROW_COUNT:
            # Row count is table-level; we attach to a special __table__ key
            test_entry: dict[str, Any] = dict(mapped_params)
            if dbt_severity != "error":
                test_entry["severity"] = dbt_severity
            add_fn("__table__", {dbt_config.test_name: test_entry})
            return

        # Column-level tests
        if not columns:
            return  # No columns specified — can't apply

        for col in columns:
            if dbt_config.is_builtin and not mapped_params:
                # Simple built-in test (not_null, unique)
                test_entry_simple = dbt_config.test_name
                if dbt_severity != "error":
                    # Need dict form for severity
                    add_fn(col, {dbt_config.test_name: {"severity": dbt_severity}})
                else:
                    add_fn(col, test_entry_simple)
            else:
                # Parameterized test
                test_entry_param: dict[str, Any] = dict(mapped_params)
                if dbt_severity != "error":
                    test_entry_param["severity"] = dbt_severity
                add_fn(col, {dbt_config.test_name: test_entry_param})

    # ------------------------------------------------------------------
    # YAML renderers (hand-built for control over formatting)
    # ------------------------------------------------------------------

    def _render_sources_yml(self, tables: list[dict[str, Any]]) -> str:
        """Render ``_sources.yml``."""
        c = self.contract
        lines = [
            "version: 2",
            "",
            "sources:",
            f"  - name: {self._contract_snake}",
            f'    description: "Auto-generated by Griot from contract {c.id} v{c.version}"',
        ]

        # Meta block
        lines.append("    meta:")
        lines.append(f'      griot_contract_id: "{c.id}"')
        lines.append(f'      griot_contract_version: "{c.version}"')
        governance = getattr(c, "governance", None) or {}
        if isinstance(governance, dict):
            producer = governance.get("data_producer", {})
            consumer = governance.get("data_consumer", {})
            if producer:
                lines.append(f'      griot_data_producer: "{producer.get("name", "")}"')
            if consumer:
                lines.append(f'      griot_data_consumer: "{consumer.get("name", "")}"')

        # Freshness from SLA
        sla = getattr(c, "sla", None)
        if sla and isinstance(sla, dict):
            freshness = sla.get("freshness", {})
            max_age = freshness.get("maxAge", freshness.get("max_age", ""))
            if max_age:
                warn_count, warn_period = _parse_max_age(max_age)
                err_count = warn_count * 2
                lines.append("    freshness:")
                lines.append(f"      warn_after: {{count: {warn_count}, period: {warn_period}}}")
                lines.append(f"      error_after: {{count: {err_count}, period: {warn_period}}}")

        # Tables
        lines.append("    tables:")
        schemas = self.contract.schemas or []
        for idx, table in enumerate(tables):
            lines.append(f"      - name: {table['name']}")
            if table.get("description"):
                lines.append(f'        description: "{table["description"]}"')
            if table.get("tags"):
                tag_list = ", ".join(table["tags"])
                lines.append(f"        tags: [{tag_list}]")

            # Per-table loaded_at_field (only if freshness SLA is configured)
            if (
                sla
                and isinstance(sla, dict)
                and sla.get("freshness", {}).get(
                    "maxAge", sla.get("freshness", {}).get("max_age", "")
                )
            ):
                if idx < len(schemas):
                    ts_field = _find_timestamp_field(schemas[idx].fields)
                    if ts_field:
                        lines.append(f"        loaded_at_field: {ts_field}")

            # Columns with tests
            col_tests = table.get("columns", {})
            has_columns = any(
                tests for col, tests in col_tests.items() if col != "__table__" and tests
            )
            if has_columns:
                lines.append("        columns:")
                for col_name, tests in col_tests.items():
                    if col_name == "__table__" or not tests:
                        continue
                    lines.append(f"          - name: {col_name}")
                    lines.append("            tests:")
                    for test in tests:
                        if isinstance(test, str):
                            lines.append(f"              - {test}")
                        elif isinstance(test, dict):
                            for test_name, params in test.items():
                                if isinstance(params, dict) and params:
                                    lines.append(f"              - {test_name}:")
                                    for pk, pv in params.items():
                                        lines.append(
                                            f"                  {pk}: {self._yaml_value(pv)}"
                                        )
                                else:
                                    lines.append(f"              - {test_name}")

        lines.append("")
        return "\n".join(lines)

    def _render_models_yml(self, models: list[dict[str, Any]]) -> str:
        """Render ``_models.yml``."""
        lines = [
            "version: 2",
            "",
            "models:",
        ]

        for model in models:
            lines.append(f"  - name: {model['name']}")
            lines.append(f'    description: "{model["description"]}"')

            col_tests = model.get("columns", {})
            has_columns = any(
                tests for col, tests in col_tests.items() if col != "__table__" and tests
            )
            if has_columns:
                lines.append("    columns:")
                for col_name, tests in col_tests.items():
                    if col_name == "__table__" or not tests:
                        continue
                    lines.append(f"      - name: {col_name}")
                    lines.append("        tests:")
                    for test in tests:
                        if isinstance(test, str):
                            lines.append(f"          - {test}")
                        elif isinstance(test, dict):
                            for test_name, params in test.items():
                                if isinstance(params, dict) and params:
                                    lines.append(f"          - {test_name}:")
                                    for pk, pv in params.items():
                                        lines.append(f"              {pk}: {self._yaml_value(pv)}")
                                else:
                                    lines.append(f"          - {test_name}")

        lines.append("")
        return "\n".join(lines)

    def _render_staging_sql(self, schema: Any, leaf: str) -> str:
        """Render ``stg_{table}.sql`` stub."""
        c = self.contract
        fields_dict = schema.fields

        lines = [
            f"-- Auto-generated by Griot from contract {c.id}",
            "-- Do not edit manually -- re-run `griot scaffold` to regenerate",
            "",
            "with source as (",
            f"    select * from {{{{ source('{self._contract_snake}', '{leaf}') }}}}",
            "),",
            "",
            "staged as (",
            "    select",
        ]

        # Generate cast expressions
        cast_lines: list[str] = []
        for name, info in fields_dict.items():
            sql_type = map_logical_type(info.logical_type, self.target)
            cast_lines.append(f"        cast({name} as {sql_type}) as {name}")

        lines.append(",\n".join(cast_lines))
        lines.append("    from source")
        lines.append(")")
        lines.append("")
        lines.append("select * from staged")
        lines.append("")

        return "\n".join(lines)

    def _render_meta_macro(self) -> str:
        """Render the generate_contract_meta.sql helper macro."""
        return """\
{%- macro generate_contract_meta(contract_id, contract_version) -%}
  meta:
    griot_contract_id: '{{ contract_id }}'
    griot_contract_version: '{{ contract_version }}'
    generated_by: 'griot-scaffold'
{%- endmacro -%}
"""

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _yaml_value(value: Any) -> str:
        """Format a value for inline YAML output."""
        if isinstance(value, list):
            items = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in value)
            return f"[{items}]"
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, str):
            # If it looks like a Jinja reference (source(...)), don't quote
            if value.startswith("source(") or value.startswith("ref("):
                return value
            return f"'{value}'"
        return str(value)


# ── Custom generic test macro templates ──────────────────────────────

_CUSTOM_MACROS: dict[str, str] = {
    "griot_value_between": """\
{% test griot_value_between(model, column_name, min_value, max_value) %}

select *
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endtest %}
""",
    "griot_matches_pattern": """\
{% test griot_matches_pattern(model, column_name, pattern) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and not regexp_like(cast({{ column_name }} as varchar), '{{ pattern }}')

{% endtest %}
""",
    "griot_string_length_between": """\
{% test griot_string_length_between(model, column_name, min_length, max_length) %}

select *
from {{ model }}
where length(cast({{ column_name }} as varchar)) < {{ min_length }}
   or length(cast({{ column_name }} as varchar)) > {{ max_length }}

{% endtest %}
""",
    "griot_row_count_between": """\
{% test griot_row_count_between(model, min_count, max_count) %}

with row_count as (
    select count(*) as cnt from {{ model }}
)
select cnt from row_count
where cnt < {{ min_count }} or cnt > {{ max_count }}

{% endtest %}
""",
}
