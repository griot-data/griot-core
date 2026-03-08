"""
dbt results parser -- reference implementation of ToolResultsParser.

Parses dbt's target/run_results.json into a ToolValidationReport.
This is the first built-in parser. Adding support for a new tool
(Soda, Great Expectations, etc.) follows the same pattern.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from griot_core.reporting.base import SchemaTestResult, ToolCheckResult, ToolValidationReport

# Known dbt / griot test type prefixes, longest first to avoid partial matches.
# These are stable dbt built-in + griot custom macro names (not contract-specific).
KNOWN_TEST_TYPES = [
    "griot_string_length_between",
    "griot_row_count_between",
    "griot_value_between",
    "griot_matches_pattern",
    "expression_is_true",
    "accepted_values",
    "equal_rowcount",
    "relationships",
    "not_empty",
    "not_null",
    "unique",
]


class DbtResultsParser:
    """Parse dbt run_results.json into ToolValidationReport.

    This is the reference implementation of ToolResultsParser.
    Adding support for a new tool follows this same pattern.

    Args:
        schema_mapping: Maps physical/table names to contract schema names.
            e.g. {"dim_products": "products", "fact_orders": "orders"}
        asset_id_mapping: Maps contract schema names to registry asset UUIDs.
            e.g. {"products": "uuid-1234"}
        column_names: Maps table names to known column names for cross-referencing.
            e.g. {"dim_products": ["product_id", "name", "price"]}

    Example:
        parser = DbtResultsParser(
            schema_mapping={"dim_products": "products", "fact_orders": "orders"}
        )
        report = parser.parse(Path("target/run_results.json"))
        print(report.all_passed)
    """

    def __init__(
        self,
        schema_mapping: Optional[Dict[str, str]] = None,
        asset_id_mapping: Optional[Dict[str, str]] = None,
        column_names: Optional[Dict[str, List[str]]] = None,
    ):
        self._schema_mapping = schema_mapping or {}
        self._asset_id_mapping = asset_id_mapping or {}
        self._column_names = column_names or {}

    @property
    def tool_name(self) -> str:
        return "dbt"

    def _parse_unique_id(self, uid: str) -> Dict[str, Any]:
        """Parse a dbt unique_id into structured parts.

        Uses self._schema_mapping keys and self._column_names dynamically —
        works with ANY contract's tables and columns.
        """
        parts = uid.split(".")
        if len(parts) < 4:
            return {
                "project": "",
                "check_name": "",
                "table": "",
                "column": None,
                "hash": "",
                "is_source_test": False,
                "schema_key": None,
            }

        project = parts[1]
        body = parts[2]
        hash_val = parts[3]
        is_source = body.startswith("source_")

        # Strip source_ prefix
        remainder = body[len("source_") :] if is_source else body

        # Extract check_name by matching known test type prefixes
        check_name = ""
        after_check = remainder
        for ct in KNOWN_TEST_TYPES:
            if remainder.startswith(ct + "_") or remainder == ct:
                check_name = ct
                after_check = remainder[len(ct) + 1 :] if len(remainder) > len(ct) else ""
                break
        if not check_name:
            # Fallback: take the first token
            first_underscore = remainder.find("_")
            if first_underscore > 0:
                check_name = remainder[:first_underscore]
                after_check = remainder[first_underscore + 1 :]
            else:
                check_name = remainder
                after_check = ""

        # Resolve table using schema_mapping keys (dynamic, generic)
        resolved_table = None
        schema_key = None
        for table_name, contract_schema in self._schema_mapping.items():
            if table_name.lower() in after_check.lower():
                resolved_table = table_name
                schema_key = contract_schema
                break

        # Extract column by cross-referencing known column names
        column = None
        if resolved_table and resolved_table in self._column_names:
            known_cols = self._column_names[resolved_table]
            # Sort by length descending so "total_amount" matches before "amount"
            for col in sorted(known_cols, key=len, reverse=True):
                if after_check.endswith(col) or after_check.endswith("_" + col):
                    column = col
                    break
        if column is None and after_check:
            # Fallback: last underscore-separated token after table name
            if resolved_table:
                # Remove table name portion from after_check
                table_lower = resolved_table.lower()
                ac_lower = after_check.lower()
                idx = ac_lower.find(table_lower)
                if idx >= 0:
                    suffix = after_check[idx + len(table_lower) :]
                    suffix = suffix.lstrip("_")
                    if suffix:
                        column = suffix
            if column is None:
                # Last resort: last token
                column = after_check.rsplit("_", 1)[-1] if "_" in after_check else after_check

        return {
            "project": project,
            "check_name": check_name,
            "table": resolved_table or "",
            "column": column if column else None,
            "hash": hash_val,
            "is_source_test": is_source,
            "schema_key": schema_key,
        }

    def parse(self, results_path: Path) -> ToolValidationReport:
        """Parse target/run_results.json.

        Args:
            results_path: Path to dbt's run_results.json file.

        Returns:
            ToolValidationReport with per-schema and overall results.

        Raises:
            FileNotFoundError: If results file does not exist.
            ValueError: If file cannot be parsed as valid dbt results.
        """
        results_path = Path(results_path)
        if not results_path.exists():
            raise FileNotFoundError(f"{results_path} not found. Run 'dbt build' first.")

        with open(results_path) as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in {results_path}: {e}")

        results = data.get("results", [])

        # Per-schema buckets
        schema_results: Dict[str, SchemaTestResult] = {}
        overall = SchemaTestResult(schema_name="_overall")

        # Extract dbt metadata
        metadata: Dict[str, Any] = {}
        dbt_metadata = data.get("metadata", {})
        if dbt_metadata:
            metadata["dbt_version"] = dbt_metadata.get("dbt_version", "")
            metadata["invocation_id"] = dbt_metadata.get("invocation_id", "")
            metadata["generated_at"] = dbt_metadata.get("generated_at", "")
            metadata["invocation_started_at"] = dbt_metadata.get("invocation_started_at", "")
        elapsed = data.get("elapsed_time")
        if elapsed is not None:
            metadata["elapsed_time"] = elapsed

        dbt_project = ""

        for result in results:
            uid = result.get("unique_id", "")

            # Only process test nodes
            if not uid.startswith("test."):
                continue

            status = result.get("status", "")
            exec_time = result.get("execution_time", 0.0)
            failures = result.get("failures", 0) or 0
            message = result.get("message", "")

            # Parse the unique_id into structured parts
            parsed = self._parse_unique_id(uid)
            schema_key = parsed["schema_key"]

            # Capture dbt_project from first test
            if not dbt_project and parsed["project"]:
                dbt_project = parsed["project"]

            if schema_key is None:
                schema_key = "_unknown"

            # Get or create the schema bucket
            if schema_key not in schema_results:
                schema_results[schema_key] = SchemaTestResult(
                    schema_name=schema_key,
                )

            sr = schema_results[schema_key]
            duration_ms = exec_time * 1000  # seconds -> ms

            sr.total += 1
            sr.duration_ms += duration_ms
            overall.total += 1
            overall.duration_ms += duration_ms

            if status == "pass":
                sr.passed += 1
                overall.passed += 1
            elif status == "fail":
                sr.failed += 1
                overall.failed += 1
                # Extract a useful test name from the unique_id
                parts = uid.split(".")
                test_name = parts[2] if len(parts) > 2 else uid
                error_detail = {
                    "field": test_name,
                    "constraint": test_name,
                    "message": f"{test_name}: {message} ({failures} failures)",
                }
                sr.errors.append(error_detail)
                overall.errors.append(error_detail)
            elif status == "warn":
                sr.warned += 1
                overall.warned += 1
            elif status == "error":
                sr.errored += 1
                overall.errored += 1
            elif status == "skip":
                sr.skipped += 1
                overall.skipped += 1

            # Build individual ToolCheckResult
            check_result = ToolCheckResult(
                check_name=parsed["check_name"],
                status=status,
                column=parsed["column"],
                table=parsed["table"] or None,
                id=parsed["hash"] or None,
                execution_time_ms=round(duration_ms, 2),
                severity=result.get("severity", "warning") or "warning",
                failures=failures,
                is_source_test=parsed["is_source_test"],
            )

            # Look up registry asset ID
            effective_schema = parsed["schema_key"] or schema_key
            if effective_schema and effective_schema in self._asset_id_mapping:
                check_result.asset_id = self._asset_id_mapping[effective_schema]

            # Only attach compiled SQL for failures/errors to keep payload compact
            if status in ("fail", "error"):
                compiled = result.get("compiled_code") or result.get("node", {}).get(
                    "compiled_code"
                )
                if compiled:
                    check_result.compiled_code = compiled
                if message:
                    check_result.error = message

            sr.check_results.append(check_result)
            overall.check_results.append(check_result)

        # Store dbt project in metadata
        if dbt_project:
            metadata["dbt_project"] = dbt_project

        # Remove _unknown if empty
        unknown = schema_results.pop("_unknown", None)
        if unknown is not None and unknown.total > 0:
            schema_results["_unknown"] = unknown

        all_passed = overall.failed == 0 and overall.errored == 0

        return ToolValidationReport(
            tool="dbt",
            schemas=schema_results,
            overall=overall,
            all_passed=all_passed,
            metadata=metadata,
        )

    def supports_file(self, path: Path) -> bool:
        """Check if this parser can handle the given file."""
        return path.name == "run_results.json"

    @classmethod
    def from_contract(cls, contract) -> "DbtResultsParser":
        """Create parser by extracting schema mapping from a Contract.

        Uses contract.inline_schemas[*].physical_name -> name mapping.
        Also extracts column names and registry asset IDs when available.

        Args:
            contract: A griot_core.models.Contract instance.

        Returns:
            DbtResultsParser configured with the contract's schema mapping.
        """
        schema_mapping: Dict[str, str] = {}
        asset_id_mapping: Dict[str, str] = {}
        column_names: Dict[str, List[str]] = {}

        for schema in getattr(contract, "inline_schemas", []):
            physical = getattr(schema, "physical_name", "")
            name = getattr(schema, "name", "")
            if physical and name:
                # Use just the table name part (strip schema prefix if present)
                table_name = physical.split(".")[-1]
                schema_mapping[table_name] = name

                # Collect column names for this table
                props = getattr(schema, "properties", [])
                col_names = [getattr(p, "name", "") for p in props if getattr(p, "name", "")]
                if col_names:
                    column_names[table_name] = col_names

            # Extract registry asset ID from _ref.schemaId
            ref = getattr(schema, "registry_ref", None)
            schema_id = ref.get("schemaId") if isinstance(ref, dict) else None
            if schema_id and name:
                asset_id_mapping[name] = schema_id

        return cls(
            schema_mapping=schema_mapping,
            asset_id_mapping=asset_id_mapping,
            column_names=column_names,
        )
