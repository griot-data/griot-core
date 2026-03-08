"""Jinja2-based template engine for scaffold code generation.

Loads templates from the ``templates/`` package directory, registers
custom filters, and provides the main ``scaffold_contract()`` entry
point used by the CLI.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from griot_core.scaffold.codegen import to_pascal_case, to_snake_case
from griot_core.scaffold.type_mapping import map_logical_type, map_to_python_type

# ── Template directory ───────────────────────────────────────────────

_TEMPLATES_DIR = Path(__file__).parent / "templates"

# Orchestrator-specific template file names
_ORCHESTRATOR_TEMPLATES: dict[str, str] = {
    "airflow": "dag.py.j2",
    "dagster": "job.py.j2",
    "prefect": "flow.py.j2",
}


# ── Engine ───────────────────────────────────────────────────────────


class TemplateEngine:
    """Jinja2 template engine for scaffold code generation."""

    def __init__(self) -> None:
        self.env = Environment(
            loader=FileSystemLoader(str(_TEMPLATES_DIR)),
            autoescape=select_autoescape([]),
            keep_trailing_newline=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        # Register custom filters
        self.env.filters["to_pascal_case"] = to_pascal_case
        self.env.filters["to_snake_case"] = to_snake_case
        self.env.filters["map_type"] = lambda lt, tgt: map_logical_type(lt, tgt)
        self.env.filters["map_python_type"] = map_to_python_type

    def render_template(self, template_path: str, context: dict[str, Any]) -> str:
        """Render a single template with the given context."""
        template = self.env.get_template(template_path)
        return template.render(**context)


def _build_fk_ref(relationships: list[dict[str, Any]]) -> dict[str, str] | None:
    """Extract the first foreignKey relationship as a ref dict."""
    for rel in relationships:
        if rel.get("type") == "foreignKey":
            return {
                "schema": rel.get("toSchema", ""),
                "property": rel.get("to", ""),
            }
    return None


def _build_field_context(
    fields_dict: dict[str, Any],
    target: str,
) -> list[dict[str, Any]]:
    """Build enriched field context dicts for templates."""
    result: list[dict[str, Any]] = []
    for field_info in fields_dict.values():
        rels = field_info.relationships if hasattr(field_info, "relationships") else []
        fk_ref = _build_fk_ref(rels) if rels else None
        result.append(
            {
                "name": field_info.name,
                "snake_name": to_snake_case(field_info.name),
                "logical_type": field_info.logical_type,
                "sql_type": map_logical_type(field_info.logical_type, target),
                "python_type": map_to_python_type(field_info.logical_type),
                "description": field_info.description,
                "primary_key": field_info.primary_key,
                "required": field_info.required,
                "unique": field_info.unique,
                "nullable": field_info.nullable,
                "foreign_key": bool(fk_ref),
                "fk_references": fk_ref,
            }
        )
    return result


def _build_schema_context(
    schema: Any,
    target: str,
    database: str | None,
) -> dict[str, Any]:
    """Build a context dict for a single schema."""
    fields_dict = schema.fields
    field_ctx = _build_field_context(fields_dict, target)

    raw_name = schema.physical_name or to_snake_case(schema.name)
    if database:
        leaf = raw_name.rsplit(".", 1)[-1]
        table_name = f"{database}.{leaf}"
    else:
        table_name = raw_name

    return {
        "name": schema.name,
        "physical_name": schema.physical_name or to_snake_case(schema.name),
        "class_name": to_pascal_case(schema.name),
        "table_name": table_name,
        "fields": field_ctx,
        "primary_keys": [f["name"] for f in field_ctx if f["primary_key"]],
        "foreign_keys": [
            {
                "column": f["name"],
                "ref_table": f["fk_references"]["schema"],
                "ref_column": f["fk_references"]["property"],
            }
            for f in field_ctx
            if f.get("fk_references")
        ],
        "partition_keys": [],
        "field_comments": any(f["description"] for f in field_ctx),
        "required_fields": [f for f in field_ctx if not f["nullable"] or f["required"]],
        "unique_fields": [f for f in field_ctx if f["unique"] or f["primary_key"]],
        "quality_rules": [
            {
                "id": r.get("id", r.get("metric", "rule")),
                "safe_id": to_snake_case(str(r.get("id", r.get("metric", "rule")))),
            }
            for r in (schema.quality or [])
        ],
    }


def scaffold_contract(
    contract: Any,
    target: str,
    orchestrator: str = "none",
    output_dir: Path | None = None,
    components: list[str] | None = None,
    skip: list[str] | None = None,
    ci_provider: str = "none",
    database: str | None = None,
    profile: str = "default",
    registry_url: str = "",
    adapter: str | None = None,
    **kwargs: Any,
) -> dict[str, str]:
    """Render scaffold templates for a contract.

    All schemas in the contract are included in the generated output.

    Args:
        contract: A ``Contract`` instance (griot-core).
        target: Target warehouse (``databricks``, ``snowflake``, etc.).
        orchestrator: Orchestrator (``airflow``, ``dagster``, ``prefect``, ``none``).
        output_dir: Base output directory.
        components: List of components to include (default: all applicable).
        skip: List of components to exclude.
        ci_provider: CI provider (``github``, ``gitlab``, ``none``).
        database: Database/schema prefix for DDL.
        profile: Validation profile.
        registry_url: Registry URL for orchestrator config.
        adapter: Database adapter for dbt type casting.
        **kwargs: Additional keyword arguments (ignored, for backward compat).

    Returns:
        Dict mapping ``relative_path → rendered_content``.
    """
    # dbt target uses its own dedicated generator
    if target == "dbt":
        from griot_core.scaffold.dbt_codegen import DbtArtifactGenerator

        generator = DbtArtifactGenerator(contract, database=database, target=adapter or "dbt")
        rendered = generator.generate()
        # Also generate the config file
        engine = TemplateEngine()
        now = datetime.now(timezone.utc).isoformat()
        config_ctx: dict[str, Any] = {
            "contract_id": contract.id,
            "target": target,
            "orchestrator": orchestrator,
            "profile": profile,
            "registry_url": registry_url,
            "generated_at": now,
            "schemas": [],
            "dag_id": f"{to_snake_case(contract.id)}_pipeline",
            "owner": getattr(contract, "owner", "data-engineering"),
            "components": ["config"],
        }
        rendered[".griot/config.yaml"] = engine.render_template(
            "config.yaml.j2",
            config_ctx,
        )
        return rendered

    engine = TemplateEngine()

    schemas = contract.schemas
    if not schemas:
        return {"error.txt": "No schemas found in contract."}

    # Build per-schema context list
    schemas_ctx = [_build_schema_context(s, target, database) for s in schemas]

    # Build common context shared across all templates
    now = datetime.now(timezone.utc).isoformat()
    common_ctx: dict[str, Any] = {
        "contract_id": contract.id,
        "target": target,
        "orchestrator": orchestrator,
        "profile": profile,
        "registry_url": registry_url,
        "generated_at": now,
        "schemas": schemas_ctx,
        "dag_id": f"{to_snake_case(contract.id)}_pipeline",
        "owner": getattr(contract, "owner", "data-engineering"),
    }

    # Determine which components to render
    all_components = {"config"}  # always include config

    if target and target != "none":
        all_components.add("ddl")
    if orchestrator and orchestrator != "none":
        all_components.add("pipeline")
    all_components.add("tests")
    if ci_provider and ci_provider != "none":
        all_components.add("ci")

    if components:
        all_components &= set(components) | {"config"}  # config always included
    if skip:
        all_components -= set(skip)

    common_ctx["components"] = sorted(all_components)

    # Render templates
    rendered: dict[str, str] = {}
    contract_snake = to_snake_case(contract.id)

    if "ddl" in all_components:
        tmpl_path = f"targets/{target}/ddl.sql.j2"
        try:
            rendered[f"ddl/{contract_snake}.sql"] = engine.render_template(tmpl_path, common_ctx)
        except Exception:
            from griot_core.scaffold.codegen import generate_ddl

            rendered[f"ddl/{contract_snake}.sql"] = generate_ddl(contract, target, database)

    if "pipeline" in all_components and orchestrator != "none":
        tmpl_name = _ORCHESTRATOR_TEMPLATES.get(orchestrator)
        if tmpl_name:
            tmpl_path = f"orchestrators/{orchestrator}/{tmpl_name}"
            real_name = tmpl_name.removesuffix(".j2")
            ext = real_name.rsplit(".", 1)[-1] if "." in real_name else "py"
            rendered[f"pipeline/{contract_snake}_{orchestrator}.{ext}"] = engine.render_template(
                tmpl_path, common_ctx
            )

    if "tests" in all_components:
        rendered[f"tests/test_{contract_snake}.py"] = engine.render_template(
            "tests/test_contract.py.j2", common_ctx
        )

    if "ci" in all_components and ci_provider != "none":
        rendered[f".github/workflows/{contract_snake}_validate.yml"] = engine.render_template(
            f"ci/{ci_provider}.yml.j2", common_ctx
        )

    if "config" in all_components:
        rendered[".griot/config.yaml"] = engine.render_template("config.yaml.j2", common_ctx)

    return rendered
