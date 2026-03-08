"""
Griot Core Integrations.

This module provides integrations with popular data orchestration frameworks:
- Apache Airflow: GriotValidateOperator, GriotReportOperator
- Prefect: griot_validate task
- Dagster: griot_validation_resource, griot_validated_asset

These integrations allow data teams to include Griot validation
and tool results reporting as part of their existing data pipelines.
"""

from __future__ import annotations

# Note: Integrations are imported conditionally to avoid
# requiring all orchestration frameworks to be installed.

__all__ = [
    # Airflow (requires apache-airflow)
    "GriotValidateOperator",
    "GriotValidateSensor",
    "GriotReportOperator",
    # Prefect (requires prefect)
    "griot_validate",
    "GriotValidationTask",
    # Dagster (requires dagster)
    "griot_validation_resource",
    "griot_validated_asset",
    "GriotResource",
]


def __getattr__(name: str):
    """Lazy import integrations to avoid requiring all frameworks."""
    if name in ("GriotValidateOperator", "GriotValidateSensor", "GriotReportOperator"):
        from .airflow import GriotReportOperator, GriotValidateOperator, GriotValidateSensor

        return locals()[name]

    if name in ("griot_validate", "GriotValidationTask"):
        from .prefect import GriotValidationTask, griot_validate

        return locals()[name]

    if name in ("griot_validation_resource", "griot_validated_asset", "GriotResource"):
        from .dagster import GriotResource, griot_validated_asset, griot_validation_resource

        return locals()[name]

    raise AttributeError(f"module 'griot_core.integrations' has no attribute '{name}'")
