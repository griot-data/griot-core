"""
Griot Core Reporting Framework.

Extensible framework for parsing external tool test results (dbt, Soda,
Great Expectations, etc.) and reporting them to the Griot Registry as
Runs and Validations.

Architecture:
    Tool Output (run_results.json, etc.)
        -> ToolResultsParser Protocol  (parse into standard format)
        -> ToolValidationReport        (tool-agnostic intermediate)
        -> RegistryReporter            (push to Griot Registry API)
"""

from __future__ import annotations

__all__ = [
    "ToolResultsParser",
    "ToolValidationReport",
    "SchemaTestResult",
    "ToolCheckResult",
    "RegistryReporter",
    "ReportResult",
    "ParserRegistry",
    "get_default_parser_registry",
]


def __getattr__(name: str):
    """Lazy import reporting components."""
    if name in ("ToolResultsParser", "ToolValidationReport", "SchemaTestResult", "ToolCheckResult"):
        from .base import SchemaTestResult, ToolCheckResult, ToolResultsParser, ToolValidationReport

        return locals()[name]

    if name in ("ParserRegistry", "get_default_parser_registry"):
        from .registry import ParserRegistry, get_default_parser_registry

        return locals()[name]

    if name in ("RegistryReporter", "ReportResult"):
        from .reporter import RegistryReporter, ReportResult

        return locals()[name]

    raise AttributeError(f"module 'griot_core.reporting' has no attribute '{name}'")
