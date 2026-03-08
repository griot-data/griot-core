"""
Griot Core Parsing Module.

This module provides parsers for loading contracts and schemas
from YAML and JSON formats.
"""

from __future__ import annotations

from .json_parser import (
    parse_contract_json,
    parse_schema_json,
)
from .validator import (
    StructureValidationResult,
    ValidationIssue,
    validate_contract_structure,
    validate_schema_structure,
)
from .yaml_parser import (
    load_contract_from_file,
    load_schema_from_file,
    parse_contract_yaml,
    parse_schema_yaml,
)

__all__ = [
    # YAML parsing
    "parse_contract_yaml",
    "parse_schema_yaml",
    "load_contract_from_file",
    "load_schema_from_file",
    # JSON parsing
    "parse_contract_json",
    "parse_schema_json",
    # Validation
    "validate_contract_structure",
    "validate_schema_structure",
    "ValidationIssue",
    "StructureValidationResult",
]
