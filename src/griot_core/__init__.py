"""
Griot Core - Data Contract Definition Library

Based on the Open Data Contract Standard (ODCS).

Submodules:
- griot_core.orchestration: Compute orchestration for validation jobs
- griot_core.workers: Worker implementations for different compute backends
- griot_core.validation: Validation engine and result types
- griot_core.executors: WASM and container executor runtimes
"""

from __future__ import annotations

__version__ = "0.9.2"

__all__ = [
    # Contract classes
    "Contract",
    "ContractDescription",
    "ContractTeam",
    "TeamMember",
    "ContractSupport",
    "ContractRole",
    "SLAProperty",
    "Server",
    # Schema classes
    "Schema",
    "Field",
    "FieldInfo",
    # Types
    "ContractStatus",
    "DataType",
    "Severity",
    # Quality rule types
    "QualityCheckType",
    "QualityMetric",
    "QualityOperator",
    "QualityUnit",
    "QualityRule",
    # Contract loading/export
    "load_contract",
    "load_contract_from_string",
    "load_contract_from_dict",
    "contract_to_yaml",
    "contract_to_dict",
    # Contract linting
    "lint_contract",
    "LintIssue",
    # Contract structure validation
    "validate_contract_structure",
    "ContractStructureResult",
    "ContractStructureIssue",
    # Constants
    "CONTRACT_FIELD_TYPES",
    "ODCS_MANDATORY_FIELDS",
    # Key normalization
    "normalize_keys",
    "to_snake_case",
    "to_camel_case",
    # Exceptions
    "GriotError",
    "ValidationError",
    "ContractNotFoundError",
    "ContractParseError",
    "ContractImmutableError",
    "ConstraintError",
    # Guards
    "can_modify_schema",
    "assert_can_modify_schema",
    # Reports
    "ContractReport",
    "generate_contract_report",
    # Mock data
    "generate_mock_data",
    # Manifest
    "export_manifest",
]

# Contract classes
from griot_core.contract import (
    # Constants
    CONTRACT_FIELD_TYPES,
    ODCS_MANDATORY_FIELDS,
    Contract,
    ContractDescription,
    ContractRole,
    ContractStructureIssue,
    ContractStructureResult,
    ContractSupport,
    ContractTeam,
    LintIssue,
    Server,
    SLAProperty,
    TeamMember,
    contract_to_dict,
    contract_to_yaml,
    # Contract linting
    lint_contract,
    # Contract loading/export
    load_contract,
    load_contract_from_dict,
    load_contract_from_string,
    # Key normalization
    normalize_keys,
    to_camel_case,
    to_snake_case,
    # Contract structure validation
    validate_contract_structure,
)

# Exceptions
from griot_core.exceptions import (
    ConstraintError,
    ContractImmutableError,
    ContractNotFoundError,
    ContractParseError,
    GriotError,
    ValidationError,
)

# Guards
from griot_core.guards import assert_can_modify_schema, can_modify_schema

# Manifest
from griot_core.manifest import export_manifest

# Mock data
from griot_core.mock import generate_mock_data

# Reports
from griot_core.reports import (
    ContractReport,
    generate_contract_report,
)

# Schema classes
from griot_core.schema import (
    Field,
    FieldInfo,
    Schema,
)

# Types
from griot_core.types import (
    ContractStatus,
    DataType,
    QualityCheckType,
    QualityMetric,
    QualityOperator,
    QualityRule,
    QualityUnit,
    Severity,
)
