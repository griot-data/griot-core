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

__version__ = "0.9.0"

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
    Contract,
    ContractDescription,
    ContractTeam,
    TeamMember,
    ContractSupport,
    ContractRole,
    SLAProperty,
    Server,
    # Contract loading/export
    load_contract,
    load_contract_from_string,
    load_contract_from_dict,
    contract_to_yaml,
    contract_to_dict,
    # Contract linting
    lint_contract,
    LintIssue,
    # Contract structure validation
    validate_contract_structure,
    ContractStructureResult,
    ContractStructureIssue,
    # Constants
    CONTRACT_FIELD_TYPES,
    ODCS_MANDATORY_FIELDS,
    # Key normalization
    normalize_keys,
    to_snake_case,
    to_camel_case,
)


# Types
from griot_core.types import (
    ContractStatus,
    DataType,
    Severity,
    QualityCheckType,
    QualityMetric,
    QualityOperator,
    QualityUnit,
    QualityRule,
)

# Schema classes
from griot_core.schema import (
    Schema,
    Field,
    FieldInfo,
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

# Reports
from griot_core.reports import (
    ContractReport,
    generate_contract_report,
)

# Mock data
from griot_core.mock import generate_mock_data

# Manifest
from griot_core.manifest import export_manifest