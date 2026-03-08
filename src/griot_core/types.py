"""
Griot Core Type Definitions

Enums and type definitions for the Griot data contract system.
Based on the Open Data Contract Standard (ODCS).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

__all__ = [
    # Core enums
    "ContractStatus",
    "DataType",
    "Severity",
    "DataFrameType",
    # Quality rule enums
    "QualityCheckType",
    "QualityMetric",
    "QualityOperator",
    "QualityUnit",
    # Quality rule builder
    "QualityRule",
    # Validation results
    "ErrorSeverity",
    "FieldValidationError",
]

from typing import Any


class ContractStatus(str, Enum):
    """
    Contract lifecycle status.

    Based on Open Data Contract Standard.
    """

    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class DataFrameType(str, Enum):
    """Types of data frames in a data processing context."""

    PANDAS = "pandas"
    DASK = "dask"
    SPARK = "spark"
    POLARS = "polars"


class DataType(str, Enum):
    """Supported logical types for contract fields."""

    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    OBJECT = "object"
    ANY = "any"

    @classmethod
    def from_python_type(cls, python_type: type | str) -> DataType:
        """Convert a Python type to a DataType enum value."""
        type_mapping: dict[type | str, DataType] = {
            str: cls.STRING,
            int: cls.INTEGER,
            float: cls.FLOAT,
            bool: cls.BOOLEAN,
            list: cls.ARRAY,
            dict: cls.OBJECT,
            "str": cls.STRING,
            "int": cls.INTEGER,
            "float": cls.FLOAT,
            "bool": cls.BOOLEAN,
            "list": cls.ARRAY,
            "dict": cls.OBJECT,
            "date": cls.DATE,
            "datetime": cls.DATETIME,
        }
        return type_mapping.get(python_type, cls.ANY)

    def to_python_type(self) -> type:
        """Convert DataType to Python type."""
        type_mapping: dict[DataType, type] = {
            DataType.STRING: str,
            DataType.INTEGER: int,
            DataType.FLOAT: float,
            DataType.BOOLEAN: bool,
            DataType.ARRAY: list,
            DataType.OBJECT: dict,
            DataType.DATE: str,
            DataType.DATETIME: str,
            DataType.ANY: object,
        }
        return type_mapping[self]


class Severity(str, Enum):
    """Error/issue severity levels for validation and linting."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class FieldValidationError:
    """Single field validation error."""

    field: str
    row: int | None
    value: Any
    constraint: str
    message: str
    severity: Severity = Severity.ERROR

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "field": self.field,
            "row": self.row,
            "value": _serialize_value(self.value),
            "constraint": self.constraint,
            "message": self.message,
            "severity": self.severity.value,
        }

    def __str__(self) -> str:
        loc = f"[row {self.row}]" if self.row is not None else ""
        return f"{self.field}{loc}: {self.message}"


def _serialize_value(value: Any) -> Any:
    """Convert a value to a JSON-serializable format."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    return str(value)


class QualityCheckType(str, Enum):
    """
    Types of validation checks based on ODCS specification.

    - LIBRARY: Executable checks using standard metrics (nullValues, etc.)
    - TEXT: Human-readable rules for documentation
    - SQL: SQL-based validation queries
    - CUSTOM: Vendor-specific custom checks
    """

    LIBRARY = "library"
    TEXT = "text"
    SQL = "sql"
    CUSTOM = "custom"

    def __str__(self) -> str:
        return self.value


class QualityMetric(str, Enum):
    """
    ODCS quality metrics for library-type checks.

    Property-level metrics (operate on a single column):
    - NULL_VALUES: Count of null/None values
    - MISSING_VALUES: Count of values considered missing (null, empty, N/A, etc.)
    - INVALID_VALUES: Count of values failing validation rules
    - DUPLICATE_VALUES: Count of duplicate values in a column

    Schema-level metrics (operate on entire table/dataset):
    - ROW_COUNT: Total number of rows in the dataset
    - DUPLICATE_ROWS: Duplicate rows across specified columns (schema-level)
    """

    # Property-level metrics
    NULL_VALUES = "nullValues"
    MISSING_VALUES = "missingValues"
    INVALID_VALUES = "invalidValues"
    DUPLICATE_VALUES = "duplicateValues"

    # Schema-level metrics
    ROW_COUNT = "rowCount"
    DUPLICATE_ROWS = "duplicateRows"

    def __str__(self) -> str:
        return self.value

    @property
    def is_property_level(self) -> bool:
        """Check if this metric operates at the property/column level."""
        return self in (
            QualityMetric.NULL_VALUES,
            QualityMetric.MISSING_VALUES,
            QualityMetric.INVALID_VALUES,
            QualityMetric.DUPLICATE_VALUES,
        )

    @property
    def is_schema_level(self) -> bool:
        """Check if this metric operates at the schema/table level."""
        return self in (
            QualityMetric.ROW_COUNT,
            QualityMetric.DUPLICATE_ROWS,
        )


class QualityOperator(str, Enum):
    """
    ODCS comparison operators for quality rule thresholds.

    These operators define how to compare the calculated metric value
    against the specified threshold.
    """

    MUST_BE = "mustBe"
    MUST_NOT_BE = "mustNotBe"
    MUST_BE_GREATER_THAN = "mustBeGreaterThan"
    MUST_BE_GREATER_OR_EQUAL_TO = "mustBeGreaterOrEqualTo"
    MUST_BE_LESS_THAN = "mustBeLessThan"
    MUST_BE_LESS_OR_EQUAL_TO = "mustBeLessOrEqualTo"
    MUST_BE_BETWEEN = "mustBeBetween"
    MUST_NOT_BE_BETWEEN = "mustNotBeBetween"

    def __str__(self) -> str:
        return self.value

    @property
    def comparison_type(self) -> str:
        """Get the internal comparison type code."""
        mapping = {
            QualityOperator.MUST_BE: "eq",
            QualityOperator.MUST_NOT_BE: "ne",
            QualityOperator.MUST_BE_GREATER_THAN: "gt",
            QualityOperator.MUST_BE_GREATER_OR_EQUAL_TO: "ge",
            QualityOperator.MUST_BE_LESS_THAN: "lt",
            QualityOperator.MUST_BE_LESS_OR_EQUAL_TO: "le",
            QualityOperator.MUST_BE_BETWEEN: "between",
            QualityOperator.MUST_NOT_BE_BETWEEN: "not_between",
        }
        return mapping[self]

    def compare(self, value: float, threshold: Any) -> bool:
        """
        Compare a metric value against a threshold.

        Args:
            value: The calculated metric value
            threshold: The comparison threshold

        Returns:
            True if the comparison passes, False otherwise
        """
        if self == QualityOperator.MUST_BE:
            return value == threshold
        elif self == QualityOperator.MUST_NOT_BE:
            return value != threshold
        elif self == QualityOperator.MUST_BE_GREATER_THAN:
            return value > threshold
        elif self == QualityOperator.MUST_BE_GREATER_OR_EQUAL_TO:
            return value >= threshold
        elif self == QualityOperator.MUST_BE_LESS_THAN:
            return value < threshold
        elif self == QualityOperator.MUST_BE_LESS_OR_EQUAL_TO:
            return value <= threshold
        elif self == QualityOperator.MUST_BE_BETWEEN:
            if isinstance(threshold, (list, tuple)) and len(threshold) == 2:
                return threshold[0] < value < threshold[1]
            return False
        elif self == QualityOperator.MUST_NOT_BE_BETWEEN:
            if isinstance(threshold, (list, tuple)) and len(threshold) == 2:
                return not (threshold[0] < value < threshold[1])
            return True
        return True


class QualityUnit(str, Enum):
    """
    Units for quality metric values.

    - ROWS: Absolute count of rows
    - PERCENT: Percentage of total rows
    """

    ROWS = "rows"
    PERCENT = "percent"

    def __str__(self) -> str:
        return self.value

    def calculate_metric(self, count: int, total: int) -> float:
        """
        Calculate the metric value based on the unit.

        Args:
            count: The raw count value
            total: The total number of rows

        Returns:
            The metric value in the appropriate unit
        """
        if self == QualityUnit.PERCENT and total > 0:
            return (count / total) * 100
        return float(count)


@dataclass
class QualityRule:
    """
    Builder class for creating ODCS-compliant quality rules.

    This class provides a type-safe way to build quality rules using enums
    instead of raw strings, reducing errors when defining contracts.

    Example - Property-level rules (for field.quality):
        >>> # No null values allowed
        >>> QualityRule.null_values(must_be=0)
        {'metric': 'nullValues', 'mustBe': 0}

        >>> # Less than 5% missing values
        >>> QualityRule.missing_values(must_be_less_than=5, unit=QualityUnit.PERCENT)
        {'metric': 'missingValues', 'mustBeLessThan': 5, 'unit': 'percent'}

        >>> # Values must be in valid set
        >>> QualityRule.invalid_values(must_be=0, valid_values=['active', 'inactive'])
        {'metric': 'invalidValues', 'mustBe': 0, 'arguments': {'validValues': ['active', 'inactive']}}

    Example - Schema-level rules (for schema.quality):
        >>> # Row count between 100 and 10000
        >>> QualityRule.row_count(must_be_between=[100, 10000])
        {'metric': 'rowCount', 'mustBeBetween': [100, 10000]}

        >>> # No duplicate rows on composite key
        >>> QualityRule.duplicate_rows(must_be=0, properties=['user_id', 'date'])
        {'metric': 'duplicateRows', 'mustBe': 0, 'arguments': {'properties': ['user_id', 'date']}}
    """

    @staticmethod
    def _build_rule(
        metric: QualityMetric,
        *,
        must_be: Any = None,
        must_not_be: Any = None,
        must_be_greater_than: Any = None,
        must_be_greater_or_equal_to: Any = None,
        must_be_less_than: Any = None,
        must_be_less_or_equal_to: Any = None,
        must_be_between: list[Any] | tuple[Any, Any] | None = None,
        must_not_be_between: list[Any] | tuple[Any, Any] | None = None,
        unit: QualityUnit | None = None,
        rule_id: str | None = None,
        name: str | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a quality rule dictionary from typed parameters."""
        rule: dict[str, Any] = {"id": rule_id, "metric": metric.value}

        # Add operator
        if must_be is not None:
            rule[QualityOperator.MUST_BE.value] = must_be
        elif must_not_be is not None:
            rule[QualityOperator.MUST_NOT_BE.value] = must_not_be
        elif must_be_greater_than is not None:
            rule[QualityOperator.MUST_BE_GREATER_THAN.value] = must_be_greater_than
        elif must_be_greater_or_equal_to is not None:
            rule[QualityOperator.MUST_BE_GREATER_OR_EQUAL_TO.value] = must_be_greater_or_equal_to
        elif must_be_less_than is not None:
            rule[QualityOperator.MUST_BE_LESS_THAN.value] = must_be_less_than
        elif must_be_less_or_equal_to is not None:
            rule[QualityOperator.MUST_BE_LESS_OR_EQUAL_TO.value] = must_be_less_or_equal_to
        elif must_be_between is not None:
            rule[QualityOperator.MUST_BE_BETWEEN.value] = list(must_be_between)
        elif must_not_be_between is not None:
            rule[QualityOperator.MUST_NOT_BE_BETWEEN.value] = list(must_not_be_between)

        # Add optional fields
        if unit is not None:
            rule["unit"] = unit.value
        if name is not None:
            rule["name"] = name
        if arguments is not None:
            rule["arguments"] = arguments

        return rule

    @classmethod
    def null_values(
        cls,
        *,
        must_be: int | None = None,
        must_not_be: int | None = None,
        must_be_less_than: int | None = None,
        must_be_less_or_equal_to: int | None = None,
        must_be_greater_than: int | None = None,
        must_be_greater_or_equal_to: int | None = None,
        must_be_between: list[int] | tuple[int, int] | None = None,
        unit: QualityUnit = QualityUnit.ROWS,
        rule_id: str | None = "null_values_check",
        name: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a null values quality rule.

        Args:
            must_be: Exact count of null values allowed
            must_not_be: Count that null values must not equal
            must_be_less_than: Maximum null values (exclusive)
            must_be_less_or_equal_to: Maximum null values (inclusive)
            must_be_greater_than: Minimum null values (exclusive)
            must_be_greater_or_equal_to: Minimum null values (inclusive)
            must_be_between: Range [min, max] for null values
            unit: ROWS for absolute count, PERCENT for percentage
            rule_id: Unique identifier for the rule
            name: Human-readable name

        Returns:
            ODCS-compliant quality rule dictionary
        """
        return cls._build_rule(
            QualityMetric.NULL_VALUES,
            must_be=must_be,
            must_not_be=must_not_be,
            must_be_less_than=must_be_less_than,
            must_be_less_or_equal_to=must_be_less_or_equal_to,
            must_be_greater_than=must_be_greater_than,
            must_be_greater_or_equal_to=must_be_greater_or_equal_to,
            must_be_between=must_be_between,
            unit=unit if unit != QualityUnit.ROWS else None,
            rule_id=rule_id,
            name=name,
        )

    @classmethod
    def missing_values(
        cls,
        *,
        must_be: int | None = None,
        must_not_be: int | None = None,
        must_be_less_than: int | None = None,
        must_be_less_or_equal_to: int | None = None,
        must_be_greater_than: int | None = None,
        must_be_greater_or_equal_to: int | None = None,
        must_be_between: list[int] | tuple[int, int] | None = None,
        unit: QualityUnit = QualityUnit.ROWS,
        missing_values_list: list[Any] | None = None,
        rule_id: str | None = "Missing_values_check",
        name: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a missing values quality rule.

        Missing values include null, empty strings, 'N/A', etc.

        Args:
            must_be: Exact count of missing values allowed
            must_not_be: Count that missing values must not equal
            must_be_less_than: Maximum missing values (exclusive)
            must_be_less_or_equal_to: Maximum missing values (inclusive)
            must_be_greater_than: Minimum missing values (exclusive)
            must_be_greater_or_equal_to: Minimum missing values (inclusive)
            must_be_between: Range [min, max] for missing values
            unit: ROWS for absolute count, PERCENT for percentage
            missing_values_list: Custom list of values considered missing
            rule_id: Unique identifier for the rule
            name: Human-readable name

        Returns:
            ODCS-compliant quality rule dictionary
        """
        arguments = None
        if missing_values_list is not None:
            arguments = {"missingValues": missing_values_list}

        return cls._build_rule(
            QualityMetric.MISSING_VALUES,
            must_be=must_be,
            must_not_be=must_not_be,
            must_be_less_than=must_be_less_than,
            must_be_less_or_equal_to=must_be_less_or_equal_to,
            must_be_greater_than=must_be_greater_than,
            must_be_greater_or_equal_to=must_be_greater_or_equal_to,
            must_be_between=must_be_between,
            unit=unit if unit != QualityUnit.ROWS else None,
            rule_id=rule_id,
            name=name,
            arguments=arguments,
        )

    @classmethod
    def invalid_values(
        cls,
        *,
        must_be: int | None = None,
        must_not_be: int | None = None,
        must_be_less_than: int | None = None,
        must_be_less_or_equal_to: int | None = None,
        unit: QualityUnit = QualityUnit.ROWS,
        valid_values: list[Any] | None = None,
        pattern: str | None = None,
        min_value: int | float | None = None,
        max_value: int | float | None = None,
        min_length: int | None = None,
        max_length: int | None = None,
        rule_id: str | None = "invalid_values_check",
        name: str | None = None,
    ) -> dict[str, Any]:
        """
        Create an invalid values quality rule.

        Args:
            must_be: Exact count of invalid values allowed (typically 0)
            must_not_be: Count that invalid values must not equal
            must_be_less_than: Maximum invalid values (exclusive)
            must_be_less_or_equal_to: Maximum invalid values (inclusive)
            unit: ROWS for absolute count, PERCENT for percentage
            valid_values: List of allowed values (enum constraint)
            pattern: Regex pattern values must match
            min_value: Minimum numeric value allowed
            max_value: Maximum numeric value allowed
            min_length: Minimum string length
            max_length: Maximum string length
            rule_id: Unique identifier for the rule
            name: Human-readable name

        Returns:
            ODCS-compliant quality rule dictionary
        """
        arguments: dict[str, Any] = {}
        if valid_values is not None:
            arguments["validValues"] = valid_values
        if pattern is not None:
            arguments["pattern"] = pattern
        if min_value is not None:
            arguments["minValue"] = min_value
        if max_value is not None:
            arguments["maxValue"] = max_value
        if min_length is not None:
            arguments["minLength"] = min_length
        if max_length is not None:
            arguments["maxLength"] = max_length

        return cls._build_rule(
            QualityMetric.INVALID_VALUES,
            must_be=must_be,
            must_not_be=must_not_be,
            must_be_less_than=must_be_less_than,
            must_be_less_or_equal_to=must_be_less_or_equal_to,
            unit=unit if unit != QualityUnit.ROWS else None,
            rule_id=rule_id,
            name=name,
            arguments=arguments if arguments else None,
        )

    @classmethod
    def duplicate_values(
        cls,
        *,
        must_be: int | None = None,
        must_not_be: int | None = None,
        must_be_less_than: int | None = None,
        must_be_less_or_equal_to: int | None = None,
        unit: QualityUnit = QualityUnit.ROWS,
        rule_id: str | None = "duplicate_values_check",
        name: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a duplicate values quality rule for a single column.

        For property/field-level duplicate checking.

        Args:
            must_be: Exact count of duplicates allowed (typically 0)
            must_not_be: Count that duplicates must not equal
            must_be_less_than: Maximum duplicates (exclusive)
            must_be_less_or_equal_to: Maximum duplicates (inclusive)
            unit: ROWS for absolute count, PERCENT for percentage
            rule_id: Unique identifier for the rule
            name: Human-readable name

        Returns:
            ODCS-compliant quality rule dictionary
        """
        return cls._build_rule(
            QualityMetric.DUPLICATE_VALUES,
            must_be=must_be,
            must_not_be=must_not_be,
            must_be_less_than=must_be_less_than,
            must_be_less_or_equal_to=must_be_less_or_equal_to,
            unit=unit if unit != QualityUnit.ROWS else None,
            rule_id=rule_id,
            name=name,
        )

    @classmethod
    def row_count(
        cls,
        *,
        must_be: int | None = None,
        must_not_be: int | None = None,
        must_be_greater_than: int | None = None,
        must_be_greater_or_equal_to: int | None = None,
        must_be_less_than: int | None = None,
        must_be_less_or_equal_to: int | None = None,
        must_be_between: list[int] | tuple[int, int] | None = None,
        rule_id: str | None = "row_count_check",
        name: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a row count quality rule (schema-level).

        Use this in schema.quality to validate total row count.

        Args:
            must_be: Exact row count required
            must_not_be: Row count that must not occur
            must_be_greater_than: Minimum rows (exclusive)
            must_be_greater_or_equal_to: Minimum rows (inclusive)
            must_be_less_than: Maximum rows (exclusive)
            must_be_less_or_equal_to: Maximum rows (inclusive)
            must_be_between: Range [min, max] for row count
            rule_id: Unique identifier for the rule
            name: Human-readable name

        Returns:
            ODCS-compliant quality rule dictionary
        """
        return cls._build_rule(
            QualityMetric.ROW_COUNT,
            must_be=must_be,
            must_not_be=must_not_be,
            must_be_greater_than=must_be_greater_than,
            must_be_greater_or_equal_to=must_be_greater_or_equal_to,
            must_be_less_than=must_be_less_than,
            must_be_less_or_equal_to=must_be_less_or_equal_to,
            must_be_between=must_be_between,
            rule_id=rule_id,
            name=name,
        )

    @classmethod
    def duplicate_rows(
        cls,
        *,
        must_be: int | None = None,
        must_not_be: int | None = None,
        must_be_less_than: int | None = None,
        must_be_less_or_equal_to: int | None = None,
        unit: QualityUnit = QualityUnit.ROWS,
        properties: list[str] | None = None,
        rule_id: str | None = "duplicate_rows_check",
        name: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a duplicate rows quality rule (schema-level).

        Use this in schema.quality to validate row uniqueness across columns.

        Args:
            must_be: Exact count of duplicate rows allowed (typically 0)
            must_not_be: Count that duplicates must not equal
            must_be_less_than: Maximum duplicate rows (exclusive)
            must_be_less_or_equal_to: Maximum duplicate rows (inclusive)
            unit: ROWS for absolute count, PERCENT for percentage
            properties: List of column names to check for uniqueness.
                       If None, checks all columns.
            rule_id: Unique identifier for the rule
            name: Human-readable name

        Returns:
            ODCS-compliant quality rule dictionary
        """
        arguments = None
        if properties is not None:
            arguments = {"properties": properties}

        return cls._build_rule(
            QualityMetric.DUPLICATE_ROWS,
            must_be=must_be,
            must_not_be=must_not_be,
            must_be_less_than=must_be_less_than,
            must_be_less_or_equal_to=must_be_less_or_equal_to,
            unit=unit if unit != QualityUnit.ROWS else None,
            rule_id=rule_id,
            name=name,
            arguments=arguments,
        )


"""Privacy types and enums for data protection compliance.

Supports Kenya Data Protection Act 2019 and EU GDPR requirements.
"""


class ErrorSeverity(Enum):
    """Severity level for validation issues."""

    ERROR = "error"  # Validation failure
    WARNING = "warning"  # Non-blocking issue
    INFO = "info"  # Informational


class Sensitivity(Enum):
    """Data sensitivity classification levels.

    Based on common enterprise data classification schemes
    aligned with Kenya DPA and GDPR requirements.
    """

    PUBLIC = "public"
    """Data that can be freely shared."""

    INTERNAL = "internal"
    """Data for internal use only, not public."""

    CONFIDENTIAL = "confidential"
    """Sensitive data requiring protection."""

    RESTRICTED = "restricted"
    """Highly sensitive data with strict access controls."""


class PIIType(Enum):
    """Types of Personally Identifiable Information.

    Categories based on common PII classifications.
    """

    # Direct identifiers
    NAME = "name"
    EMAIL = "email"
    PHONE = "phone"
    ADDRESS = "address"

    # Government IDs
    NATIONAL_ID = "national_id"  # Kenya National ID
    PASSPORT = "passport"
    TAX_ID = "tax_id"  # KRA PIN (Kenya)
    SSN = "ssn"  # Social Security Number

    # Financial
    CREDIT_CARD = "credit_card"
    BANK_ACCOUNT = "bank_account"
    IBAN = "iban"

    # Health
    HEALTH_DATA = "health_data"
    MEDICAL_RECORD = "medical_record"

    # Biometric
    BIOMETRIC = "biometric"
    FINGERPRINT = "fingerprint"
    FACIAL_DATA = "facial_data"

    # Digital identifiers
    IP_ADDRESS = "ip_address"
    MAC_ADDRESS = "mac_address"
    DEVICE_ID = "device_id"
    USERNAME = "username"

    # Other
    DATE_OF_BIRTH = "date_of_birth"
    GENDER = "gender"
    ETHNICITY = "ethnicity"
    RELIGION = "religion"
    POLITICAL_OPINION = "political_opinion"
    SEXUAL_ORIENTATION = "sexual_orientation"

    # Generic
    OTHER = "other"
    UNKNOWN = "unknown"


class MaskingStrategy(Enum):
    """Strategies for masking PII data.

    Different masking approaches for different data types.
    """

    # Complete masking
    REDACT = "redact"
    """Replace with [REDACTED] or similar."""

    HASH = "hash"
    """Replace with cryptographic hash."""

    # Partial masking
    PARTIAL = "partial"
    """Show first/last few characters (e.g., J***n for John)."""

    TRUNCATE = "truncate"
    """Show only first N characters."""

    ASTERISK = "asterisk"
    """Replace characters with asterisks."""

    # Value transformation
    GENERALIZE = "generalize"
    """Generalize to category (e.g., exact age -> age range)."""

    PSEUDONYMIZE = "pseudonymize"
    """Replace with consistent fake value."""

    TOKENIZE = "tokenize"
    """Replace with reversible token."""

    # Null/remove
    NULL = "null"
    """Replace with null/None."""

    REMOVE = "remove"
    """Remove the field entirely."""

    # No masking
    NONE = "none"
    """No masking applied."""


class ComplianceFramework(Enum):
    """Data protection compliance frameworks."""

    KENYA_DPA = "kenya_dpa"
    """Kenya Data Protection Act 2019."""

    GDPR = "gdpr"
    """EU General Data Protection Regulation."""

    CCPA = "ccpa"
    """California Consumer Privacy Act."""

    HIPAA = "hipaa"
    """US Health Insurance Portability and Accountability Act."""

    PCI_DSS = "pci_dss"
    """Payment Card Industry Data Security Standard."""

    CUSTOM = "custom"
    """Custom/internal privacy policy."""


@dataclass
class PrivacyInfo:
    """Privacy metadata for a schema field.

    Attributes:
        is_pii (bool): Whether this field contains PII.
        pii_type (PIIType | None): Type of PII if is_pii is True.
        sensitivity (Sensitivity): Data sensitivity classification.
        requires_masking (bool): Whether this field requires masking in outputs.
        masking_strategy (MaskingStrategy): Strategy to use when masking is required.
        requires_encryption (bool): Whether this field requires encryption at rest.
        requires_consent (bool): Whether processing requires explicit consent.
        retention_days (int | None): Data retention period in days (None = indefinite).
        purpose_limitation (list[str]): Allowed purposes for processing this data.
        compliance_frameworks (list[ComplianceFramework]): Applicable compliance frameworks.
        custom_properties (dict[str, Any]): Additional custom privacy properties.

    """

    is_pii: bool = False
    """Whether this field contains PII."""

    pii_type: PIIType | None = None
    """Type of PII if is_pii is True."""

    sensitivity: Sensitivity = Sensitivity.INTERNAL
    """Data sensitivity classification."""

    requires_masking: bool = False
    """Whether this field requires masking in outputs."""

    masking_strategy: MaskingStrategy = MaskingStrategy.NONE
    """Strategy to use when masking is required."""

    requires_encryption: bool = False
    """Whether this field requires encryption at rest."""

    requires_consent: bool = False
    """Whether processing requires explicit consent."""

    retention_days: int | None = None
    """Data retention period in days (None = indefinite)."""

    purpose_limitation: list[str] = field(default_factory=list)
    """Allowed purposes for processing this data."""

    compliance_frameworks: list[ComplianceFramework] = field(default_factory=list)
    """Applicable compliance frameworks."""

    custom_properties: dict[str, Any] = field(default_factory=dict)
    """Additional custom privacy properties."""

    def __post_init__(self):
        """Validate privacy info consistency."""
        # If is_pii but no type, set to UNKNOWN
        if self.is_pii and self.pii_type is None:
            self.pii_type = PIIType.UNKNOWN

        # If PII, should at least be INTERNAL sensitivity
        if self.is_pii and self.sensitivity == Sensitivity.PUBLIC:
            # PII should not be public - upgrade to INTERNAL
            self.sensitivity = Sensitivity.INTERNAL

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        result: dict[str, Any] = {
            "is_pii": self.is_pii,
            "sensitivity": self.sensitivity.value,
        }

        if self.pii_type is not None:
            result["pii_type"] = self.pii_type.value

        if self.requires_masking:
            result["requires_masking"] = True
            result["masking_strategy"] = self.masking_strategy.value

        if self.requires_encryption:
            result["requires_encryption"] = True

        if self.requires_consent:
            result["requires_consent"] = True

        if self.retention_days is not None:
            result["retention_days"] = self.retention_days

        if self.purpose_limitation:
            result["purpose_limitation"] = self.purpose_limitation

        if self.compliance_frameworks:
            result["compliance_frameworks"] = [f.value for f in self.compliance_frameworks]

        if self.custom_properties:
            result["custom_properties"] = self.custom_properties

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PrivacyInfo:
        """Create PrivacyInfo from dictionary."""
        pii_type = None
        if "pii_type" in data:
            pii_type = PIIType(data["pii_type"])

        sensitivity = Sensitivity.INTERNAL
        if "sensitivity" in data:
            sensitivity = Sensitivity(data["sensitivity"])

        masking_strategy = MaskingStrategy.NONE
        if "masking_strategy" in data:
            masking_strategy = MaskingStrategy(data["masking_strategy"])

        compliance_frameworks = []
        if "compliance_frameworks" in data:
            compliance_frameworks = [ComplianceFramework(f) for f in data["compliance_frameworks"]]

        return cls(
            is_pii=data.get("is_pii", False),
            pii_type=pii_type,
            sensitivity=sensitivity,
            requires_masking=data.get("requires_masking", False),
            masking_strategy=masking_strategy,
            requires_encryption=data.get("requires_encryption", False),
            requires_consent=data.get("requires_consent", False),
            retention_days=data.get("retention_days"),
            purpose_limitation=data.get("purpose_limitation", []),
            compliance_frameworks=compliance_frameworks,
            custom_properties=data.get("custom_properties", {}),
        )


@dataclass
class PrivacyViolation:
    """Represents a privacy rule violation."""

    field: str
    """Field that has the violation."""

    violation_type: str
    """Type of violation (e.g., 'undeclared_pii', 'missing_masking')."""

    message: str
    """Human-readable description of the violation."""

    severity: ErrorSeverity
    """Severity level of the violation."""

    detected_pii_type: PIIType | None = None
    """PII type that was detected (for undeclared PII)."""

    sample_values: list[Any] = field(default_factory=list)  # type: ignore[operator]
    """Sample values that triggered the violation."""

    recommendation: str | None = None
    """Recommended action to resolve the violation."""

    compliance_impact: list[ComplianceFramework] = field(default_factory=list)  # type: ignore[operator]
    """Compliance frameworks affected by this violation."""

    details: dict[str, Any] = field(default_factory=dict)  # type: ignore[operator]
    """Additional violation details."""


PII_EMAIL = PrivacyInfo(
    is_pii=True,
    pii_type=PIIType.EMAIL,
    sensitivity=Sensitivity.CONFIDENTIAL,
    requires_masking=True,
    masking_strategy=MaskingStrategy.PARTIAL,
)
PII_PHONE = PrivacyInfo(
    is_pii=True,
    pii_type=PIIType.PHONE,
    sensitivity=Sensitivity.CONFIDENTIAL,
    requires_masking=True,
    masking_strategy=MaskingStrategy.PARTIAL,
)
PII_NATIONAL_ID = PrivacyInfo(
    is_pii=True,
    pii_type=PIIType.NATIONAL_ID,
    sensitivity=Sensitivity.RESTRICTED,
    requires_masking=True,
    masking_strategy=MaskingStrategy.HASH,
    requires_encryption=True,
    compliance_frameworks=[ComplianceFramework.KENYA_DPA],
)
PII_CREDIT_CARD = PrivacyInfo(
    is_pii=True,
    pii_type=PIIType.CREDIT_CARD,
    sensitivity=Sensitivity.RESTRICTED,
    requires_masking=True,
    masking_strategy=MaskingStrategy.PARTIAL,
    requires_encryption=True,
    compliance_frameworks=[ComplianceFramework.PCI_DSS],
)
NON_PII_INTERNAL = PrivacyInfo(
    is_pii=False,
    sensitivity=Sensitivity.INTERNAL,
)
NON_PII_PUBLIC = PrivacyInfo(
    is_pii=False,
    sensitivity=Sensitivity.PUBLIC,
)
