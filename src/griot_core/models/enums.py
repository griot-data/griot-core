"""
Enums for the Griot data contract system.

This module defines all enumeration types used throughout griot-core,
including statuses, types, severities, and compliance frameworks.
"""

from __future__ import annotations

from enum import Enum


class SchemaStatus(str, Enum):
    """Status of a schema in its lifecycle."""

    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    ACTIVE = "active"
    DEPRECATED = "deprecated"


class ContractStatus(str, Enum):
    """Status of a data contract in its lifecycle."""

    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"
    ARCHIVED = "archived"  # Kept for backwards compatibility


class LogicalType(str, Enum):
    """Logical data types for schema properties."""

    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    OBJECT = "object"


class PIIType(str, Enum):
    """Types of Personally Identifiable Information."""

    NAME = "name"
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    NATIONAL_ID = "national_id"
    TAX_ID = "tax_id"
    IBAN = "iban"
    CREDIT_CARD = "credit_card"
    ADDRESS = "address"
    FINANCIAL = "financial"
    HEALTH = "health"
    BIOMETRIC = "biometric"
    IP_ADDRESS = "ip_address"
    MAC_ADDRESS = "mac_address"
    DATE_OF_BIRTH = "date_of_birth"
    PASSPORT = "passport"


class CheckType(str, Enum):
    """Types of validation checks."""

    DATA_QUALITY = "data_quality"
    PRIVACY = "privacy"
    SCHEMA = "schema"


class CheckCategory(str, Enum):
    """Specific check categories that map to dbt test types."""

    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    RELATIONSHIPS = "relationships"
    RANGE = "range"
    PATTERN = "pattern"
    STRING_LENGTH = "string_length"
    ROW_COUNT = "row_count"
    FRESHNESS = "freshness"
    CUSTOM = "custom"


class Severity(str, Enum):
    """Severity levels for checks and alerts."""

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class Runtime(str, Enum):
    """Executor runtime types."""

    WASM = "wasm"
    CONTAINER = "container"


class RelationshipType(str, Enum):
    """Types of relationships between properties/tables."""

    FOREIGN_KEY = "foreign_key"
    REFERENCES = "references"


class Cardinality(str, Enum):
    """Cardinality of relationships."""

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class MaskingStrategy(str, Enum):
    """Strategies for masking sensitive data."""

    NONE = "none"
    REDACT = "redact"
    HASH = "hash"
    PARTIAL = "partial"
    ASTERISK = "asterisk"
    NULL = "null"
    TOKENIZE = "tokenize"


class Sensitivity(str, Enum):
    """Data sensitivity classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class ComplianceFramework(str, Enum):
    """Regulatory compliance frameworks."""

    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    KENYA_DPA = "kenya_dpa"
    SOX = "sox"
