"""
Property model for schema columns/fields.

Properties represent individual columns or fields in a schema,
with unified constraints covering data integrity, partitioning, and privacy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from .enums import Cardinality, LogicalType, MaskingStrategy, PIIType, RelationshipType, Sensitivity


@dataclass
class PropertyConstraints:
    """
    Unified constraints for a property.

    Covers data integrity, partitioning, and privacy constraints.
    """

    # Data Integrity
    primary_key: bool = False
    required: bool = False
    unique: bool = False
    nullable: bool = True

    # Partitioning
    partitioned: bool = False
    partition_key_position: Optional[int] = None

    # Privacy (first-class)
    is_pii: bool = False
    pii_type: Optional[PIIType] = None
    masking_strategy: Optional[MaskingStrategy] = None
    sensitivity: Optional[Sensitivity] = None


@dataclass
class Relationship:
    """
    Defines a relationship from this property to another property/table.

    Used for foreign keys and other references between schemas.
    """

    to: str  # Format: "schema_name/column_name" e.g., "hr.departments/department_id"
    type: RelationshipType
    cardinality: Cardinality
    description: str = ""


@dataclass
class Property:
    """
    A property (column/field) in a schema.

    Properties are the building blocks of schemas, representing
    individual data elements with their types, constraints, and relationships.

    Attributes:
        id: Unique identifier for this property (e.g., "COL-001")
        name: The property name (e.g., "employee_id")
        logical_type: The logical data type (string, integer, etc.)
        physical_type: The physical type in the database (e.g., "VARCHAR(36)")
        description: Human-readable description of this property
        ordinal_position: Position of this column in the schema (0-indexed)
        constraints: Data integrity, partitioning, and privacy constraints
        relationships: Relationships to other properties/tables
    """

    id: str
    name: str
    logical_type: LogicalType
    physical_type: str
    description: str
    ordinal_position: int = 0
    constraints: PropertyConstraints = field(default_factory=PropertyConstraints)
    relationships: List[Relationship] = field(default_factory=list)
