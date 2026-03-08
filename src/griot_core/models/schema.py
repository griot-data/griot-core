"""
Schema model - First-class entity representing a data asset.

Schemas represent data assets (tables, views, files) and exist independently
of contracts. Multiple contracts can reference the same schema.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from .enums import SchemaStatus

if TYPE_CHECKING:
    from .check import Check
    from .property import Property


@dataclass
class Schema:
    """
    A standalone schema representing a data asset (table, view, file).

    Schemas are first-class citizens in Griot. They exist independently
    of contracts and can be referenced by multiple contracts. This enables:
    - One schema definition, multiple contracts referencing it
    - Schema versioning independent of contract versioning
    - A browsable schema catalog
    - Clear ownership: Data Engineers own schemas, Governance owns contracts

    Attributes:
        id: Unique identifier for this schema (e.g., "sch-employees-001")
        name: Human-readable name (e.g., "employees")
        version: Semantic version (e.g., "1.0.0")
        status: Lifecycle status (draft, pending_review, active, deprecated)
        physical_name: The physical name in the data platform (e.g., "hr.employees_tbl")
        description: Human-readable description of this schema
        owner_team: The team responsible for this schema (e.g., "data-platform-team")
        properties: List of properties (columns) in this schema
        checks: Optional schema-level validation checks
        tags: Tags for categorization and discovery
        created_at: When this schema was created
        updated_at: When this schema was last modified
    """

    # Identity
    id: str
    name: str
    version: str
    status: SchemaStatus

    # Physical mapping
    physical_name: str
    description: str

    # Ownership
    owner_team: str

    # Properties (columns)
    properties: List["Property"] = field(default_factory=list)

    # Schema-level checks (optional)
    checks: List["Check"] = field(default_factory=list)

    # Metadata
    tags: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Registry metadata — populated when schema was pulled from the registry
    registry_ref: Optional[Dict[str, Any]] = None
