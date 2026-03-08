"""
Griot Schema - Schema Definition within a Contract

This module defines the Schema class, which represents a single
data schema within a Contract following the Open Data Contract Standard (ODCS).

A Schema contains:
- Schema-level metadata (id, name, logical_type, physical_type, etc.)
- Properties (fields) as Field objects
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Callable, ClassVar, TypeVar

from griot_core import QualityRule
from griot_core._utils import (
    extract_base_type,
    is_optional_type,
    logical_type_to_python,
    python_type_to_logical,
)
from griot_core.types import DataType, PIIType, PrivacyInfo, Sensitivity
from griot_core.types import PrivacyInfo as PI

__all__ = [
    "Schema",
    "Field",
    "FieldInfo",
]

T = TypeVar("T")


# =============================================================================
# FieldInfo - Schema Field Metadata
# =============================================================================


@dataclass
class FieldInfo:
    """
    Field information for a schema property - ODCS property structure.

    This represents a single field/column within a schema, containing
    all ODCS-compliant field metadata.

    Attributes:
        name: Field name (required)
        id: Unique field identifier
        logical_type: ODCS logical type (string, integer, etc.)
        physical_type: Database/storage type (VARCHAR(36), INT, etc.)
        python_type: Python type for validation
        description: Field description
        primary_key: Whether this field is the primary key
        partitioned: Whether field is used for partitioning
        partition_key_position: Position in partition key
        required: Whether field is required
        unique: Whether field values must be unique
        nullable: Whether field can contain null values
        critical_data_element: Whether field is critical
        relationships: Foreign key and other relationships
        tags: Classification tags
        custom_properties: Extension properties (constraints, semantic, privacy)
        authoritative_definitions: Reference definitions
        quality: Field-level quality rules
        default: Default value
        has_default: Whether a default was specified
        default_factory: Factory for default values
    """

    # Required
    name: str
    logical_type: str = "string"

    # For internal Python use
    python_type: type = str

    # Core info
    id: str | None = None
    physical_type: str | None = None
    description: str = ""

    # Field flags
    primary_key: bool = False
    partitioned: bool = False
    partition_key_position: int | None = None
    required: bool = False
    unique: bool = False
    nullable: bool = True
    critical_data_element: bool = False

    # Relationships
    relationships: list[dict[str, Any]] = dataclass_field(default_factory=list)
    tags: list[str] = dataclass_field(default_factory=list)

    # Custom properties (contains constraints, semantic, privacy)
    custom_properties: dict[str, Any] = dataclass_field(default_factory=dict)

    # Authoritative definitions
    authoritative_definitions: list[dict[str, Any]] = dataclass_field(default_factory=list)

    # Quality rules specific to this field
    quality: list[dict[str, Any]] = dataclass_field(default_factory=list)

    # Default values
    default: Any = None
    has_default: bool = False
    default_factory: Callable[[], Any] | None = None

    # Privacy metadata (stored in custom_properties.privacy)
    _privacy: Any = None  # PrivacyInfo or None

    @property
    def privacy(self) -> "PrivacyInfo | None":
        """Get privacy metadata for this field.

        Returns:
            PrivacyInfo instance if privacy is defined, None otherwise.
        """
        # First check if directly set
        if self._privacy is not None:
            return self._privacy

        # Check custom_properties
        privacy_data = self.custom_properties.get("privacy")
        if privacy_data is None:
            return None

        # Import here to avoid circular imports

        if isinstance(privacy_data, PI):
            return privacy_data
        elif isinstance(privacy_data, dict):
            return PI.from_dict(privacy_data)

        return None

    @privacy.setter
    def privacy(self, value: "PrivacyInfo | None") -> None:
        """Set privacy metadata for this field."""
        self._privacy = value
        if value is not None:
            self.custom_properties["privacy"] = value.to_dict()
        elif "privacy" in self.custom_properties:
            del self.custom_properties["privacy"]

    @property
    def is_pii(self) -> bool:
        """Check if this field contains PII."""
        privacy = self.privacy
        return privacy.is_pii if privacy else False

    @property
    def sensitivity(self) -> str | None:
        """Get sensitivity level for this field."""
        privacy = self.privacy
        return privacy.sensitivity.value if privacy else None

    @property
    def type(self) -> DataType:
        """Get DataType from logical_type."""
        type_map = {
            "string": DataType.STRING,
            "integer": DataType.INTEGER,
            "float": DataType.FLOAT,
            "boolean": DataType.BOOLEAN,
            "date": DataType.DATE,
            "datetime": DataType.DATETIME,
            "array": DataType.ARRAY,
            "object": DataType.OBJECT,
        }
        return type_map.get(self.logical_type.lower(), DataType.STRING)

    def to_dict(self) -> dict[str, Any]:
        """Convert to ODCS property format (camelCase)."""
        result: dict[str, Any] = {"name": self.name}

        if self.id:
            result["id"] = self.id
        result["logicalType"] = self.logical_type
        if self.physical_type:
            result["physicalType"] = self.physical_type
        if self.description:
            result["description"] = self.description

        if self.primary_key:
            result["primary_key"] = True
        if self.partitioned:
            result["partitioned"] = True
        if self.partition_key_position is not None:
            result["partitionKeyPosition"] = self.partition_key_position
        if self.required:
            result["required"] = True
        if self.unique:
            result["unique"] = True
        result["nullable"] = self.nullable
        if self.critical_data_element:
            result["criticalDataElement"] = True

        if self.relationships:
            result["relationships"] = self.relationships
        if self.tags:
            result["tags"] = self.tags
        if self.custom_properties:
            result["customProperties"] = self.custom_properties
        if self.authoritative_definitions:
            result["authoritativeDefinitions"] = self.authoritative_definitions
        if self.quality:
            result["quality"] = self.quality

        return result

    def get_dq_checks(self) -> list[dict[str, Any]]:
        """Return quality attributes from custom_properties."""
        return self.quality

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FieldInfo:
        """Create from dictionary.

        Handles both ODCS format and registry format:
        - ODCS: primary_key, required, unique, nullable
        - Registry: is_primary_key, is_required, is_unique, is_nullable, is_pii
        """
        logical_type = data.get("logical_type") or data.get("logicalType", "string")

        # Build custom_properties, preserving PII / privacy / constraint metadata
        custom_props = data.get("custom_properties", {}) or data.get("customProperties", {})
        # Merge registry-format fields into custom_properties.privacy
        is_pii = data.get("is_pii", False)
        pii_type = data.get("pii_type")
        masking = data.get("masking_strategy")
        sensitivity = data.get("sensitivity")
        if is_pii or pii_type or masking or sensitivity:
            custom_props.setdefault("privacy", {})
            custom_props["privacy"]["is_pii"] = is_pii
            if pii_type is not None:
                custom_props["privacy"]["pii_type"] = pii_type
            if masking is not None:
                custom_props["privacy"]["masking_strategy"] = masking
            if sensitivity is not None:
                custom_props["privacy"]["sensitivity"] = sensitivity
        # Preserve field-level constraints
        constraints = data.get("constraints")
        if constraints:
            custom_props["constraints"] = constraints

        return cls(
            name=data.get("name", ""),
            id=data.get("id") or data.get("property_id"),
            logical_type=logical_type,
            python_type=logical_type_to_python(logical_type),
            physical_type=data.get("physical_type") or data.get("physicalType"),
            description=data.get("description", ""),
            # Handle both ODCS (primary_key) and registry (is_primary_key) formats
            primary_key=data.get("primary_key", False) or data.get("is_primary_key", False),
            partitioned=data.get("partitioned", False) or data.get("is_partitioned", False),
            partition_key_position=data.get("partition_key_position")
            or data.get("partitionKeyPosition"),
            required=data.get("required", False) or data.get("is_required", False),
            unique=data.get("unique", False) or data.get("is_unique", False),
            nullable=data.get("nullable", True)
            if "nullable" in data
            else data.get("is_nullable", True),
            critical_data_element=data.get("critical_data_element", False)
            or data.get("criticalDataElement", False),
            relationships=data.get("relationships", []),
            tags=data.get("tags", []),
            custom_properties=custom_props,
            authoritative_definitions=data.get("authoritative_definitions", [])
            or data.get("authoritativeDefinitions", []),
            quality=data.get("quality", []),
        )


# =============================================================================
# Field - Field Descriptor
# =============================================================================


class Field:
    """
    Define a schema field with ODCS-compliant metadata.

    Use as a descriptor in Schema subclasses to define
    fields with validation constraints and metadata.

    Example:
        class EmployeeSchema(Schema):
            id: str = Field(
                description="Primary identifier",
                primary_key=True,
                logical_type="string",
                physical_type="VARCHAR(36)",
            )
    """

    __slots__ = (
        "description",
        "default",
        "default_factory",
        "logical_type",
        "physical_type",
        "primary_key",
        "partitioned",
        "partition_key_position",
        "required",
        "unique",
        "nullable",
        "critical_data_element",
        "relationships",
        "tags",
        "custom_properties",
        "authoritative_definitions",
        "quality",
        "privacy",
        "_name",
        "_has_default",
    )

    def __init__(
        self,
        description: str,
        *,
        default: Any = ...,
        default_factory: Callable[[], Any] | None = None,
        logical_type: str | None = None,
        physical_type: str | None = None,
        primary_key: bool = False,
        partitioned: bool = False,
        partition_key_position: int | None = None,
        required: bool = False,
        unique: bool = False,
        nullable: bool = True,
        critical_data_element: bool = False,
        is_pii: bool = False,
        pii_type: PIIType | None = None,
        sensitivity: Sensitivity = Sensitivity.INTERNAL,
        requires_masking: bool = False,
        relationships: list[dict[str, Any]] | None = None,
        tags: list[str] | None = None,
        custom_properties: dict[str, Any] | None = None,
        authoritative_definitions: list[dict[str, Any]] | None = None,
        quality: list[dict[str, Any]] | None = None,
        privacy: "PrivacyInfo | None" = None,
    ) -> None:
        """
        Initialize a schema field.

        Args:
            description: Human-readable field description (required)
            default: Default value for this field
            default_factory: Factory function for default value
            logical_type: ODCS logical type (string, integer, etc.)
            physical_type: Physical storage type (VARCHAR, INT, etc.)
            primary_key: Whether this is the primary key
            partitioned: Whether used for partitioning
            partition_key_position: Position in partition key
            required: Whether field is required
            unique: Whether values must be unique
            nullable: Whether null values are allowed
            critical_data_element: Whether this is a critical element
            relationships: Foreign key and other relationships
            tags: Classification tags
            custom_properties: Extension properties
            authoritative_definitions: Reference definitions
            quality: Field-level quality rules
            privacy: Privacy metadata (PrivacyInfo) for data protection
        """
        self.description = description
        self.default = default
        self.default_factory = default_factory
        self._has_default = default is not ...
        self._name: str | None = None

        self.logical_type = logical_type
        self.physical_type = physical_type
        self.primary_key = primary_key
        self.partitioned = partitioned
        self.partition_key_position = partition_key_position
        self.required = required
        self.unique = unique
        self.nullable = nullable
        self.critical_data_element = critical_data_element
        self.relationships = relationships or []
        self.tags = tags or []
        self.custom_properties = custom_properties or {}
        self.authoritative_definitions = authoritative_definitions or []
        self.quality = quality or []
        self.privacy = privacy

        # Auto add quality rules
        if quality is not None:
            self.quality = quality
        else:
            self._auto_add_quality_rules()

        # Store privacy in custom_properties for ODCS serialization
        if privacy is not None:
            self.custom_properties["privacy"] = privacy.to_dict()
        else:
            self._check_pii_settings(sensitivity, requires_masking, is_pii, pii_type)

        if self._has_default and self.default_factory is not None:
            raise ValueError("Cannot specify both 'default' and 'default_factory'")

    def _auto_add_quality_rules(self) -> None:
        """Automatically add quality rules based on field properties."""

        if self.primary_key:
            self.quality.extend([QualityRule.duplicate_rows(must_be=0)])
            self.quality.extend([QualityRule.null_values(must_be=0)])
        if self.unique:
            if QualityRule.duplicate_rows(must_be=0) not in self.quality:
                self.quality.extend([QualityRule.duplicate_rows(must_be=0)])
        if self.required:
            if QualityRule.null_values(must_be=0) not in self.quality:
                self.quality.extend([QualityRule.null_values(must_be=0)])

    def _check_pii_settings(self, sensitivity, requires_masking, is_pii, pii_type) -> None:
        """Check and set privacy settings if any PII-related args are provided."""

        if is_pii and pii_type is None:
            raise ValueError("'pii_type' must be specified if 'is_pii' is True")
        privacy_args = [sensitivity, requires_masking, is_pii, pii_type]
        if any(arg is not None for arg in privacy_args):
            privacy = PrivacyInfo(
                is_pii=is_pii,
                pii_type=pii_type,
                sensitivity=Sensitivity.RESTRICTED if is_pii else sensitivity,
                requires_masking=requires_masking,
            )
            self.custom_properties["privacy"] = privacy.to_dict()

    def __set_name__(self, owner: type, name: str) -> None:
        """Set field name when used as descriptor."""
        self._name = name

    def get_default(self) -> Any:
        """Get the default value for this field."""
        if self.default_factory is not None:
            return self.default_factory()
        if self._has_default:
            return self.default
        return None

    def has_default(self) -> bool:
        """Check if this field has a default value."""
        return self._has_default or self.default_factory is not None

    def to_field_info(self, name: str, py_type: type) -> FieldInfo:
        """
        Convert to FieldInfo.

        Args:
            name: Field name
            py_type: Python type hint

        Returns:
            FieldInfo instance
        """
        logical_type = self.logical_type
        if logical_type is None:
            logical_type = python_type_to_logical(py_type)

        field_info = FieldInfo(
            name=name,
            logical_type=logical_type,
            python_type=py_type,
            physical_type=self.physical_type,
            description=self.description,
            primary_key=self.primary_key,
            partitioned=self.partitioned,
            partition_key_position=self.partition_key_position,
            required=self.required,
            unique=self.unique,
            nullable=self.nullable,
            critical_data_element=self.critical_data_element,
            relationships=self.relationships,
            tags=self.tags,
            custom_properties=self.custom_properties,
            authoritative_definitions=self.authoritative_definitions,
            quality=self.quality,
            default=self.default if self._has_default else None,
            has_default=self._has_default,
            default_factory=self.default_factory,
        )

        # Set privacy directly (not just through custom_properties)
        if self.privacy is not None:
            field_info._privacy = self.privacy

        return field_info

    def __repr__(self) -> str:
        """String representation."""
        parts = [f"description={self.description!r}"]
        if self.primary_key:
            parts.append("primary_key=True")
        if self.logical_type:
            parts.append(f"logical_type={self.logical_type!r}")
        return f"Field({', '.join(parts)})"


# =============================================================================
# Schema - Schema Definition Class
# =============================================================================


class SchemaMeta(type):
    """Metaclass for Schema that processes field definitions."""

    # Schema-level attributes that should NOT be treated as data fields
    _SCHEMA_METADATA_ATTRS = frozenset(
        {
            "id",
            "name",
            "logical_type",
            "physical_type",
            "physical_name",
            "description",
            "business_name",
            "authoritative_definitions",
            "quality",
            "tags",
        }
    )

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> SchemaMeta:
        cls = super().__new__(mcs, name, bases, namespace)

        # Skip processing for the base class
        if name == "Schema" and not any(hasattr(b, "_schema_fields") for b in bases):
            return cls

        fields: dict[str, FieldInfo] = {}

        # Inherit fields from base classes (but not from Schema base class)
        for base in reversed(bases):
            if hasattr(base, "_schema_fields") and base.__name__ != "Schema":
                fields.update(base._schema_fields)

        # Get type hints only from THIS class, not inherited
        own_hints = getattr(cls, "__annotations__", {})

        # Process fields
        for field_name, field_type in own_hints.items():
            # Skip private attributes
            if field_name.startswith("_"):
                continue

            field_def = namespace.get(field_name)

            # If explicitly defined as Field(), always include it (overrides metadata attrs)
            if isinstance(field_def, Field):
                py_type = extract_base_type(field_type)
                field_info = field_def.to_field_info(field_name, py_type)
                fields[field_name] = field_info
            elif field_name in mcs._SCHEMA_METADATA_ATTRS:
                # Skip schema-level metadata attributes unless explicitly defined as Field
                continue
            elif field_def is None or not isinstance(
                field_def, (classmethod, staticmethod, property)
            ):
                if field_name not in fields:
                    py_type = extract_base_type(field_type)
                    fields[field_name] = FieldInfo(
                        name=field_name,
                        logical_type=python_type_to_logical(py_type),
                        python_type=py_type,
                        description="",
                        nullable=is_optional_type(field_type),
                        has_default=field_def is not None,
                        default=field_def,
                    )

        cls._schema_fields = fields  # type: ignore[attr-defined]
        cls._schema_field_names = tuple(fields.keys())  # type: ignore[attr-defined]

        # Find primary key
        cls._schema_primary_key = None  # type: ignore[attr-defined]
        for fname, finfo in fields.items():
            if finfo.primary_key:
                cls._schema_primary_key = fname  # type: ignore[attr-defined]
                break

        return cls


class Schema(metaclass=SchemaMeta):
    """
    Schema definition within a contract.

    This class represents a single schema (table/view/file) containing
    field definitions. It is schema-level only and does NOT contain
    contract-level metadata (api_version, kind, status, etc.).

    Schema-level attributes:
        - id: Unique schema identifier
        - name: Schema name
        - logical_type: ODCS logical type (typically "object")
        - physical_type: Physical type (table, view, file)
        - physical_name: Actual name in storage system
        - description: Schema description
        - business_name: Business-friendly name
        - authoritative_definitions: Reference definitions
        - quality: Schema-level quality rules
        - tags: Classification tags

    Field attributes (via metaclass):
        - _schema_fields: Dict of field name -> FieldInfo
        - _schema_field_names: Tuple of field names
        - _schema_primary_key: Primary key field name

    Example:
        class EmployeeSchema(Schema):
            _name = "Employees"
            _physical_name = "employees_tbl"

            id: str = Field(
                description="Primary identifier",
                primary_key=True,
            )
            name: str = Field(description="Employee name")
    """

    # Schema-level metadata (class vars)
    _id: ClassVar[str | None] = None
    _name: ClassVar[str] = ""
    _logical_type: ClassVar[str] = "object"
    _physical_type: ClassVar[str | None] = None
    _physical_name: ClassVar[str | None] = None
    _description: ClassVar[str] = ""
    _business_name: ClassVar[str | None] = None
    _authoritative_definitions: ClassVar[list[dict[str, Any]]] = []
    _quality: ClassVar[list[dict[str, Any]]] = []
    _tags: ClassVar[list[str]] = []

    # Field metadata (set by metaclass)
    _schema_fields: ClassVar[dict[str, FieldInfo]] = {}
    _schema_field_names: ClassVar[tuple[str, ...]] = ()
    _schema_primary_key: ClassVar[str | None] = None

    # Note: Instance attributes are initialized in __init__, not as class variables
    # to avoid mutable default argument issues. These type hints are for documentation.

    def __init__(
        self,
        *,
        id: str,
        name: str,
        logical_type: str = "object",
        physical_type: str | None = None,
        physical_name: str | None = None,
        description: str = "",
        business_name: str | None = None,
        authoritative_definitions: list[dict[str, Any]] | None = None,
        quality: list[dict[str, Any]] | None = None,
        tags: list[str] | None = None,
        properties: list[FieldInfo | dict[str, Any]] | None = None,
    ) -> None:
        """
        Initialize a schema definition.

        Args:
            id: Unique schema identifier
            name: Schema name
            logical_type: ODCS logical type
            physical_type: Physical storage type
            physical_name: Actual storage name
            description: Schema description
            business_name: Business-friendly name
            authoritative_definitions: Reference definitions
            quality: Schema-level quality rules
            tags: Classification tags
            properties: Field definitions
        """
        self.id = id
        self.name = name or self.__class__.__name__
        self.logical_type = logical_type
        self.physical_type = physical_type
        self.physical_name = physical_name
        self.description = description
        self.business_name = business_name
        self.authoritative_definitions = authoritative_definitions or []
        self.quality = quality or []
        self.tags = tags or []

        # Instance-level fields (for data-driven schemas)
        self._instance_fields: dict[str, FieldInfo] = {}
        if properties:
            for prop in properties:
                if isinstance(prop, FieldInfo):
                    self._instance_fields[prop.name] = prop
                elif isinstance(prop, dict):
                    field_info = FieldInfo.from_dict(prop)
                    self._instance_fields[field_info.name] = field_info

    # -------------------------------------------------------------------------
    # Field Access
    # -------------------------------------------------------------------------

    @property
    def fields(self) -> dict[str, FieldInfo]:
        """Get all fields (class-defined and instance)."""
        result = dict(self._schema_fields)
        result.update(self._instance_fields)
        return result

    @classmethod
    def list_fields(cls) -> list[FieldInfo]:
        """List all class-defined fields."""
        return list(cls._schema_fields.values())

    @classmethod
    def get_field(cls, name: str) -> FieldInfo | None:
        """Get a class-defined field by name."""
        return cls._schema_fields.get(name)

    @classmethod
    def get_primary_key(cls) -> str | None:
        """Get the primary key field name."""
        return cls._schema_primary_key

    @classmethod
    def field_names(cls) -> tuple[str, ...]:
        """Get tuple of class-defined field names."""
        return cls._schema_field_names

    def get_instance_field(self, name: str) -> FieldInfo | None:
        """Get a field by name (instance or class)."""
        return self.fields.get(name)

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to ODCS schema format (camelCase).

        Returns:
            Dictionary in ODCS format
        """
        result: dict[str, Any] = {"name": self.name}

        if self.id:
            result["id"] = self.id
        if self.logical_type:
            result["logicalType"] = self.logical_type
        if self.physical_type:
            result["physicalType"] = self.physical_type
        if self.physical_name:
            result["physicalName"] = self.physical_name
        if self.description:
            result["description"] = self.description
        if self.business_name:
            result["businessName"] = self.business_name
        if self.authoritative_definitions:
            result["authoritativeDefinitions"] = self.authoritative_definitions

        # Properties (fields)
        fields = self.fields
        if fields:
            result["properties"] = [f.to_dict() for f in fields.values()]

        if self.quality:
            result["quality"] = self.quality
        if self.tags:
            result["tags"] = self.tags

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Schema:
        """
        Create Schema from dictionary.

        Args:
            data: Dictionary in ODCS schema format

        Returns:
            Schema instance
        """
        from griot_core.contract import normalize_keys

        # Normalize to snake_case
        data = normalize_keys(data, to_format="snake")

        # Parse properties
        properties: list[FieldInfo] = []
        for prop_data in data.get("properties", []):
            properties.append(FieldInfo.from_dict(prop_data))

        # Accept both "quality" and "checks" as the schema-level checks list
        quality = data.get("quality", []) or data.get("checks", [])

        return cls(
            id=data.get("id"),  # type: ignore[arg-type]
            name=data.get("name", ""),
            logical_type=data.get("logical_type", "object"),
            physical_type=data.get("physical_type"),
            physical_name=data.get("physical_name"),
            description=data.get("description", ""),
            business_name=data.get("business_name"),
            authoritative_definitions=data.get("authoritative_definitions", []),
            quality=quality,
            tags=data.get("tags", []),
            properties=properties,  # type: ignore[arg-type]
        )

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def __repr__(self) -> str:
        """String representation."""
        return f"<Schema name={self.name!r} fields={len(self.fields)}>"

    def __str__(self) -> str:
        """Human-readable string."""
        return f"Schema({self.name}, {len(self.fields)} fields)"
