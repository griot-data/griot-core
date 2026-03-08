"""
Griot Core Contract

Data contract definition and manipulation based on the Open Data Contract Standard (ODCS).
A GriotContract contains contract-level metadata and one or more schema definitions.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

import yaml

from griot_core.exceptions import ContractNotFoundError, ContractParseError
from griot_core.models.schema_ref import SchemaRef
from griot_core.types import ContractStatus, Severity

if TYPE_CHECKING:
    from griot_core.schema import Schema

__all__ = [
    # Contract class
    "Contract",
    # Schema reference
    "SchemaRef",
    # Contract-level dataclasses
    "ContractDescription",
    "ContractTeam",
    "TeamMember",
    "ContractSupport",
    "ContractRole",
    "SLAProperty",
    "Server",
    # Loading functions
    "load_contract",
    "load_contract_from_string",
    "load_contract_from_dict",
    # Export functions
    "contract_to_yaml",
    "contract_to_dict",
    # Linting
    "lint_contract",
    "LintIssue",
    # Contract structure validation
    "validate_contract_structure",
    "ContractStructureResult",
    "ContractStructureIssue",
    # Key normalization
    "normalize_keys",
    "to_snake_case",
    "to_camel_case",
    # Constants
    "CONTRACT_FIELD_TYPES",
    "ODCS_MANDATORY_FIELDS",
]


# =============================================================================
# ODCS Key Normalization
# =============================================================================

_ODCS_KEY_MAP = {
    "apiVersion": "api_version",
    "dataProduct": "data_product",
    "authoritativeDefinitions": "authoritative_definitions",
    "slaProperties": "sla_properties",
    "contractCreatedTs": "contract_created_ts",
    "logicalType": "logical_type",
    "physicalType": "physical_type",
    "physicalName": "physical_name",
    "businessName": "business_name",
    "customProperties": "custom_properties",
    "partitionKeyPosition": "partition_key_position",
    "criticalDataElement": "critical_data_element",
}

_ODCS_KEY_MAP_REVERSE = {v: k for k, v in _ODCS_KEY_MAP.items()}


def to_snake_case(s: str) -> str:
    """Convert camelCase to snake_case."""
    if s in _ODCS_KEY_MAP:
        return _ODCS_KEY_MAP[s]
    result = []
    for i, c in enumerate(s):
        if c.isupper() and i > 0:
            result.append("_")
        result.append(c.lower())
    return "".join(result)


def to_camel_case(s: str) -> str:
    """Convert snake_case to camelCase."""
    if s in _ODCS_KEY_MAP_REVERSE:
        return _ODCS_KEY_MAP_REVERSE[s]
    parts = s.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def normalize_keys(data: dict[str, Any], to_format: str = "snake") -> dict[str, Any]:
    """Normalize keys between camelCase and snake_case recursively."""
    converter = to_snake_case if to_format == "snake" else to_camel_case
    result: dict[str, Any] = {}

    for key, value in data.items():
        new_key = converter(key)
        if isinstance(value, dict):
            result[new_key] = normalize_keys(value, to_format)
        elif isinstance(value, list):
            result[new_key] = [
                normalize_keys(item, to_format) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            result[new_key] = value

    return result


# =============================================================================
# ODCS Contract Schema Constants
# =============================================================================

CONTRACT_FIELD_TYPES: dict[str, type] = {
    "api_version": str,
    "kind": str,
    "id": str,
    "version": str,
    "status": str,
    "schema": list,
    "custom_properties": dict,
    "name": str,
    "data_product": str,
    "authoritative_definitions": list,
    "description": dict,
    "tags": list,
    "support": list,
    "team": dict,
    "roles": list,
    "sla_properties": list,
    "servers": list,
    "contract_created_ts": str,
}

ODCS_MANDATORY_FIELDS = frozenset(
    {
        "api_version",
        "kind",
        "id",
        "version",
        "status",
        "schema",
        "custom_properties",
    }
)


# =============================================================================
# Contract-Level Dataclasses
# =============================================================================


@dataclass
class ContractDescription:
    """Contract description metadata (ODCS description structure)."""

    purpose: str = ""
    usage: str = ""
    limitations: str = ""
    logical_type: str = "string"

    def to_dict(self) -> dict[str, Any]:
        """Convert to ODCS dictionary format."""
        result: dict[str, Any] = {}
        if self.purpose:
            result["purpose"] = self.purpose
        if self.usage:
            result["usage"] = self.usage
        if self.limitations:
            result["limitations"] = self.limitations
        # Only include logical_type if not the default
        if self.logical_type and self.logical_type != "string":
            result["logicalType"] = self.logical_type
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any] | str) -> ContractDescription:
        """Create from dictionary or string."""
        if isinstance(data, str):
            return cls(purpose=data)
        return cls(
            purpose=data.get("purpose", ""),
            usage=data.get("usage", ""),
            limitations=data.get("limitations", ""),
            logical_type=data.get("logical_type", "string"),
        )


@dataclass
class TeamMember:
    """Team member definition (ODCS team.members structure)."""

    username: str
    role: str = ""
    custom_properties: list[dict[str, Any]] = dataclass_field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to ODCS dictionary format."""
        result: dict[str, Any] = {"username": self.username}
        if self.role:
            result["role"] = self.role
        if self.custom_properties:
            result["customProperties"] = self.custom_properties
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TeamMember:
        """Create from dictionary."""
        return cls(
            username=data.get("username", ""),
            role=data.get("role", ""),
            custom_properties=data.get("custom_properties", []),
        )


@dataclass
class ContractTeam:
    """Contract team definition (ODCS team structure)."""

    id: str = ""
    name: str = ""
    description: str = ""
    members: list[TeamMember] = dataclass_field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to ODCS dictionary format."""
        result: dict[str, Any] = {}
        if self.id:
            result["id"] = self.id
        if self.name:
            result["name"] = self.name
        if self.description:
            result["description"] = self.description
        if self.members:
            result["members"] = [m.to_dict() for m in self.members]
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContractTeam:
        """Create from dictionary."""
        members = [TeamMember.from_dict(m) for m in data.get("members", [])]
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            members=members,
        )


@dataclass
class ContractSupport:
    """Contract support channel definition (ODCS support structure)."""

    id: str = ""
    channel: str = ""
    tool: str = ""
    scope: str = ""
    description: str = ""
    url: str = ""
    custom_properties: list[dict[str, Any]] = dataclass_field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to ODCS dictionary format."""
        result: dict[str, Any] = {}
        if self.id:
            result["id"] = self.id
        if self.channel:
            result["channel"] = self.channel
        if self.tool:
            result["tool"] = self.tool
        if self.scope:
            result["scope"] = self.scope
        if self.description:
            result["description"] = self.description
        if self.url:
            result["url"] = self.url
        if self.custom_properties:
            result["customProperties"] = self.custom_properties
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContractSupport:
        """Create from dictionary."""
        return cls(
            id=data.get("id", ""),
            channel=data.get("channel", ""),
            tool=data.get("tool", ""),
            scope=data.get("scope", ""),
            description=data.get("description", ""),
            url=data.get("url", ""),
            custom_properties=data.get("custom_properties", []),
        )


@dataclass
class ContractRole:
    """Contract role/access definition (ODCS roles structure)."""

    role: str
    access: str = ""
    first_level_approvers: str = ""
    second_level_approvers: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to ODCS dictionary format."""
        result: dict[str, Any] = {"role": self.role}
        if self.access:
            result["access"] = self.access
        if self.first_level_approvers:
            result["firstLevelApprovers"] = self.first_level_approvers
        if self.second_level_approvers:
            result["secondLevelApprovers"] = self.second_level_approvers
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContractRole:
        """Create from dictionary."""
        return cls(
            role=data.get("role", ""),
            access=data.get("access", ""),
            first_level_approvers=data.get("first_level_approvers", ""),
            second_level_approvers=data.get("second_level_approvers", ""),
        )


@dataclass
class SLAProperty:
    """SLA property definition (ODCS slaProperties structure)."""

    id: str = ""
    property: str = ""
    value: str | int | float = ""
    value_ext: str | int | float = ""
    unit: str = ""
    element: str = ""
    driver: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to ODCS dictionary format."""
        result: dict[str, Any] = {}
        if self.id:
            result["id"] = self.id
        if self.property:
            result["property"] = self.property
        if self.value:
            result["value"] = self.value
        if self.value_ext:
            result["valueExt"] = self.value_ext
        if self.unit:
            result["unit"] = self.unit
        if self.element:
            result["element"] = self.element
        if self.driver:
            result["driver"] = self.driver
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SLAProperty:
        """Create from dictionary."""
        return cls(
            id=data.get("id", ""),
            property=data.get("property", ""),
            value=data.get("value", ""),
            value_ext=data.get("value_ext", ""),
            unit=data.get("unit", ""),
            element=data.get("element", ""),
            driver=data.get("driver", ""),
        )


@dataclass
class Server:
    """Server/data source definition (ODCS servers structure)."""

    server: str
    type: str = ""
    description: str = ""
    environment: str = ""
    project: str = ""
    dataset: str = ""
    roles: list[str] = dataclass_field(default_factory=list)
    custom_properties: list[dict[str, Any]] = dataclass_field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to ODCS dictionary format."""
        result: dict[str, Any] = {"server": self.server}
        if self.type:
            result["type"] = self.type
        if self.description:
            result["description"] = self.description
        if self.environment:
            result["environment"] = self.environment
        if self.project:
            result["project"] = self.project
        if self.dataset:
            result["dataset"] = self.dataset
        if self.roles:
            result["roles"] = self.roles
        if self.custom_properties:
            result["customProperties"] = self.custom_properties
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Server:
        """Create from dictionary."""
        return cls(
            server=data.get("server", ""),
            type=data.get("type", ""),
            description=data.get("description", ""),
            environment=data.get("environment", ""),
            project=data.get("project", ""),
            dataset=data.get("dataset", ""),
            roles=data.get("roles", []),
            custom_properties=data.get("custom_properties", []),
        )


# =============================================================================
# Contract Class
# =============================================================================


class Contract:
    """
    Top-level data contract entity following ODCS.

    A Contract contains contract-level metadata and one or more Schema objects.
    This separation allows a single contract to govern multiple related schemas.

    Attributes:
        api_version: ODCS version (e.g., "v1.0.0")
        _kind: Contract type (e.g., "DataContract")
        id: Unique contract identifier
        name: Human-readable contract name
        version: Contract version
        status: Lifecycle status (draft, active, deprecated, retired)
        schemas: List of Schema objects

    Example:
        from griot_core import Contract, Schema, Field

        class Employees(Schema):
            id: str = Field(description="Employee ID", primary_key=True)
            name: str = Field(description="Employee name")

        contract = Contract(
            id="emp-001",
            name="Employee Contract",
            version="1.0.0",
            schemas=[Employees()],
        )
    """

    def __init__(
        self,
        *,
        id: str,
        schemas: list[Schema] | None = None,
        schema_refs: list[SchemaRef] | None = None,
        api_version: str = "v1.0.0",
        name: str = "",
        version: str = "1.0.0",
        status: ContractStatus | str = ContractStatus.DRAFT,
        domain: str = "",
        data_product: str = "",
        description: ContractDescription | dict[str, Any] | str | None = None,
        tags: list[str] | None = None,
        team: ContractTeam | dict[str, Any] | None = None,
        roles: list[ContractRole | dict[str, Any]] | None = None,
        servers: list[Server | dict[str, Any]] | None = None,
        sla_properties: list[SLAProperty | dict[str, Any]] | None = None,
        support: list[ContractSupport | dict[str, Any]] | None = None,
        authoritative_definitions: list[dict[str, Any]] | None = None,
        custom_properties: dict[str, Any] | None = None,
        # NEW: Registry metadata fields (P9-C01)
        uuid: str | None = None,
        extends: str | None = None,
        extends_contract_id: str | None = None,
        is_template: bool = False,
        template_category: str | None = None,
        owner_team_id: str | None = None,
        created_by: str | None = None,
        # NEW: Contract-level checks
        checks: list[dict[str, Any]] | None = None,
        # NEW: Structured configs (registry format)
        compliance: dict[str, Any] | None = None,
        sla: dict[str, Any] | None = None,
        executors: dict[str, Any] | None = None,
        governance: dict[str, Any] | None = None,
        # NEW: Reviewer info
        reviewer_type: str | None = None,
        reviewer_id: str | None = None,
        reviewer_name: str | None = None,
        # NEW: Resolution metadata
        resolved_definition: dict[str, Any] | None = None,
        inheritance_chain: list[str] | None = None,
        yaml_source: str | None = None,
        # Timestamps
        created_at: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        self.api_version = api_version
        self._kind = "DataContract"
        self.id = id
        self.name = name
        self.version = version

        # NEW: Registry metadata
        self.uuid = uuid
        self.extends = extends
        self.extends_contract_id = extends_contract_id
        self.is_template = is_template
        self.template_category = template_category
        self.owner_team_id = owner_team_id
        self.created_by = created_by

        # NEW: Contract-level checks
        self.checks = checks or []

        # NEW: Structured configs
        self.compliance = compliance
        self.sla = sla
        self.executors = executors
        self.governance = governance

        # NEW: Reviewer info
        self.reviewer_type = reviewer_type
        self.reviewer_id = reviewer_id
        self.reviewer_name = reviewer_name

        # NEW: Resolution metadata
        self.resolved_definition = resolved_definition
        self.inheritance_chain = inheritance_chain or []
        self.yaml_source = yaml_source

        # Timestamps
        self.created_at = created_at
        self.updated_at = updated_at

        # Validate: must have at least one schema OR one schema_ref
        if not schemas and not schema_refs:
            raise ValueError("A contract must have at least one schema or schema_ref.")

        if isinstance(status, str):
            try:
                self.status = ContractStatus(status)
            except ValueError:
                self.status = ContractStatus.DRAFT
        else:
            self.status = status

        self.domain = domain
        self.data_product = data_product

        if description is None:
            self.description = ContractDescription()
        elif isinstance(description, ContractDescription):
            self.description = description
        else:
            self.description = ContractDescription.from_dict(description)

        self.tags = tags or []

        if team is None:
            self.team: ContractTeam | None = None
        elif isinstance(team, ContractTeam):
            self.team = team
        else:
            self.team = ContractTeam.from_dict(team)

        self.roles: list[ContractRole] = []
        if roles:
            for role in roles:
                if isinstance(role, ContractRole):
                    self.roles.append(role)
                else:
                    self.roles.append(ContractRole.from_dict(role))

        self.servers: list[Server] = []
        if servers:
            for server in servers:
                if isinstance(server, Server):
                    self.servers.append(server)
                else:
                    self.servers.append(Server.from_dict(server))

        self.sla_properties: list[SLAProperty] = []
        if sla_properties:
            for sla in sla_properties:  # type: ignore[assignment]
                if isinstance(sla, SLAProperty):
                    self.sla_properties.append(sla)
                else:
                    self.sla_properties.append(SLAProperty.from_dict(sla))  # type: ignore[arg-type]

        self.support: list[ContractSupport] = []
        if support:
            for sup in support:
                if isinstance(sup, ContractSupport):
                    self.support.append(sup)
                else:
                    self.support.append(ContractSupport.from_dict(sup))

        self.authoritative_definitions = authoritative_definitions or []
        self.custom_properties = custom_properties or {}
        self._schemas: list[Schema] = list(set(schemas)) if schemas else []
        self._schema_refs: list[SchemaRef] = list(schema_refs) if schema_refs else []

    # -------------------------------------------------------------------------
    # Schema Reference Support
    # -------------------------------------------------------------------------

    @property
    def schema_refs(self) -> list[SchemaRef]:
        """Get all schema references in this contract."""
        return self._schema_refs

    @property
    def has_schema_refs(self) -> bool:
        """Check if this contract uses schema references instead of inline schemas."""
        return len(self._schema_refs) > 0

    def add_schema_ref(self, schema_ref: SchemaRef) -> None:
        """Add a schema reference to this contract."""
        if schema_ref.schema_id in [ref.schema_id for ref in self._schema_refs]:
            print(f"SchemaRef {schema_ref.schema_id} already exists in contract. Skipping.")
            return
        self._schema_refs.append(schema_ref)

    def remove_schema_ref(self, schema_ref: SchemaRef) -> bool:
        """Remove a schema reference from this contract."""
        if len(self._schemas) == 0 and len(self._schema_refs) == 1:
            print("Cannot remove the last schema_ref when no inline schemas exist.")
            return False
        for ref in self._schema_refs:
            if ref.schema_id == schema_ref.schema_id:
                self._schema_refs.remove(ref)
                return True
        return False

    # -------------------------------------------------------------------------
    # Schema Management
    # -------------------------------------------------------------------------

    @property
    def schemas(self) -> list[Schema]:
        """Get all schemas in this contract."""
        return self._schemas

    def add_schema(self, schema: Schema) -> None:
        """Add a schema to this contract."""
        if schema.id in [existing_schema.id for existing_schema in self._schemas]:
            print(f"Schema {schema.id} already exists in contract. Skipping addition.")
            return
        print(f"Schema {schema.id} added to contract.")
        self._schemas.append(schema)

    def remove_schema(self, schema: Schema) -> bool:
        """Remove a schema from this contract."""

        if len(self._schemas) == 1:
            print("A contract must have at least one schema. Cannot remove the last schema.")
            return False
        if schema.id in [existing_schema.id for existing_schema in self._schemas]:
            self._schemas.remove(schema)
            print(f"Schema {schema.id} removed from contract.")
            return True
        print(f"Schema {schema.id} not found in contract.")
        return False

    def get_schema(self, index: int) -> Schema | None:
        """Get schema by index."""
        if 0 <= index < len(self._schemas):
            return self._schemas[index]
        return None

    def get_schema_by_id(self, schema_id: str) -> Schema | None:
        """Get schema by its ID."""
        for schema in self._schemas:
            if schema.id == schema_id:
                return schema
        return None

    def get_schema_by_name(self, name: str) -> Schema | None:
        """Get schema by its name."""
        for schema in self._schemas:
            if schema.name == name:
                return schema
        return None

    def list_schemas(self) -> list[Schema]:
        """List all schemas in this contract."""
        return list(self._schemas)

    def list_schema_names(self) -> list[str]:
        """List names of all schemas in this contract."""
        return [s.name for s in self._schemas]

    def __iter__(self) -> Iterator[Schema]:
        """Iterate over schemas."""
        return iter(self._schemas)

    def __len__(self) -> int:
        """Return number of schemas."""
        return len(self._schemas)

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_dict(self, include_metadata: bool = True) -> dict[str, Any]:
        """Convert to dictionary format compatible with registry.

        Args:
            include_metadata: Include registry metadata (uuid, timestamps, etc.)
        """
        result: dict[str, Any] = {
            "apiVersion": self.api_version,
            "kind": self._kind,
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "status": self.status.value if hasattr(self.status, "value") else str(self.status),
        }

        # Registry metadata
        if include_metadata:
            if self.uuid:
                result["uuid"] = self.uuid
            if self.created_at:
                result["created_at"] = self.created_at
            if self.updated_at:
                result["updated_at"] = self.updated_at
            if self.created_by:
                result["created_by"] = self.created_by

        # Inheritance
        if self.extends:
            result["extends"] = self.extends
        if self.extends_contract_id:
            result["extends_contract_id"] = self.extends_contract_id
        if self.is_template:
            result["is_template"] = self.is_template
        if self.template_category:
            result["template_category"] = self.template_category

        # Contract-level checks (new format)
        if self.checks:
            result["checks"] = self.checks

        # Structured configs (new format)
        if self.compliance:
            result["compliance"] = self.compliance
        if self.sla:
            result["sla"] = self.sla
        if self.executors:
            result["executors"] = self.executors
        if self.governance:
            result["governance"] = self.governance

        # Reviewer info
        if self.reviewer_type:
            result["reviewer_type"] = self.reviewer_type
        if self.reviewer_id:
            result["reviewer_id"] = self.reviewer_id
        if self.reviewer_name:
            result["reviewer_name"] = self.reviewer_name

        # Schema refs (separate from hydrated schemas)
        if self._schema_refs:
            result["schemaRefs"] = [ref.to_dict() for ref in self._schema_refs]

        # Serialize schemas (inline or hydrated)
        if self._schemas:
            result["schema"] = [s.to_dict() for s in self._schemas]

        # Other metadata
        if self.domain:
            result["domain"] = self.domain
        if self.data_product:
            result["dataProduct"] = self.data_product
        if self.owner_team_id:
            result["owner_team_id"] = self.owner_team_id

        desc_dict = self.description.to_dict() if self.description else {}
        if desc_dict:
            result["description"] = desc_dict

        if self.tags:
            result["tags"] = self.tags
        if self.authoritative_definitions:
            result["authoritativeDefinitions"] = self.authoritative_definitions
        if self.support:
            result["support"] = [s.to_dict() for s in self.support]
        if self.team:
            result["team"] = self.team.to_dict()
        if self.roles:
            result["roles"] = [r.to_dict() for r in self.roles]
        if self.sla_properties:
            result["slaProperties"] = [s.to_dict() for s in self.sla_properties]
        if self.servers:
            result["servers"] = [s.to_dict() for s in self.servers]
        if self.custom_properties:
            result["customProperties"] = self.custom_properties

        # Resolution metadata
        if include_metadata:
            if self.resolved_definition:
                result["resolved_definition"] = self.resolved_definition
            if self.inheritance_chain:
                result["inheritance_chain"] = self.inheritance_chain

        return result

    def to_yaml(self, include_metadata: bool = False) -> str:
        """Export contract to YAML string.

        Args:
            include_metadata: Include registry metadata (uuid, timestamps, etc.)
        """
        data = self.to_dict(include_metadata=include_metadata)

        # Remove empty values for cleaner YAML
        data = {k: v for k, v in data.items() if v is not None and v != [] and v != {}}

        return yaml.dump(
            data,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )

    def preview(self, include_metadata: bool = False) -> None:
        """Print a formatted YAML preview of the contract.

        Args:
            include_metadata: Include registry metadata (uuid, timestamps, etc.)
        """
        print(self.to_yaml(include_metadata=include_metadata))

    # -------------------------------------------------------------------------
    # Class Methods for Loading
    # -------------------------------------------------------------------------

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Contract:
        """Create Contract from dictionary.

        Supports both formats:
        - Old ODCS: sla_properties, custom_properties, schema with inline defs
        - New Registry: sla, compliance, checks, schema_refs, schema with _ref

        Handles hydrated schemas (with _ref metadata) and schema references.
        """

        # Preserve certain fields before normalization (keep original format)
        preserved_fields = {}
        for field in ("sla", "compliance", "executors", "executor_config", "governance", "checks"):
            if field in data:
                preserved_fields[field] = data.pop(field)

        # Normalize keys to snake_case
        data = normalize_keys(data, to_format="snake")

        # Restore preserved fields (not normalized)
        data.update(preserved_fields)

        # Parse schemas and schema_refs from schema array
        schemas, schema_refs = cls._parse_schemas(data)

        # Also check for separate schema_refs/schemaRefs field (avoid duplicates)
        existing_ref_ids = {ref.schema_id for ref in schema_refs}
        for ref in data.get("schema_refs", []):
            if isinstance(ref, dict):
                ref_obj = SchemaRef.from_dict(ref)
                if ref_obj.schema_id not in existing_ref_ids:
                    schema_refs.append(ref_obj)
                    existing_ref_ids.add(ref_obj.schema_id)

        return cls(
            # Identity
            api_version=data.get("api_version", "v1.0.0"),
            id=data.get("id"),  # type: ignore[arg-type]
            name=data.get("name", ""),
            version=data.get("version", "1.0.0"),
            status=data.get("status", "draft"),
            # Registry metadata
            uuid=data.get("uuid"),
            extends=data.get("extends"),
            extends_contract_id=data.get("extends_contract_id"),
            is_template=data.get("is_template", False),
            template_category=data.get("template_category"),
            owner_team_id=data.get("owner_team_id"),
            created_by=data.get("created_by"),
            # Contract-level checks (new format)
            checks=data.get("checks", []),
            # Structured configs (new format)
            compliance=data.get("compliance"),
            sla=data.get("sla"),
            executors=data.get("executors") or data.get("executor_config"),
            governance=data.get("governance"),
            # Reviewer info
            reviewer_type=data.get("reviewer_type"),
            reviewer_id=data.get("reviewer_id"),
            reviewer_name=data.get("reviewer_name"),
            # Resolution metadata
            resolved_definition=data.get("resolved_definition"),
            inheritance_chain=data.get("inheritance_chain", []),
            yaml_source=data.get("yaml_source"),
            # Timestamps
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            # Old ODCS fields
            domain=data.get("domain", ""),
            data_product=data.get("data_product", ""),
            description=data.get("description"),
            tags=data.get("tags", []),
            team=data.get("team"),
            roles=data.get("roles", []),
            servers=data.get("servers", []),
            sla_properties=data.get("sla_properties", []),
            support=data.get("support", []),
            authoritative_definitions=data.get("authoritative_definitions", []),
            custom_properties=data.get("custom_properties", {}),
            # Schemas
            schemas=schemas if schemas else None,
            schema_refs=schema_refs if schema_refs else None,
        )

    @classmethod
    def _parse_schemas(cls, data: dict[str, Any]) -> tuple[list, list]:
        """Parse schemas from contract data.

        Handles:
        1. Inline schemas (no _ref)
        2. Hydrated schemas (has _ref with schema_id)
        3. Schema references (has schema_id or $ref)
        """
        from griot_core.schema import Schema

        schemas: list[Schema] = []
        schema_refs: list[SchemaRef] = []

        for s in data.get("schema", []):
            if isinstance(s, Schema):
                schemas.append(s)
            elif isinstance(s, SchemaRef):
                schema_refs.append(s)
            elif isinstance(s, dict):
                # Check if this is a hydrated schema (has _ref metadata)
                if "_ref" in s:
                    # Extract schema ref from _ref
                    ref_data = s["_ref"]
                    schema_refs.append(
                        SchemaRef(
                            schema_id=ref_data.get("schema_id") or ref_data.get("schemaId"),
                            version=ref_data.get("version"),
                        )
                    )
                    # Also parse as inline schema for immediate use
                    schemas.append(Schema.from_dict(s))
                # Check if this is a schema reference
                elif "schema_id" in s or "$ref" in s or "ref" in s:
                    schema_refs.append(SchemaRef.from_dict(s))
                else:
                    # Inline schema
                    schemas.append(Schema.from_dict(s))
            else:
                raise ValueError("schemas must be a list of dicts, Schema, or SchemaRef")

        return schemas, schema_refs

    @classmethod
    def from_yaml(cls, path: str) -> Contract:
        """Load Contract from YAML file."""
        file_path = Path(path)
        if not file_path.exists():
            raise ContractNotFoundError(str(path))

        try:
            content = file_path.read_text(encoding="utf-8")
            data = yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise ContractParseError(f"Invalid YAML: {e}") from e

        if not isinstance(data, dict):
            raise ContractParseError("Contract must be a YAML mapping")

        return cls.from_dict(data)

    @classmethod
    def from_yaml_string(cls, content: str) -> Contract:
        """Load Contract from YAML string."""
        try:
            data = yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise ContractParseError(f"Invalid YAML: {e}") from e

        if not isinstance(data, dict):
            raise ContractParseError("Contract must be a YAML mapping")

        return cls.from_dict(data)

    def __repr__(self) -> str:
        return f"<Contract id={self.id!r} name={self.name!r} version={self.version!r} schemas={len(self._schemas)}>"

    def __str__(self) -> str:
        return f"Contract({self.name or self.id or 'unnamed'}, {len(self._schemas)} schemas)"


# =============================================================================
# Loading Functions
# =============================================================================


def load_contract(path: str | Path) -> Contract:
    """Load a contract from a YAML file."""
    return Contract.from_yaml(str(path))


def load_contract_from_string(content: str) -> Contract:
    """Load a contract from a YAML string."""
    return Contract.from_yaml_string(content)


def load_contract_from_dict(data: dict[str, Any]) -> Contract:
    """Load a contract from a dictionary."""
    return Contract.from_dict(data)


# =============================================================================
# Export Functions
# =============================================================================


def contract_to_yaml(contract: Contract, camel_case: bool = True) -> str:
    """Export a Contract to YAML format."""
    data = contract_to_dict(contract, camel_case=camel_case)
    return yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)


def contract_to_dict(contract: Contract, camel_case: bool = True) -> dict[str, Any]:
    """Export a Contract to dictionary format."""
    data = contract.to_dict()
    if not camel_case:
        data = normalize_keys(data, to_format="snake")
    return data


# =============================================================================
# Linting
# =============================================================================


@dataclass
class LintIssue:
    """Contract quality issue."""

    code: str
    field: str | None
    message: str
    severity: Severity
    suggestion: str | None = None


def lint_contract(contract: Contract) -> list[LintIssue]:
    """Check a Contract for quality issues and ODCS compliance.

    Rules:
      Contract-level:
        ODCS-001  Missing 'id'                                     (error)
        ODCS-002  Invalid status                                   (error)
        ODCS-003  No schemas defined                               (error)
        ODCS-004  Missing 'name'                                   (warning)
        ODCS-005  Missing 'version'                                (warning)
        ODCS-006  Missing description.purpose                      (warning)
        ODCS-007  Missing 'domain'                                 (info)
        ODCS-008  Missing 'compliance' section                     (info)
        ODCS-009  Missing 'sla' section                            (info)
        ODCS-010  Missing 'governance' section                     (info)
        ODCS-011  Governance missing data_producer                 (warning)
        ODCS-012  Governance missing data_consumer                 (warning)

      Schema-level:
        SCH-001   Schema missing 'name'                            (error)
        SCH-002   Schema missing 'physicalName'                    (warning)
        SCH-003   Schema has no properties                         (error)
        SCH-004   Schema has no checks                             (warning)
        SCH-005   Schema has no primary key                        (warning)

      Property-level:
        PROP-001  Property missing description                     (warning)
        PROP-002  Property missing logical_type                    (warning)
        PROP-003  PII field missing pii_type                       (warning)
        PROP-004  PII field missing masking_strategy               (info)
    """
    issues: list[LintIssue] = []

    # -- Contract-level checks -----------------------------------------------

    if not contract.id:
        issues.append(
            LintIssue(
                code="ODCS-001",
                field=None,
                message="Contract is missing required 'id' field",
                severity=Severity.ERROR,
                suggestion="Add a unique identifier",
            )
        )

    if contract.status not in ContractStatus:
        issues.append(
            LintIssue(
                code="ODCS-002",
                field=None,
                message=f"Contract has invalid status: {contract.status}",
                severity=Severity.ERROR,
                suggestion="Use: draft, active, deprecated, or retired",
            )
        )

    if not contract.schemas and not contract.has_schema_refs:
        issues.append(
            LintIssue(
                code="ODCS-003",
                field=None,
                message="Contract has no schemas or schema references",
                severity=Severity.ERROR,
                suggestion="Add at least one schema to the contract",
            )
        )

    if not contract.name:
        issues.append(
            LintIssue(
                code="ODCS-004",
                field=None,
                message="Contract is missing a 'name'",
                severity=Severity.WARNING,
                suggestion="Add a human-readable name",
            )
        )

    if not contract.version:
        issues.append(
            LintIssue(
                code="ODCS-005",
                field=None,
                message="Contract is missing a 'version'",
                severity=Severity.WARNING,
                suggestion="Add a semantic version (e.g. '1.0.0')",
            )
        )

    desc = contract.description
    if not desc or not getattr(desc, "purpose", None):
        issues.append(
            LintIssue(
                code="ODCS-006",
                field=None,
                message="Contract is missing description.purpose",
                severity=Severity.WARNING,
                suggestion="Add a purpose explaining why this contract exists",
            )
        )

    if not contract.data_product and not contract.domain:
        issues.append(
            LintIssue(
                code="ODCS-007",
                field=None,
                message="Contract is missing a 'domain' or 'dataProduct'",
                severity=Severity.INFO,
                suggestion="Specify the domain or data product this contract belongs to",
            )
        )

    if not contract.compliance:
        issues.append(
            LintIssue(
                code="ODCS-008",
                field=None,
                message="Contract has no 'compliance' section",
                severity=Severity.INFO,
                suggestion="Add compliance with legal, retention, and classification",
            )
        )

    if not contract.sla:
        issues.append(
            LintIssue(
                code="ODCS-009",
                field=None,
                message="Contract has no 'sla' section",
                severity=Severity.INFO,
                suggestion="Add SLA with freshness and availability targets",
            )
        )

    if not contract.governance:
        issues.append(
            LintIssue(
                code="ODCS-010",
                field=None,
                message="Contract has no 'governance' section",
                severity=Severity.INFO,
                suggestion="Add governance with data_producer and data_consumer",
            )
        )
    else:
        if not contract.governance.get("data_producer"):
            issues.append(
                LintIssue(
                    code="ODCS-011",
                    field=None,
                    message="Governance is missing 'data_producer'",
                    severity=Severity.WARNING,
                    suggestion="Specify who produces this data",
                )
            )
        if not contract.governance.get("data_consumer"):
            issues.append(
                LintIssue(
                    code="ODCS-012",
                    field=None,
                    message="Governance is missing 'data_consumer'",
                    severity=Severity.WARNING,
                    suggestion="Specify who consumes this data",
                )
            )

    # Skip inline schema validation when using only schema_refs
    if contract.has_schema_refs and not contract.schemas:
        return issues

    # -- Schema-level checks -------------------------------------------------

    for idx, schema in enumerate(contract.schemas):
        s_prefix = f"schema[{idx}]"

        if not schema.name:
            issues.append(
                LintIssue(
                    code="SCH-001",
                    field=s_prefix,
                    message=f"Schema at index {idx} is missing a 'name'",
                    severity=Severity.ERROR,
                    suggestion="Add a name to the schema",
                )
            )

        if not (schema.physical_name or ""):
            issues.append(
                LintIssue(
                    code="SCH-002",
                    field=s_prefix,
                    message=f"Schema '{schema.name}' is missing 'physicalName'",
                    severity=Severity.WARNING,
                    suggestion="Add a physicalName (e.g. 'analytics.dim_customers')",
                )
            )

        fields = schema.fields
        if not fields:
            issues.append(
                LintIssue(
                    code="SCH-003",
                    field=s_prefix,
                    message=f"Schema '{schema.name}' has no properties defined",
                    severity=Severity.ERROR,
                    suggestion="Add at least one property to the schema",
                )
            )
            continue

        has_pk = any(f.primary_key for f in fields.values())
        if not has_pk:
            issues.append(
                LintIssue(
                    code="SCH-005",
                    field=s_prefix,
                    message=f"Schema '{schema.name}' has no primary key",
                    severity=Severity.WARNING,
                    suggestion="Mark at least one property with is_primary_key: true",
                )
            )

        checks = schema.quality or []
        if not checks:
            issues.append(
                LintIssue(
                    code="SCH-004",
                    field=s_prefix,
                    message=f"Schema '{schema.name}' has no checks defined",
                    severity=Severity.WARNING,
                    suggestion="Add quality checks (completeness, uniqueness, etc.)",
                )
            )

        # -- Property-level checks -------------------------------------------

        for field_name, field_info in fields.items():
            f_prefix = f"{schema.name}.{field_name}"

            if not field_info.description:
                issues.append(
                    LintIssue(
                        code="PROP-001",
                        field=f_prefix,
                        message=f"Property '{field_name}' has no description",
                        severity=Severity.WARNING,
                        suggestion="Add a description explaining this field",
                    )
                )

            if not field_info.logical_type:
                issues.append(
                    LintIssue(
                        code="PROP-002",
                        field=f_prefix,
                        message=f"Property '{field_name}' has no logical_type",
                        severity=Severity.WARNING,
                        suggestion="Specify logical_type (string, integer, date, etc.)",
                    )
                )

            privacy = field_info.custom_properties.get("privacy") or {}
            if privacy.get("is_pii") and not privacy.get("pii_type"):
                issues.append(
                    LintIssue(
                        code="PROP-003",
                        field=f_prefix,
                        message=f"PII field '{field_name}' is missing 'pii_type'",
                        severity=Severity.WARNING,
                        suggestion="Specify pii_type (name, email, phone, etc.)",
                    )
                )
            if privacy.get("is_pii") and not privacy.get("masking_strategy"):
                issues.append(
                    LintIssue(
                        code="PROP-004",
                        field=f_prefix,
                        message=f"PII field '{field_name}' has no masking_strategy",
                        severity=Severity.INFO,
                        suggestion="Consider adding a masking strategy for PII data",
                    )
                )

    return issues


# =============================================================================
# Contract Structure Validation
# =============================================================================


@dataclass
class ContractStructureIssue:
    """Issue found during contract structure validation."""

    code: str
    path: str
    message: str
    severity: Severity
    suggestion: str | None = None


@dataclass
class ContractStructureResult:
    """Result of contract structure validation."""

    is_valid: bool
    contract_id: str | None
    issues: list[ContractStructureIssue]
    error_count: int = 0
    warning_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "contract_id": self.contract_id,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "issues": [
                {
                    "code": i.code,
                    "path": i.path,
                    "message": i.message,
                    "severity": i.severity.value,
                    "suggestion": i.suggestion,
                }
                for i in self.issues
            ],
        }

    def summary(self) -> str:
        """Return human-readable summary."""
        status = "VALID" if self.is_valid else "INVALID"
        return (
            f"Contract structure {status}: {self.error_count} errors, {self.warning_count} warnings"
        )


def validate_contract_structure(contract: Contract) -> ContractStructureResult:
    """Validate the structure of a Contract.

    When a contract uses schema_refs instead of inline schemas, schema-level
    validation (properties, primary keys, field quality) is skipped since the
    actual schema definitions are stored in the registry and will be validated
    when resolved.
    """
    issues: list[ContractStructureIssue] = []

    if not contract.id:
        issues.append(
            ContractStructureIssue(
                code="CS-001",
                path="$.id",
                message="Contract is missing required 'id' field",
                severity=Severity.ERROR,
                suggestion="Add a unique identifier to the contract",
            )
        )

    if not contract.version:
        issues.append(
            ContractStructureIssue(
                code="CS-002",
                path="$.version",
                message="Contract is missing required 'version' field",
                severity=Severity.ERROR,
                suggestion="Add a semantic version (e.g., '1.0.0')",
            )
        )

    if contract.status not in ContractStatus:
        issues.append(
            ContractStructureIssue(
                code="CS-003",
                path="$.status",
                message=f"Invalid contract status: {contract.status}",
                severity=Severity.ERROR,
                suggestion="Use: draft, active, deprecated, or retired",
            )
        )

    if not contract.name:
        issues.append(
            ContractStructureIssue(
                code="CS-004",
                path="$.name",
                message="Contract is missing a name",
                severity=Severity.WARNING,
                suggestion="Add a human-readable name",
            )
        )

    # Check for schemas OR schema_refs
    if not contract.schemas and not contract.has_schema_refs:
        issues.append(
            ContractStructureIssue(
                code="CS-010",
                path="$.schema",
                message="Contract has no schemas or schema references defined",
                severity=Severity.ERROR,
                suggestion="Add at least one schema or schema_ref to the contract",
            )
        )

    # Validate schema_refs (basic validation only - full validation happens at registry)
    if contract.has_schema_refs:
        for idx, ref in enumerate(contract.schema_refs):
            ref_path = f"$.schema[{idx}]"
            if not ref.schema_id:
                issues.append(
                    ContractStructureIssue(
                        code="CS-014",
                        path=f"{ref_path}.schema_id",
                        message=f"Schema reference at index {idx} is missing schema_id",
                        severity=Severity.ERROR,
                        suggestion="Add a schema_id to the schema reference",
                    )
                )

    # Validate inline schemas (only if not using refs)
    if contract.schemas and not contract.has_schema_refs:
        for idx, schema in enumerate(contract.schemas):
            schema_path = f"$.schema[{idx}]"

            if not schema.name:
                issues.append(
                    ContractStructureIssue(
                        code="CS-011",
                        path=f"{schema_path}.name",
                        message=f"Schema at index {idx} is missing a name",
                        severity=Severity.ERROR,
                        suggestion="Add a name to the schema",
                    )
                )

            fields = schema.fields
            if fields:
                for field_name, field_info in fields.items():
                    field_path = f"{schema_path}.properties.{field_name}"

                    if not field_info.quality:
                        issues.append(
                            ContractStructureIssue(
                                code="CS-020",
                                path=f"{field_path}.quality",
                                message=f"Field '{field_name}' has no quality checks",
                                severity=Severity.WARNING,
                                suggestion=f"Add a quality check to the {field_info} field",
                            )
                        )

                    privacy = field_info.custom_properties.get("privacy") or {}
                    if privacy.get("is_pii") and privacy.get("pii_type") is None:
                        issues.append(
                            ContractStructureIssue(
                                code="CS-022",
                                path=f"{field_path}.privacy",
                                message=f"Field '{field_name}' has no PII tag",
                                severity=Severity.WARNING,
                                suggestion=f"Consider adding a PII type to the {field_name} field",
                            )
                        )

                    if not field_info.description:
                        issues.append(
                            ContractStructureIssue(
                                code="CS-023",
                                path=f"{field_path}.description",
                                message=f"Field '{field_name}' has no description",
                                severity=Severity.WARNING,
                                suggestion="Add a description explaining the field's purpose",
                            )
                        )

    error_count = sum(1 for i in issues if i.severity == Severity.ERROR)
    warning_count = sum(1 for i in issues if i.severity == Severity.WARNING)

    return ContractStructureResult(
        is_valid=error_count == 0,
        contract_id=contract.id,
        issues=issues,
        error_count=error_count,
        warning_count=warning_count,
    )
