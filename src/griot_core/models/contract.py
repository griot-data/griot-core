"""
Contract model with inheritance support.

Contracts are agreements about data assets (schemas). They can extend
other contracts or templates to inherit configuration, overriding only
what differs for the specific use case.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from .enums import ContractStatus, Severity
from .schema_ref import SchemaRef

if TYPE_CHECKING:
    from .check import Check
    from .schema import Schema


@dataclass
class ContractDescription:
    """
    Contract description metadata.

    Provides human-readable context about the contract's purpose and usage.
    """

    purpose: str = ""
    usage: str = ""
    limitations: str = ""


@dataclass
class ExecutorProfile:
    """
    Configuration for a specific execution profile.

    Different teams (data engineering, software engineering, etc.)
    can have different check configurations and runtime preferences.
    """

    description: str = ""
    checks_include: List[str] = field(default_factory=list)  # ["all"] or specific checks
    checks_exclude: List[str] = field(default_factory=list)
    runtime_preference: List[str] = field(default_factory=lambda: ["wasm", "container"])
    custom_executors: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class AutoCheckConfig:
    """
    Configuration for auto-generated checks from constraints.

    Auto-checks are automatically created based on property constraints
    (nullable, unique, primary_key, required, pii_masking).
    """

    enabled: bool = True
    include: List[str] = field(
        default_factory=lambda: ["nullable", "unique", "primary_key", "required", "pii_masking"]
    )
    severity: Severity = Severity.CRITICAL


@dataclass
class ExecutorConfig:
    """
    Executor configuration for the contract.

    Defines default runtime settings, auto-check configuration,
    and profile-specific overrides.
    """

    default_runtime_preference: List[str] = field(default_factory=lambda: ["wasm", "container"])
    default_timeout: str = "300s"
    registry: str = "registry://executors"
    auto_checks: AutoCheckConfig = field(default_factory=AutoCheckConfig)
    profiles: Dict[str, ExecutorProfile] = field(default_factory=dict)


@dataclass
class DataSubjectRight:
    """Configuration for a data subject right (GDPR/CCPA)."""

    enabled: bool = False
    sla: str = "P30D"  # ISO 8601 duration
    endpoint: str = ""
    exceptions: List[str] = field(default_factory=list)
    formats: List[str] = field(default_factory=list)  # For portability


@dataclass
class DataSubjectRights:
    """Collection of data subject rights configurations."""

    access_request: DataSubjectRight = field(default_factory=DataSubjectRight)
    erasure_request: DataSubjectRight = field(default_factory=DataSubjectRight)
    portability_request: DataSubjectRight = field(default_factory=DataSubjectRight)


@dataclass
class RetentionPolicy:
    """Data retention policy configuration."""

    period: str = "P7Y"  # ISO 8601 duration
    policy: str = "archive"  # delete | archive | anonymize
    exceptions: List[Dict[str, Any]] = field(default_factory=list)
    review_schedule: str = "P1Y"


@dataclass
class CrossBorderConfig:
    """Cross-border data transfer configuration."""

    transfers_allowed: bool = False
    approved_countries: List[str] = field(default_factory=list)
    transfer_mechanisms: Dict[str, bool] = field(default_factory=dict)
    restrictions: List[str] = field(default_factory=list)


@dataclass
class AuditConfig:
    """Audit logging configuration."""

    logging_required: bool = True
    log_retention: str = "P365D"
    logged_events: List[str] = field(default_factory=lambda: ["read", "write", "delete", "export"])
    audit_log_location: str = ""


@dataclass
class ExportControls:
    """Data export controls configuration."""

    bulk_download_allowed: bool = False
    api_access_allowed: bool = True
    rate_limit: str = "1000/hour"
    requires_approval: List[str] = field(default_factory=list)


@dataclass
class Regulation:
    """Regulatory compliance configuration."""

    name: str
    applicable: bool = True
    articles: List[Dict[str, Any]] = field(default_factory=list)
    documentation: str = ""


@dataclass
class LegalConfig:
    """Legal and regulatory configuration."""

    jurisdiction: List[str] = field(default_factory=list)
    legal_basis: str = "legitimate_interest"
    data_controller: str = ""
    data_processor: Optional[str] = None
    regulations: List[Regulation] = field(default_factory=list)


@dataclass
class ComplianceConfig:
    """
    Compliance configuration for the contract.

    Covers data classification, legal requirements, data subject rights,
    retention, cross-border transfers, audit, and export controls.
    """

    classification: str = "internal"  # public | internal | confidential | restricted
    legal: LegalConfig = field(default_factory=LegalConfig)
    data_subject_rights: DataSubjectRights = field(default_factory=DataSubjectRights)
    retention: RetentionPolicy = field(default_factory=RetentionPolicy)
    cross_border: CrossBorderConfig = field(default_factory=CrossBorderConfig)
    audit: AuditConfig = field(default_factory=AuditConfig)
    export_controls: ExportControls = field(default_factory=ExportControls)


@dataclass
class SLAConfig:
    """
    SLA configuration for the contract.

    Defines freshness, availability, volume, and quality targets.
    """

    freshness_max_age: str = "24h"
    freshness_column: str = ""
    availability_schedule: str = "00:00-23:59 UTC"
    availability_target_uptime: str = "99%"
    volume_expected_daily_records: Optional[int] = None
    volume_tolerance_percent: int = 20
    volume_alert_on_anomaly: bool = True
    quality_minimum_completeness: str = "99%"
    quality_maximum_null_rate: str = "1%"


@dataclass
class ReviewConfig:
    """Governance review configuration."""

    cadence: str = "quarterly"  # monthly | quarterly | annually
    last_review: Optional[datetime] = None
    next_review: Optional[datetime] = None
    reviewers: List[str] = field(default_factory=list)


@dataclass
class ChangeManagement:
    """Change management configuration."""

    breaking_change_notice: str = "P30D"  # ISO 8601 duration
    deprecation_notice: str = "P90D"
    migration_support: bool = True


@dataclass
class ApprovalWorkflow:
    """Approval workflow for different change types."""

    schema_changes: List[str] = field(default_factory=list)
    check_changes: List[str] = field(default_factory=list)
    compliance_changes: List[str] = field(default_factory=list)


@dataclass
class GovernanceConfig:
    """
    Governance configuration for the contract.

    Covers review schedules, change management, and approval workflows.
    """

    review: ReviewConfig = field(default_factory=ReviewConfig)
    change_management: ChangeManagement = field(default_factory=ChangeManagement)
    approval_workflow: ApprovalWorkflow = field(default_factory=ApprovalWorkflow)


@dataclass
class Server:
    """
    Server/data source definition.

    Defines where the data lives and how to connect to it.
    """

    name: str
    type: str  # bigquery | snowflake | postgres | s3 | etc.
    environment: str  # prod | staging | dev
    connection: Dict[str, Any] = field(default_factory=dict)
    roles: List[str] = field(default_factory=list)


@dataclass
class TeamMember:
    """A team member."""

    email: str
    role: str = "member"


@dataclass
class TeamConfig:
    """
    Team configuration for the contract.

    Defines who is responsible for the contract.
    """

    name: str = ""
    description: str = ""
    members: List[str] = field(default_factory=list)


@dataclass
class SupportChannel:
    """
    Support channel definition.

    Defines how to get help with this contract.
    """

    type: str  # slack | email | jira | pagerduty
    contact: str


@dataclass
class AuthoritativeDefinition:
    """Reference to an authoritative definition."""

    type: str  # canonical | reference
    url: str
    description: str = ""


@dataclass
class Contract:
    """
    A data contract - an agreement about one or more schemas.

    Contracts can extend other contracts or templates to inherit
    configuration, overriding only what differs for the specific use case.

    Attributes:
        api_version: API schema version (e.g., "v1.0.0")
        kind: Always "DataContract"
        id: Unique identifier for this contract
        name: Human-readable name
        version: Semantic version (e.g., "1.0.0")
        status: Lifecycle status (draft, active, deprecated, archived)
        extends: Optional URI to parent contract/template for inheritance
        schema_refs: References to standalone schemas
        inline_schemas: Schemas defined inline within this contract
        description: Contract description metadata
        owner: Owner team/person
        data_product: Associated data product
        tags: Tags for categorization
        authoritative_definitions: Links to canonical sources
        executors: Executor configuration with profiles
        checks: Contract-level checks (apply to all schemas)
        compliance: Compliance configuration
        sla: SLA configuration
        governance: Governance configuration
        servers: Data source definitions
        team: Team configuration
        support: Support channels
    """

    # Identity
    api_version: str = "v1.0.0"
    kind: str = "DataContract"
    id: str = ""
    name: str = ""
    version: str = "1.0.0"
    status: ContractStatus = ContractStatus.DRAFT

    # Inheritance
    extends: Optional[str] = None  # "griot://templates/base-contract@1.0"

    # Schema references (not embedded!)
    schema_refs: List[SchemaRef] = field(default_factory=list)
    inline_schemas: List["Schema"] = field(default_factory=list)

    # Metadata
    description: Optional[ContractDescription] = None
    owner: Optional[str] = None
    data_product: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    authoritative_definitions: List[AuthoritativeDefinition] = field(default_factory=list)

    # Configuration (can be inherited/overridden)
    executors: Optional[ExecutorConfig] = None
    checks: List["Check"] = field(default_factory=list)
    compliance: Optional[ComplianceConfig] = None
    sla: Optional[SLAConfig] = None
    governance: Optional[GovernanceConfig] = None

    # Infrastructure
    servers: List[Server] = field(default_factory=list)
    team: Optional[TeamConfig] = None
    support: List[SupportChannel] = field(default_factory=list)

    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
