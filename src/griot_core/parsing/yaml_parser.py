"""
YAML parser for contracts and schemas.

Parses YAML files into griot-core dataclasses following the
Data Contract Protocol Specification.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

from griot_core.models import (
    ApprovalWorkflow,
    AuditConfig,
    AuthoritativeDefinition,
    AutoCheckConfig,
    Cardinality,
    ChangeManagement,
    Check,
    CheckCategory,
    CheckCondition,
    CheckType,
    ComplianceConfig,
    # Core models
    Contract,
    # Contract configuration
    ContractDescription,
    ContractStatus,
    CrossBorderConfig,
    DataSubjectRight,
    DataSubjectRights,
    ExecutorConfig,
    ExecutorProfile,
    ExportControls,
    GovernanceConfig,
    LegalConfig,
    LogicalType,
    MaskingStrategy,
    PIIType,
    Property,
    PropertyConstraints,
    Regulation,
    Relationship,
    RelationshipType,
    RetentionPolicy,
    ReviewConfig,
    Schema,
    SchemaRef,
    # Enums
    SchemaStatus,
    Sensitivity,
    Server,
    Severity,
    SLAConfig,
    SupportChannel,
    TeamConfig,
)


def parse_contract_yaml(yaml_content: str) -> Contract:
    """
    Parse a YAML string into a Contract object.

    Args:
        yaml_content: YAML string containing the contract definition

    Returns:
        Contract object

    Raises:
        ValueError: If the YAML is invalid or missing required fields
    """
    data = yaml.safe_load(yaml_content)
    if not isinstance(data, dict):
        raise ValueError("Contract YAML must be a dictionary")
    return _parse_contract_dict(data)


def parse_schema_yaml(yaml_content: str) -> Schema:
    """
    Parse a YAML string into a Schema object.

    Args:
        yaml_content: YAML string containing the schema definition

    Returns:
        Schema object

    Raises:
        ValueError: If the YAML is invalid or missing required fields
    """
    data = yaml.safe_load(yaml_content)
    if not isinstance(data, dict):
        raise ValueError("Schema YAML must be a dictionary")
    return _parse_schema_dict(data)


def load_contract_from_file(file_path: Union[str, Path]) -> Contract:
    """
    Load a Contract from a YAML file.

    Args:
        file_path: Path to the YAML file

    Returns:
        Contract object
    """
    path = Path(file_path)
    with path.open("r", encoding="utf-8") as f:
        return parse_contract_yaml(f.read())


def load_schema_from_file(file_path: Union[str, Path]) -> Schema:
    """
    Load a Schema from a YAML file.

    Args:
        file_path: Path to the YAML file

    Returns:
        Schema object
    """
    path = Path(file_path)
    with path.open("r", encoding="utf-8") as f:
        return parse_schema_yaml(f.read())


def _parse_contract_dict(data: Dict[str, Any]) -> Contract:
    """Parse a dictionary into a Contract object."""
    # Parse status
    status = ContractStatus.DRAFT
    if "status" in data:
        status = ContractStatus(data["status"])

    # Parse description
    description = None
    if "description" in data:
        desc_data = data["description"]
        if isinstance(desc_data, dict):
            description = ContractDescription(
                purpose=desc_data.get("purpose", ""),
                usage=desc_data.get("usage", ""),
                limitations=desc_data.get("limitations", ""),
            )

    # Parse schema references and inline schemas
    schema_refs = []
    inline_schemas = []
    if "schema" in data:
        for schema_item in data["schema"]:
            if "$ref" in schema_item:
                schema_refs.append(
                    SchemaRef(  # type: ignore[call-arg]
                        ref=schema_item["$ref"],
                        version_pinned=schema_item.get("version_pinned", True),
                    )
                )
            else:
                inline_schemas.append(_parse_schema_dict(schema_item))

    # Parse checks
    checks = []
    if "checks" in data:
        for check_data in data["checks"]:
            checks.append(_parse_check_dict(check_data))

    # Parse executors
    executors = None
    if "executors" in data:
        executors = _parse_executor_config(data["executors"])

    # Parse compliance
    compliance = None
    if "compliance" in data:
        compliance = _parse_compliance_config(data["compliance"])

    # Parse SLA
    sla = None
    if "sla" in data:
        sla = _parse_sla_config(data["sla"])

    # Parse governance
    governance = None
    if "governance" in data:
        governance = _parse_governance_config(data["governance"])

    # Parse servers
    servers = []
    if "servers" in data:
        for server_data in data["servers"]:
            servers.append(
                Server(
                    name=server_data.get("name", ""),
                    type=server_data.get("type", ""),
                    environment=server_data.get("environment", ""),
                    connection=server_data.get("connection", {}),
                    roles=server_data.get("roles", []),
                )
            )

    # Parse team
    team = None
    if "team" in data:
        team_data = data["team"]
        team = TeamConfig(
            name=team_data.get("name", ""),
            description=team_data.get("description", ""),
            members=team_data.get("members", []),
        )

    # Parse support channels
    support = []
    if "support" in data:
        support_data = data["support"]
        if "channels" in support_data:
            for channel in support_data["channels"]:
                support.append(
                    SupportChannel(
                        type=channel.get("type", ""),
                        contact=channel.get("contact", ""),
                    )
                )

    # Parse authoritative definitions
    auth_defs = []
    if "authoritativeDefinitions" in data:
        for auth_def in data["authoritativeDefinitions"]:
            auth_defs.append(
                AuthoritativeDefinition(
                    type=auth_def.get("type", ""),
                    url=auth_def.get("url", ""),
                    description=auth_def.get("description", ""),
                )
            )

    return Contract(
        api_version=data.get("apiVersion", "v1.0.0"),
        kind=data.get("kind", "DataContract"),
        id=data.get("id", ""),
        name=data.get("name", ""),
        version=data.get("version", "1.0.0"),
        status=status,
        extends=data.get("extends"),
        schema_refs=schema_refs,
        inline_schemas=inline_schemas,
        description=description,
        owner=data.get("owner"),
        data_product=data.get("dataProduct"),
        tags=data.get("tags", []),
        authoritative_definitions=auth_defs,
        executors=executors,
        checks=checks,
        compliance=compliance,
        sla=sla,
        governance=governance,
        servers=servers,
        team=team,
        support=support,
    )


def _parse_schema_dict(data: Dict[str, Any]) -> Schema:
    """Parse a dictionary into a Schema object."""
    # Parse status
    status = SchemaStatus.DRAFT
    if "status" in data:
        status = SchemaStatus(data["status"])

    # Parse properties
    properties = []
    if "properties" in data:
        for idx, prop_data in enumerate(data["properties"]):
            properties.append(_parse_property_dict(prop_data, idx))

    # Parse checks
    checks = []
    if "checks" in data:
        for check_data in data["checks"]:
            checks.append(_parse_check_dict(check_data))

    schema = Schema(
        id=data.get("id", ""),
        name=data.get("name", ""),
        version=data.get("version", "1.0.0"),
        status=status,
        physical_name=data.get("physicalName", ""),
        description=data.get("description", ""),
        owner_team=data.get("owner_team", data.get("ownerTeam", "")),
        properties=properties,
        checks=checks,
        tags=data.get("tags", []),
        created_at=_parse_datetime(data.get("created_at")),
        updated_at=_parse_datetime(data.get("updated_at")),
    )

    # Preserve registry _ref block if present (e.g. from pulled contracts)
    ref_data = data.get("_ref")
    if ref_data and isinstance(ref_data, dict):
        schema.registry_ref = ref_data

    return schema


def _parse_property_dict(data: Dict[str, Any], ordinal: int = 0) -> Property:
    """Parse a dictionary into a Property object.

    Supports both the legacy camelCase format and the registry snake_case
    format where ``is_primary_key``, ``is_pii``, etc. are top-level fields
    and ``constraints`` is a list of ``{type, value}`` objects.
    """
    # Parse logical type — accept both camelCase and snake_case keys
    logical_type = LogicalType.STRING
    raw_lt = data.get("logicalType") or data.get("logical_type")
    if raw_lt:
        try:
            logical_type = LogicalType(raw_lt)
        except ValueError:
            pass  # Keep default

    # --- Build PropertyConstraints ---
    # Registry format: flags live at top level of the property dict
    # Legacy format: flags live inside a constraints dict
    constraints_raw = data.get("constraints", {})
    is_legacy_constraints = isinstance(constraints_raw, dict)

    if is_legacy_constraints:
        # Legacy: constraints is a dict with primary_key, required, etc.
        cd = constraints_raw
    else:
        # Registry: constraints is a list of {type, value}; flags are top-level
        cd = data

    pii_type = None
    raw_pii = cd.get("pii_type")
    if raw_pii:
        try:
            pii_type = PIIType(raw_pii)
        except ValueError:
            pass

    masking_strategy = None
    raw_ms = cd.get("masking_strategy")
    if raw_ms:
        try:
            masking_strategy = MaskingStrategy(raw_ms)
        except ValueError:
            pass

    sensitivity = None
    raw_sens = cd.get("sensitivity")
    if raw_sens:
        try:
            sensitivity = Sensitivity(raw_sens)
        except ValueError:
            pass

    constraints = PropertyConstraints(
        primary_key=cd.get("is_primary_key", cd.get("primary_key", False)),
        required=cd.get("is_required", cd.get("required", False)),
        unique=cd.get("is_unique", cd.get("unique", False)),
        nullable=cd.get("is_nullable", cd.get("nullable", True)),
        partitioned=cd.get("is_partitioned", cd.get("partitioned", False)),
        partition_key_position=cd.get("partition_key_position"),
        is_pii=cd.get("is_pii", False),
        pii_type=pii_type,
        masking_strategy=masking_strategy,
        sensitivity=sensitivity,
    )

    # Parse relationships
    relationships = []
    if "relationships" in data:
        for rel_data in data["relationships"]:
            rel_type = RelationshipType.REFERENCES
            if "type" in rel_data:
                try:
                    rel_type = RelationshipType(rel_data["type"])
                except ValueError:
                    pass

            cardinality = Cardinality.ONE_TO_ONE
            if "cardinality" in rel_data:
                card_value = rel_data["cardinality"].replace("-", "_")
                try:
                    cardinality = Cardinality(card_value)
                except ValueError:
                    pass

            relationships.append(
                Relationship(
                    to=rel_data.get("to", ""),
                    type=rel_type,
                    cardinality=cardinality,
                    description=rel_data.get("description", ""),
                )
            )

    return Property(
        id=data.get("property_id", data.get("id", "")),
        name=data.get("name", ""),
        logical_type=logical_type,
        physical_type=data.get("physical_type", data.get("physicalType", "")),
        description=data.get("description", ""),
        ordinal_position=data.get("ordinal_position", ordinal),
        constraints=constraints,
        relationships=relationships,
    )


def _parse_check_dict(data: Dict[str, Any]) -> Check:
    """Parse a dictionary into a Check object."""
    # Parse check type
    check_type = CheckType.DATA_QUALITY
    if "type" in data:
        try:
            check_type = CheckType(data["type"])
        except ValueError:
            pass

    # Parse severity
    severity = Severity.WARNING
    if "severity" in data:
        try:
            severity = Severity(data["severity"])
        except ValueError:
            pass

    # Parse condition
    when = None
    if "when" in data:
        when_data = data["when"]
        when = CheckCondition(
            environment=when_data.get("environment"),
            profile=when_data.get("profile"),
            tags=when_data.get("tags"),
        )

    # Parse category
    category = CheckCategory.CUSTOM
    if "category" in data:
        try:
            category = CheckCategory(data["category"])
        except ValueError:
            pass

    # Accept "arguments" as alias for "parameters" (backward compat)
    parameters = data.get("parameters", {}) or data.get("arguments", {})

    return Check(
        name=data.get("name", ""),
        description=data.get("description", ""),
        type=check_type,
        executor=data.get("executor", ""),
        parameters=parameters,
        check_function=data.get("checkFunction", data.get("check_function", "validate")),
        severity=severity,
        when=when,
        tags=data.get("tags", []),
        category=category,
        columns=data.get("columns", []),
    )


def _parse_executor_config(data: Dict[str, Any]) -> ExecutorConfig:
    """Parse executor configuration."""
    default = data.get("default", {})
    auto_checks_data = data.get("auto_checks", {})

    # Parse auto checks
    auto_checks = AutoCheckConfig(
        enabled=auto_checks_data.get("enabled", True),
        include=auto_checks_data.get(
            "include", ["nullable", "unique", "primary_key", "required", "pii_masking"]
        ),
        severity=Severity(auto_checks_data.get("severity", "critical")),
    )

    # Parse profiles
    profiles = {}
    if "profiles" in data:
        for name, profile_data in data["profiles"].items():
            checks_config = profile_data.get("checks", {})
            profiles[name] = ExecutorProfile(
                description=profile_data.get("description", ""),
                checks_include=checks_config.get("include", []),
                checks_exclude=checks_config.get("exclude", []),
                runtime_preference=profile_data.get("runtime_preference", ["wasm", "container"]),
                custom_executors=profile_data.get("custom_executors", []),
            )

    return ExecutorConfig(
        default_runtime_preference=default.get("runtime_preference", ["wasm", "container"]),
        default_timeout=default.get("timeout", "300s"),
        registry=default.get("registry", "registry://executors"),
        auto_checks=auto_checks,
        profiles=profiles,
    )


def _parse_compliance_config(data: Dict[str, Any]) -> ComplianceConfig:
    """Parse compliance configuration."""
    # Parse legal
    legal_data = data.get("legal", {})
    regulations = []
    for reg_data in legal_data.get("regulations", []):
        regulations.append(
            Regulation(
                name=reg_data.get("name", ""),
                applicable=reg_data.get("applicable", True),
                articles=reg_data.get("articles", []),
                documentation=reg_data.get("documentation", ""),
            )
        )

    legal = LegalConfig(
        jurisdiction=legal_data.get("jurisdiction", []),
        legal_basis=legal_data.get("legal_basis", "legitimate_interest"),
        data_controller=legal_data.get("data_controller", ""),
        data_processor=legal_data.get("data_processor"),
        regulations=regulations,
    )

    # Parse data subject rights
    dsr_data = data.get("data_subject_rights", {})
    data_subject_rights = DataSubjectRights(
        access_request=_parse_data_subject_right(dsr_data.get("access_request", {})),
        erasure_request=_parse_data_subject_right(dsr_data.get("erasure_request", {})),
        portability_request=_parse_data_subject_right(dsr_data.get("portability_request", {})),
    )

    # Parse retention
    retention_data = data.get("retention", {})
    retention = RetentionPolicy(
        period=retention_data.get("period", "P7Y"),
        policy=retention_data.get("policy", "archive"),
        exceptions=retention_data.get("exceptions", []),
        review_schedule=retention_data.get("review_schedule", "P1Y"),
    )

    # Parse cross border
    cb_data = data.get("cross_border", {})
    cross_border = CrossBorderConfig(
        transfers_allowed=cb_data.get("transfers_allowed", False),
        approved_countries=cb_data.get("approved_countries", []),
        transfer_mechanisms=cb_data.get("transfer_mechanisms", {}),
        restrictions=cb_data.get("restrictions", []),
    )

    # Parse audit
    audit_data = data.get("audit", {})
    audit = AuditConfig(
        logging_required=audit_data.get("logging_required", True),
        log_retention=audit_data.get("log_retention", "P365D"),
        logged_events=audit_data.get("logged_events", ["read", "write", "delete", "export"]),
        audit_log_location=audit_data.get("audit_log_location", ""),
    )

    # Parse export controls
    export_data = data.get("export_controls", {})
    export_controls = ExportControls(
        bulk_download_allowed=export_data.get("bulk_download_allowed", False),
        api_access_allowed=export_data.get("api_access_allowed", True),
        rate_limit=export_data.get("rate_limit", "1000/hour"),
        requires_approval=export_data.get("requires_approval", []),
    )

    return ComplianceConfig(
        classification=data.get("classification", "internal"),
        legal=legal,
        data_subject_rights=data_subject_rights,
        retention=retention,
        cross_border=cross_border,
        audit=audit,
        export_controls=export_controls,
    )


def _parse_data_subject_right(data: Dict[str, Any]) -> DataSubjectRight:
    """Parse a data subject right."""
    return DataSubjectRight(
        enabled=data.get("enabled", False),
        sla=data.get("sla", "P30D"),
        endpoint=data.get("endpoint", ""),
        exceptions=data.get("exceptions", []),
        formats=data.get("formats", []),
    )


def _parse_sla_config(data: Dict[str, Any]) -> SLAConfig:
    """Parse SLA configuration."""
    freshness = data.get("freshness", {})
    availability = data.get("availability", {})
    volume = data.get("volume", {})
    quality = data.get("quality", {})

    return SLAConfig(
        freshness_max_age=freshness.get("max_age", "24h"),
        freshness_column=freshness.get("column", ""),
        availability_schedule=availability.get("schedule", "00:00-23:59 UTC"),
        availability_target_uptime=availability.get("target_uptime", "99%"),
        volume_expected_daily_records=volume.get("expected_daily_records"),
        volume_tolerance_percent=volume.get("tolerance_percent", 20),
        volume_alert_on_anomaly=volume.get("alert_on_anomaly", True),
        quality_minimum_completeness=quality.get("minimum_completeness", "99%"),
        quality_maximum_null_rate=quality.get("maximum_null_rate", "1%"),
    )


def _parse_governance_config(data: Dict[str, Any]) -> GovernanceConfig:
    """Parse governance configuration."""
    # Parse review
    review_data = data.get("review", {})
    review = ReviewConfig(
        cadence=review_data.get("cadence", "quarterly"),
        last_review=_parse_datetime(review_data.get("last_review")),
        next_review=_parse_datetime(review_data.get("next_review")),
        reviewers=review_data.get("reviewers", []),
    )

    # Parse change management
    cm_data = data.get("change_management", {})
    change_management = ChangeManagement(
        breaking_change_notice=cm_data.get("breaking_change_notice", "P30D"),
        deprecation_notice=cm_data.get("deprecation_notice", "P90D"),
        migration_support=cm_data.get("migration_support", True),
    )

    # Parse approval workflow
    aw_data = data.get("approval_workflow", {})
    approval_workflow = ApprovalWorkflow(
        schema_changes=aw_data.get("schema_changes", []),
        check_changes=aw_data.get("check_changes", []),
        compliance_changes=aw_data.get("compliance_changes", []),
    )

    return GovernanceConfig(
        review=review,
        change_management=change_management,
        approval_workflow=approval_workflow,
    )


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse a datetime string."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None
