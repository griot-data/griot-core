"""
Check model for validation definitions.

Checks define validation rules that are executed against data.
Each check specifies an executor URI that points to a WASM module
or container image that performs the actual validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .enums import CheckCategory, CheckType, Severity


@dataclass
class CheckCondition:
    """
    Condition for when a check should run.

    Used to conditionally execute checks based on environment,
    profile, or other runtime parameters.
    """

    environment: Optional[List[str]] = None  # ["dev", "staging"]
    profile: Optional[List[str]] = None  # ["data_engineering", "privacy_audit"]
    tags: Optional[List[str]] = None  # Only run if data has these tags


@dataclass
class Check:
    """
    A validation check definition.

    Checks are validation rules that specify what to validate and how.
    The actual validation logic is encapsulated in executors (WASM modules
    or container images) that are referenced by URI.

    Attributes:
        name: Unique name for this check (e.g., "salary_range_valid")
        description: Human-readable description of what this check validates
        type: The category of check (data_quality, privacy, schema)
        executor: URI pointing to the executor that runs this check
                  Examples:
                  - "registry://executors/null-check@1.0"
                  - "file://path/to/executor.wasm"
                  - "oci://ghcr.io/griot/checks/drift:1.0"
        parameters: Key-value pairs passed to the executor
        check_function: Name of the exported function to call inside the
                        executor module.  Defaults to "validate" for backward
                        compatibility.  For multi-function WASM bundles this
                        selects the specific check (e.g. "null_check").
        severity: How critical a failure of this check is
        when: Optional condition for when this check should run
        tags: Tags for categorizing and filtering checks
    """

    name: str
    description: str
    type: CheckType
    executor: str  # URI to executor (WASM or container)
    parameters: Dict[str, Any] = field(default_factory=dict)
    check_function: str = "validate"
    severity: Severity = Severity.WARNING
    when: Optional[CheckCondition] = None
    tags: List[str] = field(default_factory=list)
    category: CheckCategory = CheckCategory.CUSTOM
    columns: List[str] = field(default_factory=list)
