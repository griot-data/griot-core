"""
Griot Core Validation Module.

This module provides the validation engine for running checks
against data using executor runtime.
"""

from __future__ import annotations

from .engine import ValidationEngine
from .profile import (
    ProfileResolver,
    ResolvedCheck,
    ResolvedProfile,
)
from .result import (
    CheckExecutionResult,
    CheckStatus,
    SchemaValidationResult,
    ValidationMode,
    ValidationResult,
    ValidationSummary,
)
from .types import (
    ProfileConfig,
    ValidationContext,
    ValidationOptions,
)

__all__ = [
    # Results
    "ValidationResult",
    "SchemaValidationResult",
    "CheckExecutionResult",
    "ValidationSummary",
    "ValidationMode",
    "CheckStatus",
    # Types
    "ValidationContext",
    "ValidationOptions",
    "ProfileConfig",
    # Profile
    "ProfileResolver",
    "ResolvedProfile",
    "ResolvedCheck",
    # Engine
    "ValidationEngine",
]
