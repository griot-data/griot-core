"""
Griot Core Exception Hierarchy

Custom exceptions for the Griot data contract system.
All exceptions inherit from GriotError base class.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "GriotError",
    "ValidationError",
    "ContractNotFoundError",
    "ContractParseError",
    "BreakingChangeError",
    "ConstraintError",
    "ContractImmutableError",
]

from griot_core.types import FieldValidationError


class GriotError(Exception):
    """Base exception for all Griot errors."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.message!r})"


class ValidationError(GriotError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        errors: list[FieldValidationError] | None = None,
    ) -> None:
        super().__init__(message)
        self.errors: list[FieldValidationError] = errors or []

    @property
    def error_count(self) -> int:
        """Return the number of validation errors."""
        return len(self.errors)

    def __str__(self) -> str:
        if self.errors:
            return f"{self.message} ({self.error_count} errors)"
        return self.message


class ContractNotFoundError(GriotError):
    """Raised when a contract file cannot be found."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"Contract not found: {path}")


class ContractParseError(GriotError):
    """Raised when contract YAML or Python definition is invalid."""

    def __init__(
        self,
        message: str,
        source: str | None = None,
        line: int | None = None,
        column: int | None = None,
    ) -> None:
        self.source = source
        self.line = line
        self.column = column

        location = ""
        if source:
            location = f" in {source}"
        if line is not None:
            location += f" at line {line}"
            if column is not None:
                location += f", column {column}"

        super().__init__(f"Contract parse error{location}: {message}")


class BreakingChangeError(GriotError):
    """Raised when contract diff contains breaking changes."""

    def __init__(self, message: str, diff: Any | None = None) -> None:
        super().__init__(message)
        self.diff = diff

    def __str__(self) -> str:
        if self.diff:
            return f"{self.message} (see diff for details)"
        return self.message


class ContractImmutableError(GriotError):
    """Raised when a schema change is attempted on a non-draft contract."""

    def __init__(self, contract_id: str, current_status: str) -> None:
        self.contract_id = contract_id
        self.current_status = current_status
        super().__init__(
            f"Cannot modify schema for contract '{contract_id}': "
            f"status is '{current_status}' (must be 'draft')"
        )


class ConstraintError(GriotError):
    """Raised when a field constraint is invalid or cannot be applied."""

    def __init__(
        self,
        constraint: str,
        value: Any,
        field: str | None = None,
        reason: str | None = None,
    ) -> None:
        self.constraint = constraint
        self.value = value
        self.field = field
        self.reason = reason

        msg = f"Invalid constraint '{constraint}'"
        if field:
            msg += f" on field '{field}'"
        msg += f": value={value!r}"
        if reason:
            msg += f" ({reason})"

        super().__init__(msg)
