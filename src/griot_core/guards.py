"""Contract immutability guards.

Enforces that schema and property changes are only allowed when a
contract is in DRAFT status.  Any attempt to modify schema data on a
non-draft contract should be rejected.
"""

from __future__ import annotations

from griot_core.exceptions import GriotError
from griot_core.types import ContractStatus


class ContractImmutableError(GriotError):
    """Raised when a schema change is attempted on a non-draft contract."""

    def __init__(self, contract_id: str, current_status: str) -> None:
        self.contract_id = contract_id
        self.current_status = current_status
        super().__init__(
            f"Cannot modify schema for contract '{contract_id}': "
            f"status is '{current_status}' (must be 'draft')"
        )


def can_modify_schema(status: str | ContractStatus) -> bool:
    """Return True only if the contract status allows schema changes.

    Args:
        status: Current contract status (string or enum).

    Returns:
        True if the status is DRAFT.
    """
    status_value = status.value if isinstance(status, ContractStatus) else str(status)
    return status_value == ContractStatus.DRAFT.value


def assert_can_modify_schema(
    contract_id: str,
    status: str | ContractStatus,
) -> None:
    """Raise ContractImmutableError if the contract is not in DRAFT status.

    Args:
        contract_id: The contract identifier (for the error message).
        status: Current contract status.

    Raises:
        ContractImmutableError: If status is not DRAFT.
    """
    if not can_modify_schema(status):
        status_value = status.value if isinstance(status, ContractStatus) else str(status)
        raise ContractImmutableError(contract_id, status_value)
