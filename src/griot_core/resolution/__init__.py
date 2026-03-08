"""
Griot Core Resolution Module.

This module provides contract inheritance resolution,
merging parent contracts with child overrides.
"""

from __future__ import annotations

from .merge import deep_merge
from .resolver import (
    CircularInheritanceError,
    ContractNotFoundError,
    ContractResolver,
    InMemoryFetcher,
    ResolvedContract,
)

__all__ = [
    "ContractResolver",
    "ResolvedContract",
    "InMemoryFetcher",
    "ContractNotFoundError",
    "CircularInheritanceError",
    "deep_merge",
]
