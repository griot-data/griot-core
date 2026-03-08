"""
Griot Core Privacy Utilities.

This module provides PII pattern definitions and validation utilities.
Patterns are utility data that can be bundled into executors or passed
as parameters at runtime.
"""

from __future__ import annotations

from .patterns import PIIPattern

__all__ = [
    "PIIPattern",
]
