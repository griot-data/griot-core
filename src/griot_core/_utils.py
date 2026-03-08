"""
Griot Core Internal Utilities

Shared helper functions used across multiple modules.
This module is internal and not part of the public API.
"""

from __future__ import annotations

from typing import Any, Union

__all__ = [
    "extract_base_type",
    "is_optional_type",
    "python_type_to_logical",
    "logical_type_to_python",
    "type_str_to_python",
]


def extract_base_type(type_hint: Any) -> type:
    """
    Extract the base type from a type hint.

    Handles Optional, Union, and generic types.

    Args:
        type_hint: A type hint (e.g., str, Optional[str], List[int])

    Returns:
        The base Python type

    Examples:
        >>> extract_base_type(str)
        <class 'str'>
        >>> extract_base_type(Optional[str])
        <class 'str'>
        >>> extract_base_type(List[int])
        <class 'list'>
    """
    origin = getattr(type_hint, "__origin__", None)

    if origin is Union:
        args = getattr(type_hint, "__args__", ())
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return extract_base_type(non_none[0])
        return type(None)

    if origin is not None:
        return origin

    if isinstance(type_hint, type):
        return type_hint

    return object


def is_optional_type(type_hint: Any) -> bool:
    """
    Check if a type hint is Optional (Union with None).

    Args:
        type_hint: A type hint

    Returns:
        True if the type hint is Optional

    Examples:
        >>> is_optional_type(str)
        False
        >>> is_optional_type(Optional[str])
        True
    """
    origin = getattr(type_hint, "__origin__", None)
    if origin is Union:
        args = getattr(type_hint, "__args__", ())
        return type(None) in args
    return False


def python_type_to_logical(python_type: type) -> str:
    """
    Convert a Python type to an ODCS logical type string.

    Args:
        python_type: A Python type

    Returns:
        ODCS logical type string

    Examples:
        >>> python_type_to_logical(str)
        'string'
        >>> python_type_to_logical(int)
        'integer'
    """
    type_map = {
        str: "string",
        int: "integer",
        float: "float",
        bool: "boolean",
        list: "array",
        dict: "object",
    }
    return type_map.get(python_type, "string")


def logical_type_to_python(logical_type: str) -> type:
    """
    Convert an ODCS logical type string to a Python type.

    Args:
        logical_type: ODCS logical type string

    Returns:
        Python type

    Examples:
        >>> logical_type_to_python('string')
        <class 'str'>
        >>> logical_type_to_python('integer')
        <class 'int'>
    """
    type_map = {
        "string": str,
        "integer": int,
        "float": float,
        "boolean": bool,
        "array": list,
        "object": dict,
        "date": str,
        "datetime": str,
        "any": object,
    }
    return type_map.get(logical_type.lower(), str)


def type_str_to_python(type_str: str) -> type:
    """
    Convert a type string to Python type.

    Alias for logical_type_to_python for backward compatibility.

    Args:
        type_str: Type string

    Returns:
        Python type
    """
    return logical_type_to_python(type_str)
