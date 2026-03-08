"""
JSON parser for contracts and schemas.

Parses JSON strings or dictionaries into griot-core dataclasses.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from griot_core.models import Contract, Schema

from .yaml_parser import _parse_contract_dict, _parse_schema_dict


def parse_contract_json(json_content: str) -> Contract:
    """
    Parse a JSON string into a Contract object.

    Args:
        json_content: JSON string containing the contract definition

    Returns:
        Contract object

    Raises:
        ValueError: If the JSON is invalid or missing required fields
    """
    data = json.loads(json_content)
    if not isinstance(data, dict):
        raise ValueError("Contract JSON must be a dictionary")
    return _parse_contract_dict(data)


def parse_schema_json(json_content: str) -> Schema:
    """
    Parse a JSON string into a Schema object.

    Args:
        json_content: JSON string containing the schema definition

    Returns:
        Schema object

    Raises:
        ValueError: If the JSON is invalid or missing required fields
    """
    data = json.loads(json_content)
    if not isinstance(data, dict):
        raise ValueError("Schema JSON must be a dictionary")
    return _parse_schema_dict(data)


def parse_contract_dict(data: Dict[str, Any]) -> Contract:
    """
    Parse a dictionary into a Contract object.

    Args:
        data: Dictionary containing the contract definition

    Returns:
        Contract object
    """
    return _parse_contract_dict(data)


def parse_schema_dict(data: Dict[str, Any]) -> Schema:
    """
    Parse a dictionary into a Schema object.

    Args:
        data: Dictionary containing the schema definition

    Returns:
        Schema object
    """
    return _parse_schema_dict(data)
