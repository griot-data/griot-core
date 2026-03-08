"""
Deep merge utilities for contract inheritance.

Provides merge logic for combining parent and child contracts
with proper override semantics.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, TypeVar

T = TypeVar("T")


def deep_merge(
    parent: Dict[str, Any], child: Dict[str, Any], list_strategy: str = "extend"
) -> Dict[str, Any]:
    """
    Deep merge two dictionaries with child values overriding parent.

    Special handling:
    - Scalars: Child replaces parent
    - Dicts: Recursively merged (child keys override parent)
    - Lists: Strategy-based (extend, replace, or merge by key)

    Args:
        parent: The parent/base dictionary
        child: The child/override dictionary
        list_strategy: How to handle lists:
            - "extend": Child list extends parent list
            - "replace": Child list replaces parent list
            - "merge": Lists of dicts merged by 'name' or 'id' key

    Returns:
        Merged dictionary with child overrides applied
    """
    result = deepcopy(parent)

    for key, child_value in child.items():
        if key not in result:
            # New key from child
            result[key] = deepcopy(child_value)
        elif child_value is None:
            # Explicit null in child clears the value
            result[key] = None
        elif isinstance(child_value, dict) and isinstance(result[key], dict):
            # Recursive merge for nested dicts
            result[key] = deep_merge(result[key], child_value, list_strategy)
        elif isinstance(child_value, list) and isinstance(result[key], list):
            # List merge based on strategy
            result[key] = _merge_lists(result[key], child_value, list_strategy)
        else:
            # Scalar override
            result[key] = deepcopy(child_value)

    return result


def _merge_lists(parent_list: List[Any], child_list: List[Any], strategy: str) -> List[Any]:
    """
    Merge two lists based on strategy.

    Args:
        parent_list: The parent list
        child_list: The child list
        strategy: Merge strategy (extend, replace, merge)

    Returns:
        Merged list
    """
    if strategy == "replace":
        return deepcopy(child_list)

    if strategy == "extend":
        # Child extends parent (append non-duplicates)
        result = deepcopy(parent_list)
        existing = _get_list_identifiers(result)
        for item in child_list:
            item_id = _get_item_identifier(item)
            if item_id not in existing:
                result.append(deepcopy(item))
        return result

    if strategy == "merge":
        # Merge lists of dicts by name/id
        if not parent_list or not child_list:
            return deepcopy(child_list) if child_list else deepcopy(parent_list)

        if isinstance(parent_list[0], dict) and isinstance(child_list[0], dict):
            return _merge_dict_lists(parent_list, child_list)
        else:
            # For non-dict lists, use extend strategy
            return _merge_lists(parent_list, child_list, "extend")

    # Default: extend
    return _merge_lists(parent_list, child_list, "extend")


def _merge_dict_lists(
    parent_list: List[Dict[str, Any]], child_list: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Merge two lists of dicts by name or id key.

    Items with matching name/id are merged.
    Items only in parent are kept.
    Items only in child are added.
    """
    result = deepcopy(parent_list)
    parent_by_id: Dict[str, int] = {}

    # Index parent items by name or id
    for idx, item in enumerate(result):
        item_id = _get_item_identifier(item)
        if item_id:
            parent_by_id[item_id] = idx

    # Process child items
    for child_item in child_list:
        child_id = _get_item_identifier(child_item)
        if child_id and child_id in parent_by_id:
            # Merge with existing item
            parent_idx = parent_by_id[child_id]
            result[parent_idx] = deep_merge(result[parent_idx], child_item)
        else:
            # Add new item
            result.append(deepcopy(child_item))

    return result


def _get_item_identifier(item: Any) -> str:
    """Get the identifier for a list item."""
    if isinstance(item, dict):
        return item.get("name") or item.get("id") or ""
    elif isinstance(item, str):
        return item
    else:
        return ""


def _get_list_identifiers(items: List[Any]) -> set:
    """Get all identifiers from a list of items."""
    return {_get_item_identifier(item) for item in items if _get_item_identifier(item)}


def merge_contracts_dict(parent: Dict[str, Any], child: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two contract dictionaries with contract-specific rules.

    Rules:
    - Identity fields (id, name) come from child
    - Lists (checks, schema_refs, servers): Child extends parent
    - Dicts (executors.profiles): Child overrides specific keys
    - Scalars: Child replaces parent

    Args:
        parent: Parent contract dictionary
        child: Child contract dictionary

    Returns:
        Merged contract dictionary
    """
    result = deep_merge(parent, child, list_strategy="merge")

    # Ensure identity fields come from child
    identity_fields = ["id", "name", "version", "status"]
    for field in identity_fields:
        if field in child:
            result[field] = child[field]

    # Handle extends - remove it from resolved contract
    result.pop("extends", None)

    return result
