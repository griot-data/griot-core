"""
Griot Core Data Connectors.

This module provides data source connectors that return data
in Arrow IPC format for use by executors.

All connectors implement the DataConnector protocol and return
Arrow IPC bytes, not DataFrame objects. This keeps griot-core
framework-agnostic.
"""

from __future__ import annotations

from .base import (
    BaseConnector,
    ConnectionTestResult,
    ConnectorConfig,
    ConnectorType,
    DataConnector,
    FetchResult,
)
from .registry import (
    ConnectorInfo,
    ConnectorNotFoundError,
    ConnectorRegistrationError,
    ConnectorRegistry,
    get_default_registry,
    register_connector,
)

__all__ = [
    # Base types
    "DataConnector",
    "BaseConnector",
    "ConnectorConfig",
    "ConnectorType",
    "ConnectionTestResult",
    "FetchResult",
    # Registry
    "ConnectorRegistry",
    "ConnectorInfo",
    "ConnectorNotFoundError",
    "ConnectorRegistrationError",
    "get_default_registry",
    "register_connector",
]
