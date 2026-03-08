"""
Connector registry for managing data source connectors.

Provides a central registry for registering, discovering, and
instantiating data connectors by type.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type

from .base import (
    ConnectionTestResult,
    ConnectorConfig,
    ConnectorType,
    DataConnector,
)


class ConnectorNotFoundError(Exception):
    """Raised when a connector type is not registered."""

    pass


class ConnectorRegistrationError(Exception):
    """Raised when connector registration fails."""

    pass


@dataclass
class ConnectorInfo:
    """
    Information about a registered connector.

    Attributes:
        connector_type: Type of connector
        connector_class: The connector implementation class
        name: Human-readable name
        description: Description of the connector
        required_params: Required connection parameters
        optional_params: Optional connection parameters with defaults
        supports_sampling: Whether the connector supports sampling
        supports_arrow_native: Whether the data source has native Arrow support
    """

    connector_type: ConnectorType
    connector_class: Type[DataConnector]
    name: str
    description: str = ""
    required_params: List[str] = field(default_factory=list)
    optional_params: Dict[str, Any] = field(default_factory=dict)
    supports_sampling: bool = True
    supports_arrow_native: bool = False


class ConnectorRegistry:
    """
    Registry for data source connectors.

    Provides a central registry for registering, discovering, and
    instantiating data connectors. Each connector type can only be
    registered once.

    Example:
        >>> registry = ConnectorRegistry()
        >>> registry.register(
        ...     connector_type=ConnectorType.SNOWFLAKE,
        ...     connector_class=SnowflakeConnector,
        ...     name="Snowflake",
        ...     description="Snowflake Data Cloud connector",
        ...     required_params=["account", "user", "warehouse", "database"],
        ... )
        >>> connector = registry.create_connector(
        ...     connector_type=ConnectorType.SNOWFLAKE,
        ...     config=ConnectorConfig(
        ...         connector_type=ConnectorType.SNOWFLAKE,
        ...         connection_params={"account": "...", ...}
        ...     )
        ... )
    """

    def __init__(self) -> None:
        """Initialize the connector registry."""
        self._connectors: Dict[ConnectorType, ConnectorInfo] = {}
        self._factories: Dict[ConnectorType, Callable[[ConnectorConfig], DataConnector]] = {}

    def register(
        self,
        connector_type: ConnectorType,
        connector_class: Type[DataConnector],
        name: str,
        description: str = "",
        required_params: Optional[List[str]] = None,
        optional_params: Optional[Dict[str, Any]] = None,
        supports_sampling: bool = True,
        supports_arrow_native: bool = False,
        factory: Optional[Callable[[ConnectorConfig], DataConnector]] = None,
    ) -> None:
        """
        Register a connector type.

        Args:
            connector_type: Type of connector to register
            connector_class: The connector implementation class
            name: Human-readable name
            description: Description of the connector
            required_params: Required connection parameters
            optional_params: Optional parameters with default values
            supports_sampling: Whether the connector supports sampling
            supports_arrow_native: Whether the data source has native Arrow support
            factory: Optional custom factory function for creating instances

        Raises:
            ConnectorRegistrationError: If connector type already registered
        """
        if connector_type in self._connectors:
            raise ConnectorRegistrationError(
                f"Connector type '{connector_type.value}' is already registered"
            )

        info = ConnectorInfo(
            connector_type=connector_type,
            connector_class=connector_class,
            name=name,
            description=description,
            required_params=required_params or [],
            optional_params=optional_params or {},
            supports_sampling=supports_sampling,
            supports_arrow_native=supports_arrow_native,
        )

        self._connectors[connector_type] = info

        if factory:
            self._factories[connector_type] = factory

    def unregister(self, connector_type: ConnectorType) -> None:
        """
        Unregister a connector type.

        Args:
            connector_type: Type of connector to unregister

        Raises:
            ConnectorNotFoundError: If connector type not registered
        """
        if connector_type not in self._connectors:
            raise ConnectorNotFoundError(
                f"Connector type '{connector_type.value}' is not registered"
            )

        del self._connectors[connector_type]
        self._factories.pop(connector_type, None)

    def get_connector_info(self, connector_type: ConnectorType) -> ConnectorInfo:
        """
        Get information about a registered connector.

        Args:
            connector_type: Type of connector

        Returns:
            ConnectorInfo for the connector type

        Raises:
            ConnectorNotFoundError: If connector type not registered
        """
        if connector_type not in self._connectors:
            raise ConnectorNotFoundError(
                f"Connector type '{connector_type.value}' is not registered"
            )

        return self._connectors[connector_type]

    def create_connector(
        self,
        connector_type: ConnectorType,
        config: ConnectorConfig,
    ) -> DataConnector:
        """
        Create a connector instance.

        Args:
            connector_type: Type of connector to create
            config: Connector configuration

        Returns:
            Instantiated connector

        Raises:
            ConnectorNotFoundError: If connector type not registered
            ValueError: If required parameters are missing
        """
        if connector_type not in self._connectors:
            raise ConnectorNotFoundError(
                f"Connector type '{connector_type.value}' is not registered"
            )

        info = self._connectors[connector_type]

        # Validate required parameters
        missing = []
        for param in info.required_params:
            if param not in config.connection_params:
                missing.append(param)

        if missing:
            raise ValueError(f"Missing required parameters for {info.name}: {', '.join(missing)}")

        # Use custom factory if provided
        if connector_type in self._factories:
            return self._factories[connector_type](config)

        # Default: instantiate the class with config
        return info.connector_class(config)  # type: ignore[call-arg]

    def create_from_dict(
        self,
        connector_type: str,
        connection_params: Dict[str, Any],
        **kwargs: Any,
    ) -> DataConnector:
        """
        Create a connector from a type string and parameters dict.

        Convenience method for creating connectors from configuration files.

        Args:
            connector_type: Type string (e.g., "snowflake", "bigquery")
            connection_params: Connection parameters
            **kwargs: Additional ConnectorConfig options

        Returns:
            Instantiated connector

        Raises:
            ValueError: If connector type string is invalid
        """
        try:
            ct = ConnectorType(connector_type.lower())
        except ValueError:
            valid = [c.value for c in ConnectorType]
            raise ValueError(
                f"Invalid connector type '{connector_type}'. Valid types: {', '.join(valid)}"
            )

        config = ConnectorConfig(
            connector_type=ct,
            connection_params=connection_params,
            **kwargs,
        )

        return self.create_connector(ct, config)

    def list_connectors(self) -> List[ConnectorInfo]:
        """
        List all registered connectors.

        Returns:
            List of ConnectorInfo for all registered connectors
        """
        return list(self._connectors.values())

    def list_connector_types(self) -> List[ConnectorType]:
        """
        List all registered connector types.

        Returns:
            List of registered ConnectorType values
        """
        return list(self._connectors.keys())

    def is_registered(self, connector_type: ConnectorType) -> bool:
        """
        Check if a connector type is registered.

        Args:
            connector_type: Type to check

        Returns:
            True if registered, False otherwise
        """
        return connector_type in self._connectors

    def validate_config(
        self,
        connector_type: ConnectorType,
        config: ConnectorConfig,
    ) -> List[str]:
        """
        Validate a connector configuration.

        Args:
            connector_type: Type of connector
            config: Configuration to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if connector_type not in self._connectors:
            errors.append(f"Unknown connector type: {connector_type.value}")
            return errors

        info = self._connectors[connector_type]

        # Check required params
        for param in info.required_params:
            if param not in config.connection_params:
                errors.append(f"Missing required parameter: {param}")

        # Check type mismatch
        if config.connector_type != connector_type:
            errors.append(
                f"Config connector_type ({config.connector_type.value}) "
                f"does not match requested type ({connector_type.value})"
            )

        return errors

    async def test_connector(
        self,
        connector_type: ConnectorType,
        config: ConnectorConfig,
    ) -> ConnectionTestResult:
        """
        Test a connector configuration.

        Creates a temporary connector instance and tests the connection.

        Args:
            connector_type: Type of connector
            config: Configuration to test

        Returns:
            ConnectionTestResult with success status
        """
        try:
            connector = self.create_connector(connector_type, config)
            return await connector.test_connection()
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message="Failed to create connector",
                errors=[str(e)],
            )


# Global default registry instance
_default_registry: Optional[ConnectorRegistry] = None


def get_default_registry() -> ConnectorRegistry:
    """
    Get the default connector registry.

    Returns:
        The global default ConnectorRegistry instance
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = ConnectorRegistry()
    return _default_registry


def register_connector(
    connector_type: ConnectorType,
    connector_class: Type[DataConnector],
    name: str,
    **kwargs: Any,
) -> None:
    """
    Register a connector in the default registry.

    Convenience function for registering connectors.

    Args:
        connector_type: Type of connector
        connector_class: Connector implementation class
        name: Human-readable name
        **kwargs: Additional registration options
    """
    get_default_registry().register(
        connector_type=connector_type,
        connector_class=connector_class,
        name=name,
        **kwargs,
    )
