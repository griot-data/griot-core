"""
Parser registry for managing tool results parsers.

Follows the ConnectorRegistry pattern from connectors/registry.py.
Provides a central registry for registering, discovering, and
instantiating tool results parsers by tool name.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type

from .base import ToolResultsParser


class ParserNotFoundError(Exception):
    """Raised when a parser for a tool is not registered."""

    pass


class ParserRegistrationError(Exception):
    """Raised when parser registration fails."""

    pass


@dataclass
class ParserInfo:
    """Information about a registered parser.

    Attributes:
        tool_name: Name of the tool (e.g., "dbt", "soda").
        parser_class: The parser implementation class.
        description: Description of the parser.
        file_patterns: File patterns this parser handles (e.g., ["run_results.json"]).
    """

    tool_name: str
    parser_class: Type
    description: str = ""
    file_patterns: List[str] = field(default_factory=list)


class ParserRegistry:
    """Registry for tool results parsers.

    Provides a central registry for registering, discovering, and
    instantiating parsers. Each tool name can only be registered once.

    Example:
        >>> registry = ParserRegistry()
        >>> registry.register(
        ...     tool_name="dbt",
        ...     parser_class=DbtResultsParser,
        ...     description="Parse dbt run_results.json",
        ...     file_patterns=["run_results.json"],
        ... )
        >>> parser = registry.create_parser("dbt", schema_mapping={"dim_products": "products"})
    """

    def __init__(self) -> None:
        self._parsers: Dict[str, ParserInfo] = {}
        self._factories: Dict[str, Callable[..., ToolResultsParser]] = {}

    def register(
        self,
        tool_name: str,
        parser_class: Type,
        description: str = "",
        file_patterns: Optional[List[str]] = None,
        factory: Optional[Callable[..., ToolResultsParser]] = None,
    ) -> None:
        """Register a parser for a tool.

        Args:
            tool_name: Name of the tool (e.g., "dbt").
            parser_class: The parser implementation class.
            description: Description of the parser.
            file_patterns: File patterns this parser handles.
            factory: Optional custom factory function for creating instances.

        Raises:
            ParserRegistrationError: If tool name already registered.
        """
        if tool_name in self._parsers:
            raise ParserRegistrationError(f"Parser for tool '{tool_name}' is already registered")

        info = ParserInfo(
            tool_name=tool_name,
            parser_class=parser_class,
            description=description,
            file_patterns=file_patterns or [],
        )

        self._parsers[tool_name] = info

        if factory:
            self._factories[tool_name] = factory

    def create_parser(
        self,
        tool_name: str,
        **kwargs: Any,
    ) -> ToolResultsParser:
        """Create a parser instance for the given tool.

        Args:
            tool_name: Name of the tool.
            **kwargs: Arguments passed to the parser constructor.

        Returns:
            Instantiated parser.

        Raises:
            ParserNotFoundError: If tool name not registered.
        """
        if tool_name not in self._parsers:
            available = ", ".join(self._parsers.keys()) or "(none)"
            raise ParserNotFoundError(
                f"No parser registered for tool '{tool_name}'. Available parsers: {available}"
            )

        if tool_name in self._factories:
            return self._factories[tool_name](**kwargs)

        info = self._parsers[tool_name]
        return info.parser_class(**kwargs)

    def list_parsers(self) -> List[ParserInfo]:
        """List all registered parsers."""
        return list(self._parsers.values())

    def is_registered(self, tool_name: str) -> bool:
        """Check if a parser is registered for a tool."""
        return tool_name in self._parsers


# ---------------------------------------------------------------------------
# Global default registry
# ---------------------------------------------------------------------------

_default_registry: Optional[ParserRegistry] = None


def _register_builtins(registry: ParserRegistry) -> None:
    """Register built-in parsers (dbt)."""
    from .parsers.dbt import DbtResultsParser

    registry.register(
        tool_name="dbt",
        parser_class=DbtResultsParser,
        description="Parse dbt run_results.json into ToolValidationReport",
        file_patterns=["run_results.json"],
    )


def get_default_parser_registry() -> ParserRegistry:
    """Get the default parser registry with built-in parsers registered.

    Returns:
        The global default ParserRegistry instance.
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = ParserRegistry()
        _register_builtins(_default_registry)
    return _default_registry
