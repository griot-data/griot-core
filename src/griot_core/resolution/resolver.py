"""
Contract inheritance resolver.

Resolves contract inheritance chains by fetching parent contracts
and merging them with child overrides.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

from griot_core.models import Contract

from .merge import merge_contracts_dict


class ContractFetcher(Protocol):
    """Protocol for fetching contracts by URI."""

    def fetch(self, uri: str) -> Dict[str, Any]:
        """
        Fetch a contract by URI.

        Args:
            uri: Contract URI (e.g., "griot://templates/base-contract@1.0")

        Returns:
            Contract as dictionary

        Raises:
            ContractNotFoundError: If the contract cannot be found
        """
        ...


class ContractNotFoundError(Exception):
    """Raised when a contract cannot be found."""

    def __init__(self, uri: str, message: str = ""):
        self.uri = uri
        self.message = message or f"Contract not found: {uri}"
        super().__init__(self.message)


class CircularInheritanceError(Exception):
    """Raised when a circular inheritance chain is detected."""

    def __init__(self, chain: List[str]):
        self.chain = chain
        cycle = " -> ".join(chain)
        super().__init__(f"Circular inheritance detected: {cycle}")


@dataclass
class ResolvedContract:
    """
    Result of resolving a contract's inheritance chain.

    Attributes:
        resolved_definition: The fully merged contract (what runs)
        override_definition: The original contract (what user wrote)
        inheritance_chain: List of parent contract URIs in order
        resolution_warnings: Any warnings encountered during resolution
    """

    resolved_definition: Dict[str, Any]
    override_definition: Dict[str, Any]
    inheritance_chain: List[str] = field(default_factory=list)
    resolution_warnings: List[str] = field(default_factory=list)


class ContractResolver:
    """
    Resolves contract inheritance chains and produces fully-merged contracts.

    The resolver fetches parent contracts and merges them with child
    overrides according to the inheritance rules.

    Resolution order (later overrides earlier):
    1. griot://templates/base-contract (implicit root)
    2. Parent contracts (recursively resolved)
    3. This contract's overrides

    Example:
        >>> resolver = ContractResolver(registry_client)
        >>> resolved = resolver.resolve(contract_dict)
        >>> print(resolved.inheritance_chain)
        ['griot://templates/base-contract@1.0', 'griot://templates/pii-contract@1.0']
    """

    # Default base contract that all contracts implicitly extend
    DEFAULT_BASE = "griot://templates/base-contract@1.0"

    def __init__(
        self,
        fetcher: Optional[ContractFetcher] = None,
        default_base: Optional[str] = None,
        max_depth: int = 10,
    ):
        """
        Initialize the resolver.

        Args:
            fetcher: Contract fetcher for retrieving parent contracts
            default_base: Override the default base contract URI
            max_depth: Maximum inheritance depth to prevent runaway chains
        """
        self._fetcher = fetcher
        self._default_base = default_base or self.DEFAULT_BASE
        self._max_depth = max_depth
        self._resolution_cache: Dict[str, ResolvedContract] = {}

    def resolve(
        self,
        contract: Dict[str, Any],
        skip_base: bool = False,
    ) -> ResolvedContract:
        """
        Resolve a contract by merging with its parent chain.

        Args:
            contract: The contract dictionary to resolve
            skip_base: If True, don't implicitly extend base contract

        Returns:
            ResolvedContract containing:
            - resolved_definition: The fully merged contract
            - override_definition: The original contract
            - inheritance_chain: List of parent URIs

        Raises:
            ContractNotFoundError: If a parent contract cannot be found
            CircularInheritanceError: If inheritance chain has cycles
        """
        override_definition = contract.copy()
        inheritance_chain: List[str] = []
        warnings: List[str] = []

        # Build the inheritance chain
        extends = contract.get("extends")

        if extends:
            # Explicit parent - resolve recursively
            resolved = self._resolve_with_parent(
                contract,
                extends,
                inheritance_chain,
                warnings,
                visited=set(),
                depth=0,
            )
        elif not skip_base and self._fetcher:
            # Implicit base contract
            try:
                base = self._fetcher.fetch(self._default_base)
                inheritance_chain.append(self._default_base)
                resolved = merge_contracts_dict(base, contract)
            except ContractNotFoundError:
                # No base available, use contract as-is
                warnings.append(f"Base contract not found: {self._default_base}")
                resolved = contract.copy()
        else:
            # No inheritance
            resolved = contract.copy()

        return ResolvedContract(
            resolved_definition=resolved,
            override_definition=override_definition,
            inheritance_chain=inheritance_chain,
            resolution_warnings=warnings,
        )

    def _resolve_with_parent(
        self,
        contract: Dict[str, Any],
        parent_uri: str,
        chain: List[str],
        warnings: List[str],
        visited: set,
        depth: int,
    ) -> Dict[str, Any]:
        """
        Recursively resolve a contract with its parent.

        Args:
            contract: The child contract
            parent_uri: URI of the parent contract
            chain: Inheritance chain being built
            warnings: List to collect warnings
            visited: Set of visited URIs for cycle detection
            depth: Current recursion depth

        Returns:
            Merged contract dictionary
        """
        # Check depth
        if depth >= self._max_depth:
            warnings.append(
                f"Maximum inheritance depth ({self._max_depth}) reached. Stopping at {parent_uri}"
            )
            return contract.copy()

        # Check for cycles
        if parent_uri in visited:
            raise CircularInheritanceError(list(visited) + [parent_uri])

        visited.add(parent_uri)

        # Check cache
        if parent_uri in self._resolution_cache:
            cached = self._resolution_cache[parent_uri]
            chain.extend(cached.inheritance_chain)
            chain.append(parent_uri)
            return merge_contracts_dict(cached.resolved_definition, contract)

        # Fetch parent
        if not self._fetcher:
            warnings.append(f"No fetcher available to resolve: {parent_uri}")
            return contract.copy()

        try:
            parent = self._fetcher.fetch(parent_uri)
        except ContractNotFoundError as e:
            warnings.append(f"Parent contract not found: {e.uri}")
            return contract.copy()

        # Check if parent also extends
        parent_extends = parent.get("extends")
        if parent_extends:
            # Recursively resolve parent first
            resolved_parent = self._resolve_with_parent(
                parent,
                parent_extends,
                chain,
                warnings,
                visited.copy(),
                depth + 1,
            )
        else:
            # Parent is the root (or extends implicit base)
            if self._fetcher:
                try:
                    base = self._fetcher.fetch(self._default_base)
                    chain.append(self._default_base)
                    resolved_parent = merge_contracts_dict(base, parent)
                except ContractNotFoundError:
                    resolved_parent = parent.copy()
            else:
                resolved_parent = parent.copy()

        chain.append(parent_uri)

        # Merge parent with child
        return merge_contracts_dict(resolved_parent, contract)

    def resolve_contract_object(
        self,
        contract: Contract,
        skip_base: bool = False,
    ) -> ResolvedContract:
        """
        Resolve a Contract object by merging with its parent chain.

        This is a convenience method that converts the Contract to dict,
        resolves it, and returns the result.

        Args:
            contract: The Contract object to resolve
            skip_base: If True, don't implicitly extend base contract

        Returns:
            ResolvedContract with resolved definition as dictionary
        """
        # Convert Contract to dict for resolution
        contract_dict = self._contract_to_dict(contract)
        return self.resolve(contract_dict, skip_base=skip_base)

    def _contract_to_dict(self, contract: Contract) -> Dict[str, Any]:
        """Convert a Contract object to a dictionary."""
        result: Dict[str, Any] = {
            "apiVersion": contract.api_version,
            "kind": contract.kind,
            "id": contract.id,
            "name": contract.name,
            "version": contract.version,
            "status": contract.status.value,
        }

        if contract.extends:
            result["extends"] = contract.extends

        if contract.owner:
            result["owner"] = contract.owner

        if contract.data_product:
            result["dataProduct"] = contract.data_product

        if contract.tags:
            result["tags"] = contract.tags

        # Add other fields as needed for inheritance resolution
        # The full serialization would be more comprehensive

        return result

    def clear_cache(self) -> None:
        """Clear the resolution cache."""
        self._resolution_cache.clear()

    def set_fetcher(self, fetcher: ContractFetcher) -> None:
        """Set the contract fetcher."""
        self._fetcher = fetcher


class InMemoryFetcher:
    """
    Simple in-memory contract fetcher for testing.

    Stores contracts in a dictionary keyed by URI.
    """

    def __init__(self, contracts: Optional[Dict[str, Dict[str, Any]]] = None):
        self._contracts: Dict[str, Dict[str, Any]] = contracts or {}

    def fetch(self, uri: str) -> Dict[str, Any]:
        """Fetch a contract by URI."""
        if uri not in self._contracts:
            raise ContractNotFoundError(uri)
        return self._contracts[uri].copy()

    def add(self, uri: str, contract: Dict[str, Any]) -> None:
        """Add a contract to the store."""
        self._contracts[uri] = contract

    def remove(self, uri: str) -> None:
        """Remove a contract from the store."""
        self._contracts.pop(uri, None)
