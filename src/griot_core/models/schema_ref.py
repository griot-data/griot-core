"""
SchemaRef model - Reference to a schema stored in the registry.

SchemaRef is used when contracts reference schemas by ID instead of
embedding inline schema definitions. This enables:
- Decoupled schema and contract management
- Schema reuse across multiple contracts
- Independent versioning
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass
class SchemaRef:
    """
    Reference to a schema stored in the registry.

    Used when a contract references an external schema by ID instead of
    defining it inline. The registry will resolve the reference to the
    actual schema definition at runtime.

    Attributes:
        schema_id: UUID or string identifier for the schema
        version: Optional semantic version (e.g., "1.0.0"). If None, uses latest.
        uri: Optional full URI (e.g., "registry://schemas/employees@1.0.0")

    Examples:
        # Simple reference by ID
        ref = SchemaRef(schema_id="sch-employees-001")

        # Reference with pinned version
        ref = SchemaRef(schema_id="sch-employees-001", version="1.0.0")

        # From URI
        ref = SchemaRef.from_uri("registry://schemas/employees@1.0.0")

        # From dict (as received from API)
        ref = SchemaRef.from_dict({"schema_id": "abc-123", "version": "1.0.0"})
    """

    schema_id: str
    version: str | None = None
    uri: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchemaRef:
        """
        Create SchemaRef from dictionary.

        Handles multiple formats:
        - {"schema_id": "...", "version": "..."}
        - {"$ref": "registry://schemas/name@version"}
        - {"ref": "registry://schemas/name@version"}

        Args:
            data: Dictionary containing schema reference data

        Returns:
            SchemaRef instance

        Raises:
            ValueError: If required fields are missing
        """
        # Handle $ref or ref format (URI-based)
        ref_uri = data.get("$ref") or data.get("ref")
        if ref_uri:
            return cls.from_uri(ref_uri)

        # Handle schema_id format
        schema_id = data.get("schema_id")
        if not schema_id:
            raise ValueError("SchemaRef requires 'schema_id' or '$ref'/'ref' field")

        # Handle both 'version' and 'schema_version' (registry format)
        version = data.get("version") or data.get("schema_version")

        return cls(
            schema_id=schema_id,
            version=version,
            uri=data.get("uri"),
        )

    @classmethod
    def from_uri(cls, uri: str) -> SchemaRef:
        """
        Create SchemaRef from a URI string.

        Parses URIs in formats:
        - "registry://schemas/name@version"
        - "registry://schemas/name" (no version)
        - "griot://schemas/name@version"

        Args:
            uri: URI string pointing to a schema

        Returns:
            SchemaRef instance

        Raises:
            ValueError: If URI format is invalid
        """
        # Pattern: protocol://type/name@version or protocol://type/name
        # Example: registry://schemas/employees@1.0.0
        pattern = r"^(?:registry|griot)://schemas/([^@]+)(?:@(.+))?$"
        match = re.match(pattern, uri)

        if match:
            schema_id = match.group(1)
            version = match.group(2)  # May be None
            return cls(schema_id=schema_id, version=version, uri=uri)

        # If doesn't match pattern, treat the whole URI as schema_id
        # This handles UUIDs and other ID formats
        return cls(schema_id=uri, uri=uri)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert SchemaRef to dictionary for serialization.

        Returns:
            Dictionary with schema_id and optional version/uri
        """
        result: dict[str, Any] = {"schema_id": self.schema_id}

        if self.version is not None:
            result["version"] = self.version

        if self.uri is not None:
            result["uri"] = self.uri

        return result

    def to_uri(self) -> str:
        """
        Generate a URI for this schema reference.

        Returns:
            URI string in format "registry://schemas/id@version"
        """
        if self.uri:
            return self.uri

        if self.version:
            return f"registry://schemas/{self.schema_id}@{self.version}"

        return f"registry://schemas/{self.schema_id}"

    def __str__(self) -> str:
        """Human-readable string representation."""
        if self.version:
            return f"SchemaRef({self.schema_id}@{self.version})"
        return f"SchemaRef({self.schema_id})"

    def __repr__(self) -> str:
        """Debug representation."""
        return (
            f"SchemaRef(schema_id={self.schema_id!r}, version={self.version!r}, uri={self.uri!r})"
        )
