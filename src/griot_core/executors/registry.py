"""
Executor registry for fetching executor specifications.

The registry manages executor specifications and provides caching
for efficient executor lookups.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol
from urllib.parse import urlparse

from griot_core.models.enums import Runtime

from .types import ExecutorSpec


class URIScheme(str, Enum):
    """Supported URI schemes for executor references."""

    REGISTRY = "registry"
    FILE = "file"
    OCI = "oci"
    HTTP = "http"
    HTTPS = "https"


@dataclass
class ExecutorManifest:
    """
    Manifest describing an executor package.

    Contains metadata about the executor and how to run it.
    """

    id: str
    version: str
    runtime: Runtime
    artifact_path: str  # Path within the package
    entry_point: str = "validate"  # Function/entrypoint name
    description: str = ""
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    dependencies: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    checksum: Optional[str] = None  # SHA256 of artifact


class ExecutorFetcher(Protocol):
    """Protocol for fetching executor artifacts."""

    async def fetch_spec(self, uri: str) -> ExecutorSpec:
        """Fetch executor specification from URI."""
        ...

    async def fetch_artifact(self, spec: ExecutorSpec) -> bytes:
        """Fetch the actual executor artifact (WASM or container reference)."""
        ...


class ExecutorNotFoundError(Exception):
    """Raised when an executor cannot be found."""

    def __init__(self, uri: str, message: str = ""):
        self.uri = uri
        self.message = message or f"Executor not found: {uri}"
        super().__init__(self.message)


class InvalidExecutorURIError(Exception):
    """Raised when an executor URI is invalid."""

    def __init__(self, uri: str, reason: str = ""):
        self.uri = uri
        self.reason = reason
        super().__init__(f"Invalid executor URI '{uri}': {reason}")


@dataclass
class CachedExecutor:
    """Cached executor with metadata."""

    spec: ExecutorSpec
    artifact_path: Optional[Path] = None
    fetched_at: Optional[float] = None
    checksum: Optional[str] = None


class ExecutorRegistry:
    """
    Registry for managing and fetching executor specifications.

    Supports multiple URI schemes:
    - registry://executors/null-check@1.0  -> Fetch from Griot registry
    - file://path/to/executor.wasm         -> Local WASM file
    - oci://ghcr.io/griot/checks/drift:1.0 -> OCI container image

    Example:
        >>> registry = ExecutorRegistry(cache_dir=Path("/tmp/executors"))
        >>> spec = await registry.get_executor("registry://executors/null-check@1.0")
        >>> print(spec.runtime)  # Runtime.WASM
    """

    # Built-in executor definitions
    BUILTIN_EXECUTORS: Dict[str, ExecutorSpec] = {
        "null-check": ExecutorSpec(
            id="null-check",
            version="1.0.0",
            runtime=Runtime.WASM,
            artifact_url="registry://executors/null-check@1.0/artifact.wasm",
            description="Count null values in a column",
            input_schema={
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "threshold": {"type": "number"},
                    "operator": {"type": "string", "enum": ["lte", "gte", "eq", "lt", "gt"]},
                },
                "required": ["column"],
            },
            tags=["data-quality", "null-check"],
        ),
        "unique-check": ExecutorSpec(
            id="unique-check",
            version="1.0.0",
            runtime=Runtime.WASM,
            artifact_url="registry://executors/unique-check@1.0/artifact.wasm",
            description="Count duplicate values in a column",
            input_schema={
                "type": "object",
                "properties": {"column": {"type": "string"}, "threshold": {"type": "number"}},
                "required": ["column"],
            },
            tags=["data-quality", "unique-check"],
        ),
        "pattern-check": ExecutorSpec(
            id="pattern-check",
            version="1.0.0",
            runtime=Runtime.WASM,
            artifact_url="registry://executors/pattern-check@1.0/artifact.wasm",
            description="Validate column values against a regex pattern",
            input_schema={
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "pattern": {"type": "string"},
                    "threshold": {"type": "number"},
                },
                "required": ["column", "pattern"],
            },
            tags=["data-quality", "pattern-check"],
        ),
        "range-check": ExecutorSpec(
            id="range-check",
            version="1.0.0",
            runtime=Runtime.WASM,
            artifact_url="registry://executors/range-check@1.0/artifact.wasm",
            description="Check values are within a min/max range",
            input_schema={
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "min": {"type": "number"},
                    "max": {"type": "number"},
                },
                "required": ["column"],
            },
            tags=["data-quality", "range-check"],
        ),
        "row-count": ExecutorSpec(
            id="row-count",
            version="1.0.0",
            runtime=Runtime.WASM,
            artifact_url="registry://executors/row-count@1.0/artifact.wasm",
            description="Check row count is within bounds",
            input_schema={
                "type": "object",
                "properties": {"min": {"type": "integer"}, "max": {"type": "integer"}},
            },
            tags=["data-quality", "row-count"],
        ),
        "freshness-check": ExecutorSpec(
            id="freshness-check",
            version="1.0.0",
            runtime=Runtime.WASM,
            artifact_url="registry://executors/freshness-check@1.0/artifact.wasm",
            description="Check data freshness by timestamp column",
            input_schema={
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "max_age": {"type": "string"},  # ISO 8601 duration
                },
                "required": ["column", "max_age"],
            },
            tags=["data-quality", "freshness-check"],
        ),
        "masking-check": ExecutorSpec(
            id="masking-check",
            version="1.0.0",
            runtime=Runtime.WASM,
            artifact_url="registry://executors/masking-check@1.0/artifact.wasm",
            description="Verify PII masking is applied",
            input_schema={
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "strategy": {"type": "string"},
                    "pii_type": {"type": "string"},
                },
                "required": ["column"],
            },
            tags=["privacy", "masking-check"],
        ),
        "pii-detection": ExecutorSpec(
            id="pii-detection",
            version="1.0.0",
            runtime=Runtime.WASM,
            artifact_url="registry://executors/pii-detection@1.0/artifact.wasm",
            description="Detect undeclared PII in data",
            input_schema={
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "patterns": {"type": "array"},
                    "declared_pii": {"type": "boolean"},
                },
                "required": ["column"],
            },
            tags=["privacy", "pii-detection"],
        ),
        "distribution-drift": ExecutorSpec(
            id="distribution-drift",
            version="1.0.0",
            runtime=Runtime.CONTAINER,  # Needs scipy
            artifact_url="oci://ghcr.io/griot/executors/distribution-drift:1.0",
            description="Detect distribution drift using PSI/KL divergence",
            input_schema={
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "baseline": {"type": "object"},
                    "method": {"type": "string", "enum": ["psi", "kl"]},
                    "threshold": {"type": "number"},
                },
                "required": ["column", "method"],
            },
            tags=["data-quality", "drift"],
        ),
        "referential-check": ExecutorSpec(
            id="referential-check",
            version="1.0.0",
            runtime=Runtime.CONTAINER,  # Needs DB connection
            artifact_url="oci://ghcr.io/griot/executors/referential-check:1.0",
            description="Check referential integrity between tables",
            input_schema={
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "reference": {
                        "type": "object",
                        "properties": {"dataset": {"type": "string"}, "column": {"type": "string"}},
                    },
                },
                "required": ["column", "reference"],
            },
            tags=["data-quality", "referential"],
        ),
    }

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        fetchers: Optional[Dict[URIScheme, ExecutorFetcher]] = None,
        ttl_seconds: int = 3600,
    ):
        """
        Initialize the executor registry.

        Args:
            cache_dir: Directory for caching executor artifacts
            fetchers: Custom fetchers for different URI schemes
            ttl_seconds: Cache TTL in seconds (default 1 hour)
        """
        self._cache_dir = cache_dir or Path.home() / ".griot" / "executors"
        self._fetchers = fetchers or {}
        self._ttl_seconds = ttl_seconds
        self._spec_cache: Dict[str, CachedExecutor] = {}

        # Ensure cache directory exists
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    async def get_executor(self, uri: str) -> ExecutorSpec:
        """
        Get executor specification from URI.

        Args:
            uri: Executor URI

        Returns:
            ExecutorSpec for the executor

        Raises:
            ExecutorNotFoundError: If executor cannot be found
            InvalidExecutorURIError: If URI is invalid
        """
        # Check cache first
        if uri in self._spec_cache:
            return self._spec_cache[uri].spec

        # Parse URI
        parsed = self._parse_uri(uri)
        scheme = parsed["scheme"]
        executor_id = parsed["executor_id"]
        version = parsed["version"]

        # Try to get from built-in executors
        if scheme == URIScheme.REGISTRY and executor_id in self.BUILTIN_EXECUTORS:
            spec = self.BUILTIN_EXECUTORS[executor_id]
            # Update version if specified differently
            if version and version != spec.version:
                spec = ExecutorSpec(
                    id=spec.id,
                    version=version,
                    runtime=spec.runtime,
                    artifact_url=spec.artifact_url.replace(f"@{spec.version}", f"@{version}"),
                    description=spec.description,
                    input_schema=spec.input_schema,
                    output_schema=spec.output_schema,
                    tags=spec.tags,
                )
            self._spec_cache[uri] = CachedExecutor(spec=spec)
            return spec

        # Try custom fetcher
        if scheme in self._fetchers:
            spec = await self._fetchers[scheme].fetch_spec(uri)
            self._spec_cache[uri] = CachedExecutor(spec=spec)
            return spec

        # Handle file:// URIs
        if scheme == URIScheme.FILE:
            spec = self._load_local_executor(parsed["path"])
            self._spec_cache[uri] = CachedExecutor(spec=spec)
            return spec

        raise ExecutorNotFoundError(uri, f"No fetcher available for scheme: {scheme}")

    def _parse_uri(self, uri: str) -> Dict[str, Any]:
        """
        Parse an executor URI.

        Supported formats:
        - registry://executors/null-check@1.0
        - file:///path/to/executor.wasm
        - oci://ghcr.io/griot/checks/drift:1.0
        """
        parsed = urlparse(uri)

        try:
            scheme = URIScheme(parsed.scheme)
        except ValueError:
            raise InvalidExecutorURIError(uri, f"Unknown scheme: {parsed.scheme}")

        result: Dict[str, Any] = {
            "scheme": scheme,
            "uri": uri,
            "executor_id": None,
            "version": None,
            "path": None,
        }

        if scheme == URIScheme.REGISTRY:
            # registry://executors/null-check@1.0
            path = parsed.path.lstrip("/")
            if "@" in path:
                name_part, version = path.rsplit("@", 1)
                result["executor_id"] = name_part.split("/")[-1]
                result["version"] = version
            else:
                result["executor_id"] = path.split("/")[-1]
                result["version"] = "1.0.0"

        elif scheme == URIScheme.FILE:
            # file:///path/to/executor.wasm
            result["path"] = parsed.path
            # Extract executor ID from filename
            filename = Path(parsed.path).stem
            result["executor_id"] = filename

        elif scheme == URIScheme.OCI:
            # oci://ghcr.io/griot/checks/drift:1.0
            path = parsed.netloc + parsed.path
            if ":" in path:
                name_part, version = path.rsplit(":", 1)
                result["executor_id"] = name_part.split("/")[-1]
                result["version"] = version
            else:
                result["executor_id"] = path.split("/")[-1]
                result["version"] = "latest"
            result["path"] = path

        return result

    def _load_local_executor(self, path: str) -> ExecutorSpec:
        """Load executor spec from local file."""
        file_path = Path(path)

        if not file_path.exists():
            raise ExecutorNotFoundError(f"file://{path}", "File does not exist")

        # Check for manifest file
        manifest_path = file_path.with_suffix(".json")
        if manifest_path.exists():
            with manifest_path.open() as f:
                manifest = json.load(f)
                return ExecutorSpec(
                    id=manifest.get("id", file_path.stem),
                    version=manifest.get("version", "1.0.0"),
                    runtime=Runtime(manifest.get("runtime", "wasm")),
                    artifact_url=f"file://{path}",
                    description=manifest.get("description", ""),
                    input_schema=manifest.get("input_schema"),
                    output_schema=manifest.get("output_schema"),
                    tags=manifest.get("tags", []),
                )

        # Infer from file extension
        suffix = file_path.suffix.lower()
        runtime = Runtime.WASM if suffix == ".wasm" else Runtime.CONTAINER

        return ExecutorSpec(
            id=file_path.stem,
            version="1.0.0",
            runtime=runtime,
            artifact_url=f"file://{path}",
            description=f"Local executor from {file_path.name}",
        )

    def register_executor(self, spec: ExecutorSpec) -> None:
        """
        Register an executor specification.

        Args:
            spec: The executor specification to register
        """
        uri = spec.uri
        self._spec_cache[uri] = CachedExecutor(spec=spec)

    def list_executors(self) -> List[ExecutorSpec]:
        """List all available executors."""
        executors = list(self.BUILTIN_EXECUTORS.values())
        for cached in self._spec_cache.values():
            if cached.spec.id not in self.BUILTIN_EXECUTORS:
                executors.append(cached.spec)
        return executors

    def clear_cache(self) -> None:
        """Clear the executor cache."""
        self._spec_cache.clear()

    def get_cache_path(self, spec: ExecutorSpec) -> Path:
        """Get the cache path for an executor artifact."""
        hash_input = f"{spec.id}:{spec.version}:{spec.artifact_url}"
        hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
        return self._cache_dir / f"{spec.id}-{spec.version}-{hash_digest}"
