"""Lock-file management for pulled contracts.

The lock file (``griot.lock``) records the exact version and content
checksum of every contract that was pulled from a registry, enabling
``griot verify-lock`` to detect drift between the local YAML files
and the pinned state.

File format (YAML)::

    contracts:
      customer-churn:
        id: customer-churn
        version: "1.2.0"
        registry: https://griot.company.com
        checksum: sha256:abc123...
        path: contracts/customer-churn.yaml
        pulled_at: "2026-02-07T10:30:00Z"
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

# -- Data classes -------------------------------------------------------------


@dataclass
class LockEntry:
    """A single entry in the lock file."""

    id: str
    version: str
    registry: str
    checksum: str
    path: str
    pulled_at: str


@dataclass
class LockData:
    """Full lock file contents."""

    contracts: dict[str, LockEntry] = field(default_factory=dict)


# -- Helpers ------------------------------------------------------------------


def compute_checksum(content: str) -> str:
    """Return ``sha256:<hex>`` checksum for *content*.

    Line endings are normalised to ``\\n`` before hashing so that the
    same logical content produces the same checksum on every platform.
    """
    normalised = content.replace("\r\n", "\n").replace("\r", "\n")
    digest = hashlib.sha256(normalised.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


# -- Public API ---------------------------------------------------------------


def write_lock(
    contract_id: str,
    version: str,
    registry_url: str,
    contract_content: str,
    output_path: Path,
    lock_path: Path | None = None,
) -> Path:
    """Add or update a contract entry in the lock file.

    Args:
        contract_id: Unique contract identifier.
        version: Semantic version string.
        registry_url: Registry the contract was pulled from.
        contract_content: Raw YAML content (used for checksum).
        output_path: Path where the contract YAML was written on disk.
        lock_path: Path to the lock file (default: ``griot.lock`` in cwd).

    Returns:
        The resolved lock-file path.
    """
    lock_path = lock_path or Path("griot.lock")

    # Read existing lock data if file exists
    lock_data = read_lock(lock_path) if lock_path.exists() else LockData()

    entry = LockEntry(
        id=contract_id,
        version=version,
        registry=registry_url,
        checksum=compute_checksum(contract_content),
        path=str(output_path),
        pulled_at=datetime.now(timezone.utc).isoformat(),
    )
    lock_data.contracts[contract_id] = entry

    # Serialise and write
    raw: dict[str, Any] = {"contracts": {cid: asdict(e) for cid, e in lock_data.contracts.items()}}
    lock_path.write_text(
        yaml.safe_dump(raw, default_flow_style=False, sort_keys=False), encoding="utf-8"
    )
    return lock_path


def read_lock(lock_path: Path | None = None) -> LockData:
    """Read and parse a lock file.

    Args:
        lock_path: Path to the lock file (default: ``griot.lock`` in cwd).

    Returns:
        Parsed ``LockData``.

    Raises:
        FileNotFoundError: If the lock file does not exist.
    """
    lock_path = lock_path or Path("griot.lock")
    if not lock_path.exists():
        raise FileNotFoundError(f"Lock file not found: {lock_path}")

    raw = yaml.safe_load(lock_path.read_text(encoding="utf-8")) or {}
    contracts_raw = raw.get("contracts", {})

    entries: dict[str, LockEntry] = {}
    for cid, data in contracts_raw.items():
        entries[cid] = LockEntry(
            id=data.get("id", cid),
            version=data.get("version", ""),
            registry=data.get("registry", ""),
            checksum=data.get("checksum", ""),
            path=data.get("path", ""),
            pulled_at=data.get("pulled_at", ""),
        )
    return LockData(contracts=entries)


def verify_lock(
    lock_path: Path | None = None,
    contracts_dir: Path | None = None,
) -> dict[str, bool]:
    """Compare local contract files against lock-file checksums.

    Uses the ``path`` stored in each lock entry to locate the file.
    Falls back to ``contracts_dir/{id}.yaml`` if no path is recorded.

    Args:
        lock_path: Path to the lock file.
        contracts_dir: Fallback directory for contracts without a stored path.

    Returns:
        Mapping of ``contract_id -> True`` (checksum matches) or ``False``
        (drift detected or file missing).
    """
    lock_path = lock_path or Path("griot.lock")
    contracts_dir = contracts_dir or Path("contracts")

    lock_data = read_lock(lock_path)
    results: dict[str, bool] = {}

    for cid, entry in lock_data.contracts.items():
        # Use the stored path first, then fall back to name-based lookup
        candidates: list[Path] = []
        if entry.path:
            candidates.append(Path(entry.path))
        candidates.extend(
            [
                contracts_dir / f"{cid}.yaml",
                contracts_dir / f"{cid}.yml",
            ]
        )

        found = False
        for candidate in candidates:
            if candidate.exists():
                content = candidate.read_text(encoding="utf-8")
                results[cid] = compute_checksum(content) == entry.checksum
                found = True
                break

        if not found:
            results[cid] = False

    return results
