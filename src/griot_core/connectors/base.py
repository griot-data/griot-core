"""
Base connector protocol and types.

Defines the DataConnector protocol that all connectors must implement.
Connectors return data in Arrow IPC format, never as DataFrames.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from griot_core.models import Schema


class ConnectorType(str, Enum):
    """Types of data source connectors."""

    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    REDSHIFT = "redshift"
    DATABRICKS = "databricks"
    S3_PARQUET = "s3_parquet"
    GCS_PARQUET = "gcs_parquet"
    AZURE_PARQUET = "azure_parquet"
    LOCAL_PARQUET = "local_parquet"
    LOCAL_CSV = "local_csv"


@dataclass
class ConnectorConfig:
    """
    Configuration for a data connector.

    This is the base configuration that all connectors share.
    Specific connectors may have additional fields in their
    connection parameters.

    Attributes:
        connector_type: Type of connector (snowflake, bigquery, etc.)
        connection_params: Connection-specific parameters
        timeout_seconds: Connection timeout in seconds
        retry_count: Number of retries for failed connections
        ssl_enabled: Whether to use SSL
        extra_options: Additional connector-specific options
    """

    connector_type: ConnectorType
    connection_params: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 30
    retry_count: int = 3
    ssl_enabled: bool = True
    extra_options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConnectionTestResult:
    """
    Result of testing a connector's connection.

    Attributes:
        success: Whether the connection test succeeded
        message: Human-readable message about the result
        latency_ms: Connection latency in milliseconds
        server_version: Database server version if available
        errors: List of error messages if failed
    """

    success: bool
    message: str = ""
    latency_ms: Optional[float] = None
    server_version: Optional[str] = None
    errors: List[str] = field(default_factory=list)


@dataclass
class FetchResult:
    """
    Result of fetching data from a connector.

    Attributes:
        data: Arrow IPC format bytes
        row_count: Number of rows fetched
        byte_size: Size of the data in bytes
        fetch_time_ms: Time taken to fetch in milliseconds
        truncated: Whether the data was truncated (sample_size applied)
        warnings: Any warnings during fetch
    """

    data: bytes  # Arrow IPC format
    row_count: int
    byte_size: int
    fetch_time_ms: float
    truncated: bool = False
    warnings: List[str] = field(default_factory=list)


@runtime_checkable
class DataConnector(Protocol):
    """
    Protocol for data source connectors.

    All connectors MUST return data in Arrow IPC format.
    They should NEVER return DataFrames directly.

    This keeps griot-core framework-agnostic. The actual
    DataFrame processing happens inside executors (WASM or Container).

    Example implementation:

        class SnowflakeConnector:
            def __init__(self, config: ConnectorConfig):
                self.config = config

            async def fetch_as_arrow(
                self,
                schema: Schema,
                sample_size: Optional[int] = None
            ) -> bytes:
                # Connect to Snowflake
                async with self._get_connection() as conn:
                    query = f"SELECT * FROM {schema.physical_name}"
                    if sample_size:
                        query += f" SAMPLE ({sample_size} ROWS)"

                    cursor = conn.cursor()
                    cursor.execute(query)

                    # Snowflake natively supports Arrow
                    arrow_table = cursor.fetch_arrow_all()

                    # Serialize to IPC format
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
                        writer.write_table(arrow_table)
                    return sink.getvalue().to_pybytes()

            async def test_connection(self) -> ConnectionTestResult:
                try:
                    async with self._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        return ConnectionTestResult(success=True)
                except Exception as e:
                    return ConnectionTestResult(
                        success=False,
                        errors=[str(e)]
                    )
    """

    @abstractmethod
    async def fetch_as_arrow(
        self,
        schema: Schema,
        sample_size: Optional[int] = None,
    ) -> bytes:
        """
        Fetch data as Arrow IPC bytes.

        This method MUST return Arrow IPC format bytes.
        It should NOT return a DataFrame.

        Args:
            schema: The schema defining what to fetch
            sample_size: Optional limit on rows (for sampling)

        Returns:
            Arrow IPC format bytes

        Raises:
            ConnectionError: If unable to connect to data source
            QueryError: If the query fails
        """
        ...

    @abstractmethod
    async def test_connection(self) -> ConnectionTestResult:
        """
        Test that connection credentials are valid.

        Returns:
            ConnectionTestResult with success status and any errors
        """
        ...

    @abstractmethod
    async def fetch_with_query(
        self,
        query: str,
        sample_size: Optional[int] = None,
    ) -> bytes:
        """
        Fetch data using a custom query.

        Args:
            query: SQL query to execute
            sample_size: Optional limit on rows

        Returns:
            Arrow IPC format bytes
        """
        ...

    @abstractmethod
    async def get_table_schema(
        self,
        physical_name: str,
    ) -> Dict[str, Any]:
        """
        Get the schema of a table from the data source.

        Useful for schema inference and validation.

        Args:
            physical_name: Physical name of the table

        Returns:
            Dictionary with column names, types, and metadata
        """
        ...


class BaseConnector:
    """
    Base class for connector implementations.

    Provides common functionality that all connectors can inherit.
    """

    def __init__(self, config: ConnectorConfig):
        """
        Initialize the connector.

        Args:
            config: Connector configuration
        """
        self.config = config

    def _build_select_query(
        self,
        schema: Schema,
        sample_size: Optional[int] = None,
    ) -> str:
        """
        Build a SELECT query for the schema.

        Args:
            schema: The schema to query
            sample_size: Optional row limit

        Returns:
            SQL query string
        """
        columns = ", ".join(prop.name for prop in schema.properties)
        query = f"SELECT {columns} FROM {schema.physical_name}"

        if sample_size:
            # Default limit clause (override in specific connectors)
            query += f" LIMIT {sample_size}"

        return query
