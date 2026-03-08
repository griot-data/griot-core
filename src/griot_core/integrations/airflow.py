"""
Apache Airflow integration for Griot validation and reporting.

Provides Airflow operators and sensors for running Griot validation
and reporting external tool results as part of Airflow DAGs.

Operators:
    - GriotValidateOperator: Run Griot's own validation engine
    - GriotReportOperator: Parse external tool results and report to registry

Requirements:
    pip install apache-airflow

Example DAG:
    from airflow import DAG
    from airflow.utils.dates import days_ago
    from griot_core.integrations.airflow import (
        GriotValidateOperator,
        GriotReportOperator,
    )

    with DAG(
        dag_id="data_validation",
        start_date=days_ago(1),
        schedule_interval="@daily",
    ) as dag:

        validate_sales = GriotValidateOperator(
            task_id="validate_sales_data",
            contract_id="sales-contract",
            profile="data_engineering",
            fail_on_invalid=True,
        )

        report_dbt = GriotReportOperator(
            task_id="report_dbt_results",
            tool="dbt",
            results_path="target/run_results.json",
            contract_id="sales-contract",
            registry_url="http://localhost:8000/api/v1",
            token=os.environ.get("GRIOT_SA_TOKEN"),
        )
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Sequence

try:
    from airflow.models import BaseOperator
    from airflow.sensors.base import BaseSensorOperator
    from airflow.utils.context import Context

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

    # Create stub classes for type hints when Airflow not installed
    class BaseOperator:
        pass

    class BaseSensorOperator:
        pass

    class Context:
        pass


class GriotValidateOperator(BaseOperator):
    """
    Airflow operator for running Griot validation.

    Validates data against a contract using the specified profile.
    Can either fail the task on validation failure or continue
    with the result stored in XCom.

    When registry_url and report_to_registry are set, validation results
    are automatically reported to the Griot Registry as Runs and Validations.

    Args:
        contract_id: ID of the contract to validate
        profile: Execution profile (default, data_engineering, etc.)
        environment: Environment (production, staging, dev)
        registry_url: URL of the Griot registry
        token: Service account token (gsa_) for registry auth
        api_key: API key for registry auth (alternative to token)
        report_to_registry: Auto-report results when registry_url is set
        fail_on_invalid: Whether to fail the task if validation fails
        fail_on_critical: Whether to fail only on critical check failures
        arrow_data_path: Path to Arrow data file (optional)
        options: Additional validation options
        timeout: Timeout in seconds

    Example:
        >>> validate_task = GriotValidateOperator(
        ...     task_id="validate_users",
        ...     contract_id="users-contract",
        ...     profile="data_engineering",
        ...     registry_url="http://localhost:8000/api/v1",
        ...     token=os.environ.get("GRIOT_SA_TOKEN"),
        ...     fail_on_invalid=True,
        ... )
    """

    template_fields: Sequence[str] = (
        "contract_id",
        "profile",
        "environment",
        "arrow_data_path",
        "registry_url",
    )

    ui_color = "#6366f1"  # Indigo
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        contract_id: str,
        profile: str = "default",
        environment: str = "production",
        registry_url: Optional[str] = None,
        token: Optional[str] = None,
        api_key: Optional[str] = None,
        report_to_registry: bool = True,
        fail_on_invalid: bool = True,
        fail_on_critical: bool = False,
        arrow_data_path: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        timeout: int = 300,
        **kwargs: Any,
    ) -> None:
        if not AIRFLOW_AVAILABLE:
            raise ImportError(
                "Apache Airflow is required for this operator. "
                "Install with: pip install apache-airflow"
            )

        super().__init__(**kwargs)
        self.contract_id = contract_id
        self.profile = profile
        self.environment = environment
        self.registry_url = registry_url
        self.token = token
        self.api_key = api_key
        self.report_to_registry = report_to_registry
        self.fail_on_invalid = fail_on_invalid
        self.fail_on_critical = fail_on_critical
        self.arrow_data_path = arrow_data_path
        self.options = options or {}
        self.timeout = timeout

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute the validation.

        Args:
            context: Airflow execution context

        Returns:
            Validation result dictionary

        Raises:
            AirflowException: If validation fails and fail_on_invalid is True
        """
        import asyncio

        from airflow.exceptions import AirflowException

        from griot_core.workers import JobPayload, LocalWorker, WorkerStatus

        self.log.info(f"Validating contract: {self.contract_id}")
        self.log.info(f"Profile: {self.profile}, Environment: {self.environment}")

        # Load Arrow data if path provided
        arrow_data = None
        if self.arrow_data_path:
            self.log.info(f"Loading Arrow data from: {self.arrow_data_path}")
            with open(self.arrow_data_path, "rb") as f:
                arrow_data = {"default": f.read()}

        # Create worker and payload
        worker = LocalWorker()

        # If registry URL provided, configure it
        if self.registry_url:
            worker.config.registry_url = self.registry_url

        payload = JobPayload(
            job_id=f"airflow-{context['dag'].dag_id}-{context['run_id']}",
            contract_id=self.contract_id,
            profile=self.profile,
            environment=self.environment,
            arrow_data=arrow_data,
            options=self.options,
            timeout_seconds=self.timeout,
            metadata={
                "dag_id": context["dag"].dag_id,
                "task_id": context["task"].task_id,
                "run_id": context["run_id"],
                "execution_date": str(context["execution_date"]),
            },
        )

        # Run validation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(worker.execute(payload))
        finally:
            loop.close()

        # Log results
        self.log.info(f"Validation completed: {result.status.value}")
        self.log.info(f"Is valid: {result.is_valid}")

        if result.validation_result:
            schema_results = result.validation_result.get("schema_results", [])
            for sr in schema_results:
                self.log.info(f"Schema {sr['schema_id']}: valid={sr['is_valid']}")
                for cr in sr.get("check_results", []):
                    status = cr["status"]
                    severity = cr.get("severity", "info")
                    self.log.info(f"  - {cr['check_name']}: {status} ({severity})")

        # Push result to XCom
        result_dict = result.to_dict()

        # Report to registry if configured
        if self.registry_url and self.report_to_registry:
            self._report_to_registry(result, context)

        # Handle failure
        if result.status != WorkerStatus.COMPLETED:
            if self.fail_on_invalid:
                raise AirflowException(f"Validation failed: {result.errors}")

        if not result.is_valid:
            if self.fail_on_invalid:
                raise AirflowException(
                    f"Validation failed for contract {self.contract_id}. Profile: {self.profile}"
                )
            elif self.fail_on_critical:
                # Check for critical failures
                has_critical = False
                if result.validation_result:
                    for sr in result.validation_result.get("schema_results", []):
                        for cr in sr.get("check_results", []):
                            if cr.get("status") == "failed" and cr.get("severity") == "critical":
                                has_critical = True
                                break

                if has_critical:
                    raise AirflowException(
                        f"Critical validation failure for contract {self.contract_id}"
                    )

        return result_dict

    def _report_to_registry(self, result, context: Context) -> None:
        """Report validation results to the Griot Registry.

        Converts WorkerResult into a ToolValidationReport and pushes
        it as a Run with per-schema Validations.
        """
        from griot_core.reporting.base import SchemaTestResult, ToolValidationReport
        from griot_core.reporting.reporter import RegistryReporter

        self.log.info("Reporting validation results to Griot Registry...")

        # Convert WorkerResult.validation_result to ToolValidationReport
        schemas: Dict[str, SchemaTestResult] = {}
        overall_total = 0
        overall_passed = 0
        overall_failed = 0

        if result.validation_result:
            for sr in result.validation_result.get("schema_results", []):
                schema_name = sr.get("schema_id", "unknown")
                checks = sr.get("check_results", [])
                total = len(checks)
                passed_count = sum(1 for c in checks if c.get("status") == "passed")
                failed_count = sum(1 for c in checks if c.get("status") == "failed")
                errors = [
                    {
                        "field": c.get("check_name", ""),
                        "constraint": c.get("check_name", ""),
                        "message": c.get("message", ""),
                    }
                    for c in checks
                    if c.get("status") == "failed"
                ]

                schemas[schema_name] = SchemaTestResult(
                    schema_name=schema_name,
                    total=total,
                    passed=passed_count,
                    failed=failed_count,
                    errors=errors,
                )

                overall_total += total
                overall_passed += passed_count
                overall_failed += failed_count

        report = ToolValidationReport(
            tool="griot",
            schemas=schemas,
            overall=SchemaTestResult(
                schema_name="_overall",
                total=overall_total,
                passed=overall_passed,
                failed=overall_failed,
            ),
            all_passed=result.is_valid,
            metadata={
                "dag_id": context["dag"].dag_id,
                "task_id": context["task"].task_id,
                "run_id": context["run_id"],
            },
        )

        try:
            with RegistryReporter(
                registry_url=self.registry_url,
                token=self.token,
                api_key=self.api_key,
            ) as reporter:
                report_result = reporter.report(
                    report=report,
                    contract_id=self.contract_id,
                    environment=self.environment,
                    pipeline_id=f"airflow-{context['dag'].dag_id}",
                    trigger="scheduled",
                )
                self.log.info(
                    f"Reported to registry: run_id={report_result.run_id}, "
                    f"status={report_result.status}, "
                    f"schemas={report_result.schemas_reported}"
                )
        except Exception as e:
            self.log.warning(f"Failed to report to registry: {e}")


class GriotReportOperator(BaseOperator):
    """
    Airflow operator that parses external tool test results and reports
    them to the Griot Registry as Runs and Validations.

    Supports any tool with a registered parser (dbt, soda, etc.).

    Authentication (in priority order):
        1. client_id + client_secret: Operator exchanges for JWT internally
        2. token: Pre-exchanged JWT
        3. api_key: API key for X-API-Key auth

    Example (using Airflow Connection - recommended):
        >>> report = GriotReportOperator(
        ...     task_id="report_to_griot",
        ...     tool="dbt",
        ...     results_path="target/run_results.json",
        ...     contract_id="sales-forecasting-data-contract",
        ...     griot_conn_id="griot_default",  # Everything loaded from connection!
        ... )

    Example (explicit credentials):
        >>> report = GriotReportOperator(
        ...     task_id="report_to_griot",
        ...     tool="dbt",
        ...     results_path="target/run_results.json",
        ...     contract_id="sales-forecasting-data-contract",
        ...     registry_url="http://localhost:8000/api/v1",
        ...     client_id=os.environ.get("GRIOT_SA_CLIENT_ID"),
        ...     client_secret=os.environ.get("GRIOT_SA_SECRET"),
        ... )
    """

    template_fields: Sequence[str] = (
        "results_path",
        "contract_id",
        "environment",
        "registry_url",
        "griot_conn_id",
        "token",
        "client_id",
        "client_secret",
    )

    ui_color = "#10b981"  # Green (distinct from validate operator's indigo)
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        # What to report
        contract_id: str,
        tool: str = "dbt",
        results_path: str = "target/run_results.json",
        contract_version: Optional[str] = None,
        schema_mapping: Optional[Dict[str, str]] = None,
        # Where to report
        registry_url: Optional[str] = None,
        griot_conn_id: Optional[str] = None,  # NEW: Airflow Connection ID
        # Auth (priority: griot_conn_id > client_id+secret > token > api_key)
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token: Optional[str] = None,
        api_key: Optional[str] = None,
        # Context
        environment: str = "development",
        pipeline_id: Optional[str] = None,
        trigger: str = "scheduled",
        # Behavior
        fail_on_invalid: bool = False,
        parser: Optional[Any] = None,  # ToolResultsParser instance
        **kwargs: Any,
    ) -> None:
        if not AIRFLOW_AVAILABLE:
            raise ImportError(
                "Apache Airflow is required for this operator. "
                "Install with: pip install apache-airflow"
            )

        super().__init__(**kwargs)
        self.contract_id = contract_id
        self.tool = tool
        self.results_path = results_path
        self.contract_version = contract_version
        self.schema_mapping = schema_mapping
        self.registry_url = registry_url
        self.griot_conn_id = griot_conn_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = token
        self.api_key = api_key
        self.environment = environment
        self.pipeline_id = pipeline_id
        self.trigger = trigger
        self.fail_on_invalid = fail_on_invalid
        self._parser = parser

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Parse tool results and report to the Griot Registry.

        Steps:
            1. Load credentials from Airflow Connection (if griot_conn_id provided)
            2. Resolve parser (from instance, registry, or auto-detect)
            3. Parse results file into ToolValidationReport
            4. Report to registry via RegistryReporter (single call)
            5. Push summary to XCom

        Args:
            context: Airflow execution context

        Returns:
            Dict with run_id, all_passed, and summary

        Raises:
            AirflowException: If fail_on_invalid and tests failed
        """
        from airflow.exceptions import AirflowException

        from griot_core.reporting.registry import get_default_parser_registry
        from griot_core.reporting.reporter import RegistryReporter

        # Step 1: Load credentials from Airflow Connection (if provided)
        registry_url = self.registry_url
        client_id = self.client_id
        client_secret = self.client_secret
        token = self.token
        api_key = self.api_key

        if self.griot_conn_id:
            self.log.info(f"Loading credentials from Airflow Connection: {self.griot_conn_id}")
            try:
                from airflow.hooks.base import BaseHook

                conn = BaseHook.get_connection(self.griot_conn_id)

                # Registry URL: Use connection host or fall back to parameter
                if conn.host:
                    # Handle both formats: "localhost:8000/api/v1" or "http://localhost:8000/api/v1"
                    conn_host = conn.host
                    if not conn_host.startswith(("http://", "https://")):
                        conn_type = conn.conn_type or "http"
                        registry_url = f"{conn_type}://{conn_host}"
                    else:
                        registry_url = conn_host
                    self.log.info(f"Using registry URL from connection: {registry_url}")

                # Credentials: Use connection login/password or fall back to parameters
                client_id = client_id or conn.login
                client_secret = client_secret or conn.password

                # Token/API key from connection extra
                if conn.extra_dejson:
                    token = token or conn.extra_dejson.get("token")
                    api_key = api_key or conn.extra_dejson.get("api_key")

                self.log.info(f"Loaded credentials from connection (client_id: {client_id[:8]}...)")

            except Exception as e:
                self.log.warning(f"Failed to load Airflow Connection '{self.griot_conn_id}': {e}")
                self.log.warning("Falling back to explicit parameters")

        # Validate we have what we need
        if not registry_url:
            raise AirflowException(
                "registry_url is required. Provide it via 'registry_url' parameter or "
                "'griot_conn_id' Airflow Connection."
            )

        results_path = Path(self.results_path)
        self.log.info(f"Reporting {self.tool} results from {results_path}")
        self.log.info(f"Contract: {self.contract_id}, Registry: {registry_url}")

        # Step 2: Resolve parser
        parser = self._parser
        if parser is None:
            registry = get_default_parser_registry()

            # Build kwargs for parser construction
            parser_kwargs: Dict[str, Any] = {}
            if self.schema_mapping is not None:
                self.log.info(f"Using explicit schema_mapping: {self.schema_mapping}")
                parser_kwargs["schema_mapping"] = self.schema_mapping
            elif registry_url and (token or api_key or client_id):
                # Try to fetch contract from registry to derive mapping
                self.log.info(
                    "No schema_mapping provided, attempting auto-discovery from contract..."
                )
                mapping = self._fetch_schema_mapping(
                    registry_url=registry_url,
                    client_id=client_id,
                    client_secret=client_secret,
                    token=token,
                    api_key=api_key,
                )
                if mapping:
                    self.log.info(f"Auto-discovered schema_mapping: {mapping}")
                    parser_kwargs["schema_mapping"] = mapping
                else:
                    self.log.warning(
                        "Auto-discovery failed. Results may not map correctly to contract schemas. "
                        "Consider providing explicit schema_mapping parameter."
                    )

            parser = registry.create_parser(self.tool, **parser_kwargs)

        self.log.info(f"Using parser: {parser.tool_name}")

        # Step 3: Parse results
        report = parser.parse(results_path)

        self.log.info(
            f"Parsed {report.overall.total} tests: "
            f"{report.overall.passed} passed, "
            f"{report.overall.failed} failed, "
            f"{report.overall.warned} warned, "
            f"{report.overall.skipped} skipped"
        )

        # Step 4: Report to registry (single call, auth handled internally)
        pipeline_id = self.pipeline_id or f"airflow-{context['dag'].dag_id}"

        with RegistryReporter(
            registry_url=registry_url,
            client_id=client_id,
            client_secret=client_secret,
            token=token,
            api_key=api_key,
        ) as reporter:
            report_result = reporter.report(
                report=report,
                contract_id=self.contract_id,
                contract_version=self.contract_version,
                environment=self.environment,
                pipeline_id=pipeline_id,
                trigger=self.trigger,
            )

        self.log.info(
            f"Reported to registry: run_id={report_result.run_id}, "
            f"status={report_result.status}, "
            f"schemas={report_result.schemas_reported}"
        )

        # Step 5: Build result for XCom
        xcom_result = {
            "run_id": report_result.run_id,
            "all_passed": report.all_passed,
            "status": report_result.status,
            "schemas_reported": report_result.schemas_reported,
            "validations": report_result.validations,
            "summary": {
                "total": report.overall.total,
                "passed": report.overall.passed,
                "failed": report.overall.failed,
                "warned": report.overall.warned,
                "errored": report.overall.errored,
                "skipped": report.overall.skipped,
            },
        }

        # Step 6: Handle failure
        if self.fail_on_invalid and not report.all_passed:
            raise AirflowException(
                f"Tool validation failed for contract {self.contract_id}. "
                f"{report.overall.failed} tests failed, "
                f"{report.overall.errored} errored. "
                f"Run ID: {report_result.run_id}"
            )

        return xcom_result

    def _fetch_schema_mapping(
        self,
        registry_url: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> Optional[Dict[str, str]]:
        """Try to fetch contract from registry and derive schema mapping."""
        try:
            from griot_core.reporting.reporter import RegistryReporter

            reporter = RegistryReporter(
                registry_url=registry_url,
                client_id=client_id,
                client_secret=client_secret,
                token=token,
                api_key=api_key,
            )
            client = reporter._get_client()
            contract = client.get(self.contract_id, self.contract_version)
            reporter.close()
            mapping: Dict[str, str] = {}
            for schema in getattr(contract, "inline_schemas", []):
                physical = getattr(schema, "physical_name", "")
                name = getattr(schema, "name", "")
                if physical and name:
                    table_name = physical.split(".")[-1]
                    mapping[table_name] = name
            if mapping:
                self.log.info(f"Auto-discovered schema mapping: {mapping}")
                return mapping
        except Exception as e:
            self.log.warning(
                f"Could not auto-discover schema mapping from registry: {e}. "
                f"Provide schema_mapping parameter for best results."
            )
        return None


class GriotValidateSensor(BaseSensorOperator):
    """
    Airflow sensor that waits for validation to pass.

    Useful for waiting until data meets quality requirements
    before proceeding with downstream tasks.

    Args:
        contract_id: ID of the contract to validate
        profile: Execution profile
        registry_url: URL of the Griot registry
        arrow_data_path: Path to Arrow data file
        poke_interval: Seconds between pokes

    Example:
        >>> wait_for_valid = GriotValidateSensor(
        ...     task_id="wait_for_quality",
        ...     contract_id="users-contract",
        ...     poke_interval=60,
        ...     timeout=3600,
        ... )
    """

    template_fields: Sequence[str] = (
        "contract_id",
        "profile",
        "arrow_data_path",
    )

    ui_color = "#6366f1"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        contract_id: str,
        profile: str = "default",
        environment: str = "production",
        registry_url: Optional[str] = None,
        arrow_data_path: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        if not AIRFLOW_AVAILABLE:
            raise ImportError(
                "Apache Airflow is required for this sensor. "
                "Install with: pip install apache-airflow"
            )

        super().__init__(**kwargs)
        self.contract_id = contract_id
        self.profile = profile
        self.environment = environment
        self.registry_url = registry_url
        self.arrow_data_path = arrow_data_path
        self.options = options or {}

    def poke(self, context: Context) -> bool:
        """
        Check if validation passes.

        Args:
            context: Airflow execution context

        Returns:
            True if validation passes, False otherwise
        """
        import asyncio

        from griot_core.workers import JobPayload, LocalWorker, WorkerStatus

        self.log.info(f"Checking validation for contract: {self.contract_id}")

        # Load Arrow data if path provided
        arrow_data = None
        if self.arrow_data_path:
            with open(self.arrow_data_path, "rb") as f:
                arrow_data = {"default": f.read()}

        # Create worker and payload
        worker = LocalWorker()

        if self.registry_url:
            worker.config.registry_url = self.registry_url

        payload = JobPayload(
            job_id=f"airflow-sensor-{context['run_id']}",
            contract_id=self.contract_id,
            profile=self.profile,
            environment=self.environment,
            arrow_data=arrow_data,
            options=self.options,
        )

        # Run validation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(worker.execute(payload))
        finally:
            loop.close()

        self.log.info(f"Validation result: valid={result.is_valid}")

        return result.status == WorkerStatus.COMPLETED and result.is_valid
