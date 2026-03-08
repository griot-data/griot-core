"""
Kubernetes dispatcher for orchestrated validation jobs.

Dispatches WASM worker jobs and container checks as native K8s Jobs,
avoiding Docker-in-Docker by spawning containers directly on K8s.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from griot_core.orchestration.dispatcher.base import (
    ComputeBackend,
    ComputeDispatcher,
    DispatcherConfig,
)
from griot_core.orchestration.types import (
    ContainerJobSpec,
    DispatchResult,
    WasmJobSpec,
)

logger = logging.getLogger(__name__)


class KubernetesDispatcher(ComputeDispatcher):
    """
    Kubernetes dispatcher for orchestrated validation jobs.

    Creates native K8s Jobs for both WASM workers and container checks.
    This avoids Docker-in-Docker by having the orchestrator spawn
    containers directly on Kubernetes.

    WASM checks:
        All WASM checks are bundled into a single griot-core worker pod
        that runs them sequentially using the embedded WASM runtime.

    Container checks:
        Each container check runs as a separate K8s Job with the
        check's container image as the pod spec.

    Example:
        config = DispatcherConfig(
            backend=ComputeBackend.KUBERNETES,
            wasm_worker_image="griot/wasm-worker:v1.0",
        )
        dispatcher = KubernetesDispatcher(
            config,
            namespace="griot",
        )

        # Dispatch WASM checks
        result = await dispatcher.dispatch_wasm_worker(wasm_spec)

        # Dispatch container check
        result = await dispatcher.dispatch_container(container_spec)
    """

    def __init__(
        self,
        config: DispatcherConfig,
        namespace: str = "default",
        service_account: str | None = None,
    ):
        """
        Initialize Kubernetes dispatcher.

        Args:
            config: Dispatcher configuration
            namespace: Kubernetes namespace to create jobs in
            service_account: Optional service account for jobs
        """
        super().__init__(config)
        self.namespace = namespace
        self.service_account = service_account
        self._api = None
        self._batch_api = None

    @property
    def backend(self) -> ComputeBackend:
        """Return Kubernetes backend type."""
        return ComputeBackend.KUBERNETES

    def _get_api(self):
        """Get or create Kubernetes API clients."""
        if self._api is None:
            try:
                from kubernetes import client
                from kubernetes import config as k8s_config

                # Try in-cluster config first, fall back to kubeconfig
                try:
                    k8s_config.load_incluster_config()
                except k8s_config.ConfigException:
                    k8s_config.load_kube_config()

                self._api = client.CoreV1Api()
                self._batch_api = client.BatchV1Api()
            except ImportError:
                raise RuntimeError(
                    "kubernetes client is required for K8s dispatcher. "
                    "Install with: pip install kubernetes"
                )
        return self._api, self._batch_api

    def _create_wasm_worker_manifest(self, spec: WasmJobSpec) -> dict[str, Any]:
        """
        Create K8s Job manifest for WASM worker.

        Args:
            spec: WASM job specification

        Returns:
            Kubernetes Job manifest as dict
        """
        job_name = f"griot-wasm-{spec.job_id[:8]}-{uuid.uuid4().hex[:6]}"

        # Serialize spec to JSON for worker
        env_vars = [
            {"name": "GRIOT_JOB_SPEC", "value": json.dumps(spec.to_dict())},
            {"name": "GRIOT_JOB_ID", "value": spec.job_id},
            {"name": "GRIOT_JOB_TYPE", "value": "wasm_worker"},
        ]

        if spec.callback_url:
            env_vars.append({"name": "GRIOT_CALLBACK_URL", "value": spec.callback_url})

        # Add custom environment from config
        for key, value in self.config.environment.items():
            env_vars.append({"name": key, "value": value})

        # Resource limits
        memory_limit = f"{self.config.memory_mb}Mi"
        cpu_limit = f"{self.config.cpu_millicores}m"

        # Build labels
        labels = {
            "app": "griot-validation",
            "griot.io/job-type": "wasm-worker",
            "griot.io/job-id": spec.job_id,
            "griot.io/contract-id": spec.contract_id,
            **self.config.labels,
        }

        manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.namespace,
                "labels": labels,
            },
            "spec": {
                "ttlSecondsAfterFinished": 3600,
                "backoffLimit": self.config.retry_count,
                "activeDeadlineSeconds": spec.timeout_seconds,
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "wasm-worker",
                                "image": self.config.wasm_worker_image,
                                "env": env_vars,
                                "resources": {
                                    "limits": {
                                        "memory": memory_limit,
                                        "cpu": cpu_limit,
                                    },
                                    "requests": {
                                        "memory": f"{self.config.memory_mb // 2}Mi",
                                        "cpu": "100m",
                                    },
                                },
                            }
                        ],
                    },
                },
            },
        }

        if self.service_account:
            manifest["spec"]["template"]["spec"]["serviceAccountName"] = self.service_account

        return manifest

    def _create_container_job_manifest(self, spec: ContainerJobSpec) -> dict[str, Any]:
        """
        Create K8s Job manifest for a container check.

        Args:
            spec: Container job specification

        Returns:
            Kubernetes Job manifest as dict
        """
        job_name = f"griot-check-{spec.job_id[:8]}-{uuid.uuid4().hex[:6]}"

        # Environment variables for the check container
        env_vars = [
            {"name": "GRIOT_JOB_SPEC", "value": json.dumps(spec.to_dict())},
            {"name": "GRIOT_JOB_ID", "value": spec.job_id},
            {"name": "GRIOT_PARENT_JOB_ID", "value": spec.parent_job_id},
            {"name": "GRIOT_JOB_TYPE", "value": "container_check"},
            {"name": "GRIOT_CHECK_NAME", "value": spec.check.name},
            {
                "name": "GRIOT_CHECK_PARAMETERS",
                "value": json.dumps(spec.check.parameters),
            },
            {
                "name": "GRIOT_DATA_REFERENCE",
                "value": json.dumps(spec.data_reference),
            },
        ]

        if spec.callback_url:
            env_vars.append({"name": "GRIOT_CALLBACK_URL", "value": spec.callback_url})

        # Add custom environment from config
        for key, value in self.config.environment.items():
            env_vars.append({"name": key, "value": value})

        # Resource limits - use spec limits if provided, else defaults
        memory_limit = spec.resource_limits.get("memory", f"{self.config.memory_mb}Mi")
        cpu_limit = spec.resource_limits.get("cpu", f"{self.config.cpu_millicores}m")

        # Build labels
        labels = {
            "app": "griot-validation",
            "griot.io/job-type": "container-check",
            "griot.io/job-id": spec.job_id,
            "griot.io/parent-job-id": spec.parent_job_id,
            "griot.io/contract-id": spec.contract_id,
            "griot.io/check-name": spec.check.name,
            **self.config.labels,
        }

        manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.namespace,
                "labels": labels,
            },
            "spec": {
                "ttlSecondsAfterFinished": 3600,
                "backoffLimit": self.config.retry_count,
                "activeDeadlineSeconds": spec.timeout_seconds,
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "check",
                                "image": spec.image,
                                "env": env_vars,
                                "resources": {
                                    "limits": {
                                        "memory": memory_limit,
                                        "cpu": cpu_limit,
                                    },
                                    "requests": {
                                        "memory": f"{self.config.memory_mb // 2}Mi",
                                        "cpu": "100m",
                                    },
                                },
                            }
                        ],
                    },
                },
            },
        }

        if self.service_account:
            manifest["spec"]["template"]["spec"]["serviceAccountName"] = self.service_account

        return manifest

    async def dispatch_wasm_worker(self, spec: WasmJobSpec) -> DispatchResult:
        """
        Dispatch WASM checks to a griot-core worker pod.

        Creates a K8s Job running the griot-core WASM worker image.
        The worker executes all WASM checks sequentially.

        Args:
            spec: WASM job specification

        Returns:
            DispatchResult with job creation status
        """
        try:
            _, batch_api = self._get_api()

            manifest = self._create_wasm_worker_manifest(spec)
            job_name = manifest["metadata"]["name"]

            from kubernetes import client

            job = client.V1Job(**manifest)
            created = batch_api.create_namespaced_job(
                namespace=self.namespace,
                body=job,
            )

            job_uid = created.metadata.uid

            logger.info(
                "Created K8s WASM worker Job %s for job %s (uid=%s)",
                job_name,
                spec.job_id,
                job_uid,
            )

            return DispatchResult(
                success=True,
                job_id=spec.job_id,
                job_type="wasm_worker",
                backend=self.backend.value,
                invocation_id=f"{self.namespace}/{job_name}",
            )

        except Exception as e:
            logger.exception("Error creating K8s WASM worker Job for %s", spec.job_id)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="wasm_worker",
                backend=self.backend.value,
                error=str(e),
            )

    async def dispatch_container(self, spec: ContainerJobSpec) -> DispatchResult:
        """
        Dispatch a container check as a native K8s Job.

        Creates a K8s Job running the check's container image directly.
        This avoids Docker-in-Docker.

        Args:
            spec: Container job specification

        Returns:
            DispatchResult with job creation status
        """
        try:
            _, batch_api = self._get_api()

            manifest = self._create_container_job_manifest(spec)
            job_name = manifest["metadata"]["name"]

            from kubernetes import client

            job = client.V1Job(**manifest)
            created = batch_api.create_namespaced_job(
                namespace=self.namespace,
                body=job,
            )

            job_uid = created.metadata.uid

            logger.info(
                "Created K8s container check Job %s for check %s (uid=%s)",
                job_name,
                spec.check.name,
                job_uid,
            )

            return DispatchResult(
                success=True,
                job_id=spec.job_id,
                job_type="container",
                backend=self.backend.value,
                invocation_id=f"{self.namespace}/{job_name}",
            )

        except Exception as e:
            logger.exception("Error creating K8s container Job for check %s", spec.check.name)
            return DispatchResult(
                success=False,
                job_id=spec.job_id,
                job_type="container",
                backend=self.backend.value,
                error=str(e),
            )

    async def check_status(self, invocation_id: str) -> dict[str, Any]:
        """
        Check Kubernetes Job status.

        Args:
            invocation_id: The namespace/job-name from dispatch

        Returns:
            Job status information
        """
        try:
            _, batch_api = self._get_api()

            parts = invocation_id.split("/")
            if len(parts) != 2:
                return {"error": f"Invalid invocation_id format: {invocation_id}"}

            namespace, job_name = parts

            job = batch_api.read_namespaced_job_status(
                name=job_name,
                namespace=namespace,
            )

            status = job.status

            return {
                "invocation_id": invocation_id,
                "active": status.active or 0,
                "succeeded": status.succeeded or 0,
                "failed": status.failed or 0,
                "start_time": (status.start_time.isoformat() if status.start_time else None),
                "completion_time": (
                    status.completion_time.isoformat() if status.completion_time else None
                ),
                "conditions": [
                    {
                        "type": c.type,
                        "status": c.status,
                        "reason": c.reason,
                        "message": c.message,
                    }
                    for c in (status.conditions or [])
                ],
            }

        except Exception as e:
            logger.error("Error checking K8s Job status: %s", e)
            return {"error": str(e)}

    async def cancel(self, invocation_id: str) -> bool:
        """
        Cancel (delete) a Kubernetes Job.

        Args:
            invocation_id: The namespace/job-name to cancel

        Returns:
            True if deletion was successful
        """
        try:
            _, batch_api = self._get_api()

            parts = invocation_id.split("/")
            if len(parts) != 2:
                logger.error("Invalid invocation_id format: %s", invocation_id)
                return False

            namespace, job_name = parts

            from kubernetes import client

            batch_api.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                body=client.V1DeleteOptions(propagation_policy="Foreground"),
            )

            logger.info("Cancelled K8s Job %s", invocation_id)
            return True

        except Exception as e:
            logger.error("Error cancelling K8s Job: %s", e)
            return False

    async def health_check(self) -> bool:
        """
        Check if Kubernetes API is accessible.

        Returns:
            True if K8s API is reachable
        """
        try:
            api, _ = self._get_api()
            api.get_api_resources()
            return True
        except Exception as e:
            logger.error("K8s health check failed: %s", e)
            return False
