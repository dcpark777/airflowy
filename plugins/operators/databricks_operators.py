"""
Operators for Databricks.
Should include:
- SparkDatabricksOperator: Base operator for running Spark jobs on Databricks
"""

import json
import time
import urllib.error
import urllib.request
from typing import Optional, Dict, List, Any, Tuple, Callable

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook

# Retry behavior: no retry, retry immediately, or retry after backoff (delay in seconds)
RETRY_BEHAVIOR_NO_RETRY = "no_retry"
RETRY_BEHAVIOR_IMMEDIATE = "retry_immediate"
RETRY_BEHAVIOR_BACKOFF = "retry_backoff"
DEFAULT_RETRY_BACKOFF_SECONDS = 60
DEFAULT_MAX_TOTAL_RETRIES = 5


class SparkDatabricksOperator(BaseOperator):
    """
    Base operator for running Spark jobs on Databricks.
    
    This operator provides a foundation for Databricks Spark operations,
    handling cluster configuration, Spark settings, and job execution.
    Subclasses should override the execute method to implement specific
    job submission logic.
    
    Args:
        databricks_conn_id: The Airflow connection ID for Databricks.
        driver_node_type_id: The node type ID for the driver node.
        worker_node_type_id: The node type ID for worker nodes.
        num_workers: Number of worker nodes. Required if autoscale is False.
        dbr_version: Databricks Runtime version (e.g., "13.3.x-scala2.12").
        spark_config: Additional Spark configuration dictionary. Merged with defaults.
        spark_env_vars: Spark environment variables dictionary.
        job_parameters: Job parameters dictionary.
        libraries: List of library specifications (e.g., PyPI packages, Maven coordinates).
        autoscale: Whether to enable autoscaling. Defaults to False.
        autoscale_min_workers: Minimum number of workers when autoscaling. Defaults to 1.
        autoscale_max_workers: Maximum number of workers when autoscaling. Defaults to None.
        ebs_volume_type: EBS volume type for attached volumes. Defaults to None.
        ebs_volume_size: EBS volume size in GB. Defaults to None.
        ebs_volume_count: Number of EBS volumes to attach. Defaults to None.
        execution_timeout: Job execution timeout in seconds. Defaults to None.
        custom_tags: Custom tags dictionary for the cluster/job.
        availability: Instance availability type. Options: "SPOT", "ON_DEMAND". Defaults to "ON_DEMAND".
        spot_bid_price_percent: Spot bid price as percentage of on-demand price. Defaults to 100.
        cluster_init_scripts: List of cluster initialization scripts.
        service_principal_name: Service principal name for authentication. Defaults to None.
        fetch_logs: If True, fetch and log job run output and exceptions to the Airflow
            task log after the run completes. Subclasses must call _log_job_run_output(run_id)
            from execute() for this to take effect. Defaults to True.
        default_retry_backoff_seconds: Default delay in seconds when retry behavior is
            RETRY_BEHAVIOR_BACKOFF (e.g. throttling, resource availability). Defaults to 60.
        max_total_retries: Maximum number of retry attempts across all failures (cap to
            avoid unbounded loops). Defaults to 5.
        **kwargs: Additional arguments passed to BaseOperator.
    
    Example:
        ```python
        from airflow import DAG
        from operators.databricks_operators import SparkDatabricksOperator
        from datetime import datetime
        
        with DAG('databricks_dag', start_date=datetime(2023, 1, 1)) as dag:
            spark_task = SparkDatabricksOperator(
                task_id='run_spark_job',
                databricks_conn_id='databricks_default',
                driver_node_type_id='i3.xlarge',
                worker_node_type_id='i3.xlarge',
                num_workers=2,
                dbr_version='13.3.x-scala2.12',
                spark_config={'spark.sql.adaptive.enabled': 'true'},
                libraries=[{'pypi': {'package': 'pandas'}}],
            )
        ```
    """
    
    template_fields = ('databricks_conn_id', 'job_parameters', 'spark_config', 'spark_env_vars')
    
    def __init__(
        self,
        databricks_conn_id: str,
        driver_node_type_id: str,
        worker_node_type_id: str,
        num_workers: Optional[int] = None,
        dbr_version: Optional[str] = None,
        spark_config: Optional[Dict[str, Any]] = None,
        spark_env_vars: Optional[Dict[str, str]] = None,
        job_parameters: Optional[Dict[str, Any]] = None,
        libraries: Optional[List[Dict[str, Any]]] = None,
        autoscale: bool = False,
        autoscale_min_workers: int = 1,
        autoscale_max_workers: Optional[int] = None,
        ebs_volume_type: Optional[str] = None,
        ebs_volume_size: Optional[int] = None,
        ebs_volume_count: Optional[int] = None,
        execution_timeout: Optional[int] = None,
        custom_tags: Optional[Dict[str, str]] = None,
        availability: str = "ON_DEMAND",
        spot_bid_price_percent: int = 100,
        cluster_init_scripts: Optional[List[Dict[str, Any]]] = None,
        service_principal_name: Optional[str] = None,
        fetch_logs: bool = True,
        default_retry_backoff_seconds: int = DEFAULT_RETRY_BACKOFF_SECONDS,
        max_total_retries: int = DEFAULT_MAX_TOTAL_RETRIES,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.databricks_conn_id = databricks_conn_id
        self.fetch_logs = fetch_logs
        self.default_retry_backoff_seconds = default_retry_backoff_seconds
        self.max_total_retries = max_total_retries
        self.driver_node_type_id = driver_node_type_id
        self.worker_node_type_id = worker_node_type_id
        self.num_workers = num_workers
        self.dbr_version = dbr_version
        self.spark_config = spark_config or {}
        self.spark_env_vars = spark_env_vars or {}
        self.job_parameters = job_parameters or {}
        self.libraries = libraries or []
        self.autoscale = autoscale
        self.autoscale_min_workers = autoscale_min_workers
        self.autoscale_max_workers = autoscale_max_workers
        self.ebs_volume_type = ebs_volume_type
        self.ebs_volume_size = ebs_volume_size
        self.ebs_volume_count = ebs_volume_count
        self.execution_timeout = execution_timeout
        self.custom_tags = custom_tags or {}
        self.availability = availability
        self.spot_bid_price_percent = spot_bid_price_percent
        self.cluster_init_scripts = cluster_init_scripts or []
        self.service_principal_name = service_principal_name
        
        # Default Spark configuration optimized for Databricks
        self.spark_conf = {
            "spark.databricks.delta.preview.enabled": "true",
            "spark.sql.shuffle.partitions": "auto",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
            "spark.databricks.delta.targetFileSize": "128m",
            "spark.databricks.io.cache.compression.enabled": "false",
            "spark.databricks.io.cache.enabled": "true",
            "spark.databricks.io.cache.maxMetaDataCache": "5g",
            "spark.driver.maxResultSize": "0",
            "spark.rpc.message.maxSize": "1024",
        }
        # Merge user-provided Spark config with defaults
        if self.spark_config:
            self.spark_conf.update(self.spark_config)
    
    def get_hook(self):
        """
        Get the Databricks hook for this connection.

        Returns:
            The Databricks hook instance.
        """
        return BaseHook.get_hook(conn_id=self.databricks_conn_id)

    def _get_run_output(self, run_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch run output and metadata from the Databricks Jobs API (get-output and run details).

        Uses the Airflow Databricks connection (host + token) to call the Databricks REST API.
        Compatible with any Databricks connection; does not require a specific provider hook.

        Args:
            run_id: The Databricks job run ID.

        Returns:
            A dict with keys such as metadata, notebook_output, sql_output, error (message,
            stack_trace), run_page_url, or None if the API call fails (e.g. connection not
            configured or endpoint unavailable).
        """
        try:
            conn = BaseHook.get_connection(self.databricks_conn_id)
            host = (conn.host or "").rstrip("/")
            if not host:
                self.log.warning("Databricks connection %s has no host; cannot fetch run output.", self.databricks_conn_id)
                return None
            if not host.startswith("http"):
                host = "https://" + host
            extra = getattr(conn, "extra_dejson", None) or {}
            token = conn.password or (extra.get("token") if isinstance(extra, dict) else None)
            if not token:
                self.log.warning("Databricks connection %s has no token; cannot fetch run output.", self.databricks_conn_id)
                return None
        except Exception as e:  # noqa: BLE001
            self.log.warning("Could not get Databricks connection for run output: %s", e)
            return None

        result: Dict[str, Any] = {}

        # Get run details (state, run_page_url, etc.)
        try:
            url = f"{host}/api/2.1/jobs/runs/get?run_id={run_id}"
            req = urllib.request.Request(url, method="GET")
            req.add_header("Authorization", "Bearer " + token)
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            result["metadata"] = data
            result["run_page_url"] = data.get("run_page_url")
        except urllib.error.HTTPError as e:
            result["metadata"] = {"run_id": run_id, "http_error": str(e.code), "reason": e.reason}
            if e.code == 404:
                self.log.warning("Run %s not found (404). It may have been deleted.", run_id)
            else:
                self.log.warning("Failed to get run details for %s: %s %s", run_id, e.code, e.reason)
        except Exception as e:  # noqa: BLE001
            self.log.warning("Error fetching run details for %s: %s", run_id, e)
            result["metadata"] = {"run_id": run_id, "error": str(e)}

        # Get run output (notebook_output, sql_output, error message/stack_trace)
        try:
            url = f"{host}/api/2.1/jobs/runs/get-output?run_id={run_id}"
            req = urllib.request.Request(url, method="GET")
            req.add_header("Authorization", "Bearer " + token)
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            result["notebook_output"] = data.get("notebook_output")
            result["sql_output"] = data.get("sql_output")
            if data.get("error"):
                result["error"] = data["error"]
            if data.get("run_page_url"):
                result["run_page_url"] = data["run_page_url"]
        except urllib.error.HTTPError as e:
            result["output_error"] = {"code": e.code, "reason": e.reason}
            if e.code == 400:
                try:
                    body = e.read().decode() if e.fp else ""
                    if "multiple tasks" in body.lower():
                        result["error"] = {"message": "Run has multiple tasks; output must be fetched per task run."}
                except Exception:  # noqa: S110
                    pass
        except Exception as e:  # noqa: BLE001
            result["output_error"] = {"message": str(e)}

        return result

    def _log_job_run_output(self, run_id: int) -> None:
        """
        Fetch the job run output from Databricks and write it to the Airflow task log.

        Logs run metadata (e.g. state, run_page_url), any notebook/sql output, and any
        error message or stack trace. Call this from execute() after the run completes
        (success or failure) when fetch_logs is True.

        Args:
            run_id: The Databricks job run ID.
        """
        if not self.fetch_logs:
            return
        output = self._get_run_output(run_id)
        if not output:
            return

        self.log.info("=== Databricks run_id: %s ===", run_id)

        metadata = output.get("metadata") or {}
        if isinstance(metadata, dict):
            state = metadata.get("state", {}).get("life_cycle_state") or metadata.get("state")
            result_state = metadata.get("state", {}).get("result_state") if isinstance(metadata.get("state"), dict) else None
            run_page_url = output.get("run_page_url") or metadata.get("run_page_url")
            if state:
                self.log.info("Run state: %s%s", state, f" (result: {result_state})" if result_state else "")
            if run_page_url:
                self.log.info("Run URL: %s", run_page_url)

        notebook_output = output.get("notebook_output")
        if notebook_output:
            self.log.info("--- Notebook output ---")
            if isinstance(notebook_output, dict):
                for k, v in notebook_output.items():
                    self.log.info("%s: %s", k, v)
            else:
                self.log.info("%s", notebook_output)

        sql_output = output.get("sql_output")
        if sql_output:
            self.log.info("--- SQL output ---")
            if isinstance(sql_output, dict):
                for k, v in sql_output.items():
                    self.log.info("%s: %s", k, v)
            else:
                self.log.info("%s", sql_output)

        error = output.get("error")
        if error:
            self.log.error("--- Databricks run error ---")
            if isinstance(error, dict):
                msg = error.get("message") or error.get("summary") or str(error)
                self.log.error("Message: %s", msg)
                stack = error.get("stack_trace")
                if stack:
                    self.log.error("Stack trace:\n%s", stack)
            else:
                self.log.error("%s", error)

        output_err = output.get("output_error")
        if output_err:
            self.log.warning("Could not retrieve full run output: %s", output_err)

    def build_cluster_config(self) -> Dict[str, Any]:
        """
        Build the cluster configuration dictionary.
        
        This method can be overridden by subclasses to customize cluster configuration.
        
        Returns:
            Dictionary containing cluster configuration.
        """
        cluster_config = {
            "driver_node_type_id": self.driver_node_type_id,
            "node_type_id": self.worker_node_type_id,
            "spark_version": self.dbr_version,
            "spark_conf": self.spark_conf,
            "spark_env_vars": self.spark_env_vars,
        }
        
        # Add worker configuration
        if self.autoscale:
            cluster_config["autoscale"] = {
                "min_workers": self.autoscale_min_workers,
                "max_workers": self.autoscale_max_workers,
            }
        else:
            if self.num_workers is None:
                raise ValueError("num_workers is required when autoscale is False")
            cluster_config["num_workers"] = self.num_workers
        
        # Add EBS volume configuration if provided
        if self.ebs_volume_type:
            cluster_config["aws_attributes"] = {
                "ebs_volume_type": self.ebs_volume_type,
                "ebs_volume_size": self.ebs_volume_size,
                "ebs_volume_count": self.ebs_volume_count,
            }
        
        # Add availability configuration
        if self.availability == "SPOT":
            if "aws_attributes" not in cluster_config:
                cluster_config["aws_attributes"] = {}
            cluster_config["aws_attributes"]["availability"] = "SPOT"
            cluster_config["aws_attributes"]["spot_bid_price_percent"] = self.spot_bid_price_percent
        else:
            if "aws_attributes" not in cluster_config:
                cluster_config["aws_attributes"] = {}
            cluster_config["aws_attributes"]["availability"] = "ON_DEMAND"
        
        # Add libraries if provided
        if self.libraries:
            cluster_config["libraries"] = self.libraries
        
        # Add cluster init scripts if provided
        if self.cluster_init_scripts:
            cluster_config["init_scripts"] = self.cluster_init_scripts
        
        # Add custom tags if provided
        if self.custom_tags:
            cluster_config["custom_tags"] = self.custom_tags
        
        return cluster_config
    
    def execute(self, context: Context) -> Any:
        """
        Execute the Databricks Spark job.

        This is a base implementation that should be overridden by subclasses
        to implement specific job submission logic (e.g., submit_run, run_now, etc.).

        Subclasses should call :meth:`_log_job_run_output` with the run_id after the
        run completes (success or failure) when :attr:`fetch_logs` is True, so that
        job logs and exceptions appear in the Airflow task log.

        Args:
            context: The task execution context.

        Returns:
            The result of the job execution (typically a run_id or job result).

        Raises:
            NotImplementedError: If this base method is called directly.
        """
        raise NotImplementedError(
            "Subclasses must implement the execute method to define "
            "how the Databricks job should be submitted and executed."
        )

    def _run_with_retries(
        self,
        context: Context,
        run_fn: Callable[[Context], Any],
        run_id_for_logs: Optional[int] = None,
    ) -> Any:
        """
        Run a callable with smart retries based on exception classification.

        Calls run_fn(context). If it raises, the exception message is passed to
        _retry_handler to decide: no retry (re-raise), retry immediately, or retry
        after backoff. Logs and fetches job output when fetch_logs is True and
        run_id_for_logs is provided (after final failure).

        Args:
            context: Airflow task context.
            run_fn: Callable that performs the job (submit + wait). Should raise on
                failure; the exception message is used for retry classification.
            run_id_for_logs: If set and fetch_logs is True, _log_job_run_output is
                called with this run_id when the run fails (before re-raising).

        Returns:
            The return value of run_fn(context) on success.

        Raises:
            The last exception raised by run_fn if all retries are exhausted or
            the failure is classified as non-retryable.
        """
        last_exception = None
        attempt = 0

        while True:
            attempt += 1
            try:
                return run_fn(context)
            except Exception as e:
                last_exception = e
                if attempt > self.max_total_retries:
                    self.log.warning(
                        "Max total retries (%s) exceeded.", self.max_total_retries
                    )
                    if run_id_for_logs is not None and self.fetch_logs:
                        self._log_job_run_output(run_id_for_logs)
                    raise

                msg = str(e) or getattr(e, "message", "") or ""
                behavior, reason, max_retries, retry_delay_seconds = self._retry_handler(
                    msg
                )

                if behavior == RETRY_BEHAVIOR_NO_RETRY:
                    self.log.warning("Failure classified as non-retryable: %s", reason)
                    if run_id_for_logs is not None and self.fetch_logs:
                        self._log_job_run_output(run_id_for_logs)
                    raise

                if max_retries <= 0:
                    if run_id_for_logs is not None and self.fetch_logs:
                        self._log_job_run_output(run_id_for_logs)
                    raise

                self.log.warning(
                    "Attempt %s failed (%s). Retrying (delay=%ss). Reason: %s",
                    attempt,
                    reason,
                    retry_delay_seconds,
                    msg[:200] + ("..." if len(msg) > 200 else ""),
                )
                if retry_delay_seconds > 0:
                    self.log.info(
                        "Sleeping %s seconds before retry (backoff).",
                        retry_delay_seconds,
                    )
                    time.sleep(retry_delay_seconds)

    def _retry_handler(self, spark_error_message: str) -> Tuple[str, str, int, int]:
        """
        Determine retry behavior based on the Spark/Databricks error message.

        Classifies the failure into one of three behaviors:
        - **No retry**: Code/config errors (syntax, auth, file not found, etc.). Retry won't help.
        - **Retry immediately**: Transient API/network issues. A retry may fix right away.
        - **Retry after backoff**: Throttling or resource availability. Waiting before retry may help.

        Args:
            spark_error_message: The error message (and optionally logs) from the job.

        Returns:
            tuple: (retry_behavior, failure_reason, max_retries, retry_delay_seconds)
            - retry_behavior: One of RETRY_BEHAVIOR_NO_RETRY, RETRY_BEHAVIOR_IMMEDIATE,
              RETRY_BEHAVIOR_BACKOFF.
            - failure_reason: Human-readable reason for the classification.
            - max_retries: Max retries allowed (0 when retry_behavior is NO_RETRY).
            - retry_delay_seconds: Delay before each retry (0 for IMMEDIATE; used for BACKOFF).

        Note:
            Subclasses can override _get_retryable_error_patterns() and
            _get_non_retryable_error_patterns() to customize behavior.
        """
        if not spark_error_message:
            return (
                RETRY_BEHAVIOR_IMMEDIATE,
                "Empty error message - allowing retry",
                3,
                0,
            )

        error_lower = spark_error_message.lower()

        # Non-retryable first (code/config/auth/etc.)
        non_retryable_patterns = self._get_non_retryable_error_patterns()
        for pattern, reason in sorted(
            non_retryable_patterns.items(), key=lambda x: len(x[0]), reverse=True
        ):
            if pattern.lower() in error_lower:
                return (
                    RETRY_BEHAVIOR_NO_RETRY,
                    f"Non-retryable error: {reason}",
                    0,
                    0,
                )

        # Retryable: immediate (e.g. API/network) vs backoff (e.g. throttling, resources)
        retryable_patterns = self._get_retryable_error_patterns()
        backoff = self.default_retry_backoff_seconds
        for pattern, (reason, max_retries, retry_delay_seconds) in sorted(
            retryable_patterns.items(), key=lambda x: len(x[0]), reverse=True
        ):
            if pattern.lower() in error_lower:
                delay = retry_delay_seconds if retry_delay_seconds is not None else backoff
                behavior = (
                    RETRY_BEHAVIOR_BACKOFF
                    if delay > 0
                    else RETRY_BEHAVIOR_IMMEDIATE
                )
                return (
                    behavior,
                    f"Retryable error: {reason}",
                    max_retries,
                    delay,
                )

        # Unknown: allow retry with backoff (conservative)
        return (
            RETRY_BEHAVIOR_BACKOFF,
            "Unknown error - allowing retry with backoff",
            2,
            backoff,
        )

    def _get_retryable_error_patterns(self) -> Dict[str, Tuple[str, int, Optional[int]]]:
        """
        Retryable error patterns with (reason, max_retries, retry_delay_seconds).

        retry_delay_seconds: 0 = retry immediately (e.g. API/network). None or >0 = use
        default_retry_backoff_seconds (e.g. throttling, resource availability).
        """
        backoff = None  # use operator's default_retry_backoff_seconds
        immediate = 0

        return {
            # Retry immediately: API/network/transient service
            "connection": ("Connection error - network issue", 3, immediate),
            "timeout": ("Timeout error - may resolve on retry", 3, immediate),
            "connection reset": ("Connection reset - transient network issue", 3, immediate),
            "connection refused": (
                "Connection refused - service may be temporarily unavailable",
                3,
                immediate,
            ),
            "network": ("Network error - transient issue", 3, immediate),
            "service unavailable": ("Service unavailable - transient", 3, immediate),
            "503": ("Service unavailable (503)", 3, immediate),
            "502": ("Bad gateway (502) - transient", 3, immediate),
            "504": ("Gateway timeout (504)", 3, immediate),
            "read timeout": ("Read timeout - network issue", 3, immediate),
            "write timeout": ("Write timeout - network issue", 3, immediate),
            "temporary failure": ("Temporary failure - will retry", 3, immediate),
            "temporary error": ("Temporary error - will retry", 3, immediate),
            "retry": ("Error suggests retry", 3, immediate),
            # Retry after backoff: throttling, resources, cluster
            "throttl": ("Rate limiting - retry after backoff", 3, backoff),
            "rate limit": ("Rate limit exceeded - retry after backoff", 3, backoff),
            "out of memory": ("Out of memory - retry after backoff", 2, backoff),
            "no space left": ("Disk space issue - retry after backoff", 2, backoff),
            "resource": ("Resource unavailable - retry after backoff", 3, backoff),
            "cluster": ("Cluster issue - retry after backoff", 3, backoff),
            "cluster terminated": ("Cluster terminated unexpectedly", 2, backoff),
            "spot instance": ("Spot instance interruption", 3, backoff),
            "instance terminated": ("Instance terminated - transient", 3, backoff),
            "execution timeout": ("Execution timeout - retry after backoff", 2, backoff),
            "lock": ("Lock contention - retry after backoff", 2, backoff),
            "deadlock": ("Deadlock detected - retry after backoff", 2, backoff),
        }
    
    def _get_non_retryable_error_patterns(self) -> Dict[str, str]:
        """
        Get a dictionary of non-retryable error patterns and their failure reasons.
        
        Returns:
            Dictionary mapping error pattern strings to human-readable failure reasons.
            Patterns are matched case-insensitively against error messages.
        
        Note:
            Subclasses can override this method to customize non-retryable error patterns.
        """
        return {
            # Syntax and code errors
            "syntax error": "Syntax error in code",
            "parse error": "Parse error - code issue",
            "compilation error": "Compilation error - code issue",
            "nameerror": "NameError - undefined variable or function",
            "typeerror": "TypeError - type mismatch",
            "attributeerror": "AttributeError - missing attribute",
            "indentationerror": "IndentationError - code formatting issue",
            
            # Configuration errors
            "configuration error": "Configuration error - invalid settings",
            "invalid configuration": "Invalid configuration",
            "missing configuration": "Missing required configuration",
            "config": "Configuration issue",
            
            # Authentication and authorization errors
            "authentication": "Authentication failed - credentials issue",
            "authorization": "Authorization failed - permission issue",
            "unauthorized": "Unauthorized access",
            "forbidden": "Forbidden - access denied",
            "401": "Unauthorized (401)",
            "403": "Forbidden (403)",
            
            # Data validation errors
            "data validation": "Data validation failed",
            "invalid data": "Invalid data format or content",
            "schema": "Schema mismatch or validation error",
            "null pointer": "Null pointer exception - data issue",
            "nullreference": "Null reference - data issue",
            
            # File and path errors
            "file not found": "File not found - path issue",
            "no such file": "File does not exist",
            "directory not found": "Directory does not exist",
            "path not found": "Path does not exist",
            
            # Logic errors
            "assertion": "Assertion failed - logic error",
            "illegal argument": "Illegal argument - invalid input",
            "illegal state": "Illegal state - logic error",
            "index out of bounds": "Index out of bounds - logic error",
            
            # Version and compatibility errors
            "version": "Version mismatch or incompatibility",
            "incompatible": "Incompatible version or format",
            "not supported": "Feature not supported",
            
            # Client errors (4xx)
            "400": "Bad request (400) - invalid request",
            "404": "Not found (404) - resource does not exist",
            "405": "Method not allowed (405)",
            "409": "Conflict (409) - resource conflict",
            "422": "Unprocessable entity (422) - validation error",
        }
