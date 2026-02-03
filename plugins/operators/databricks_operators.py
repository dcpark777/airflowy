"""
Operators for Databricks.
Should include:
- SparkDatabricksOperator: Base operator for running Spark jobs on Databricks
"""

from typing import Optional, Dict, List, Any, Tuple

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook


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
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        self.databricks_conn_id = databricks_conn_id
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

    def _retry_handler(self, spark_error_message: str) -> Tuple[bool, str, int]:
        """
        Determine the retry behavior based on the Spark error message.
        
        This method analyzes the error message and determines whether the job should
        fail immediately or be retried, along with the appropriate retry count.
        
        Args:
            spark_error_message: The error message from the Spark job.
        
        Returns:
            tuple: (instant_fail, failure_reason, max_retries)
            - instant_fail (bool): Whether to fail the job immediately (True) or allow retries (False).
            - failure_reason (str): A human-readable reason for the failure classification.
            - max_retries (int): The maximum number of retries allowed (0 if instant_fail is True).
        
        Note:
            The error classification uses pattern matching against known error patterns.
            Subclasses can override `_get_retryable_error_patterns()` and 
            `_get_non_retryable_error_patterns()` to customize behavior.
        """
        if not spark_error_message:
            return False, "Empty error message - allowing retry", 3
        
        error_lower = spark_error_message.lower()
        
        # Check for non-retryable errors first (fail immediately)
        # Sort by pattern length (longest first) to match specific patterns before general ones
        non_retryable_patterns = self._get_non_retryable_error_patterns()
        for pattern, reason in sorted(non_retryable_patterns.items(), key=lambda x: len(x[0]), reverse=True):
            if pattern.lower() in error_lower:
                return True, f"Non-retryable error: {reason}", 0
        
        # Check for retryable errors (allow retries with specific counts)
        # Sort by pattern length (longest first) to match specific patterns before general ones
        retryable_patterns = self._get_retryable_error_patterns()
        for pattern, (reason, max_retries) in sorted(retryable_patterns.items(), key=lambda x: len(x[0]), reverse=True):
            if pattern.lower() in error_lower:
                return False, f"Retryable error: {reason}", max_retries
        
        # Default: allow retries for unknown errors (conservative approach)
        return False, "Unknown error - allowing retry", 2
    
    def _get_retryable_error_patterns(self) -> Dict[str, Tuple[str, int]]:
        """
        Get a dictionary of retryable error patterns and their retry configurations.
        
        Returns:
            Dictionary mapping error pattern strings to tuples of (reason, max_retries).
            Patterns are matched case-insensitively against error messages.
        
        Note:
            Subclasses can override this method to customize retryable error patterns.
        """
        return {
            # Network and connectivity errors
            "connection": ("Connection error - network issue", 3),
            "timeout": ("Timeout error - may resolve on retry", 3),
            "connection reset": ("Connection reset - transient network issue", 3),
            "connection refused": ("Connection refused - service may be temporarily unavailable", 3),
            "network": ("Network error - transient issue", 3),
            
            # Resource availability errors
            "out of memory": ("Out of memory - may succeed with retry", 2),
            "no space left": ("Disk space issue - may be resolved", 2),
            "resource": ("Resource unavailable - transient issue", 3),
            "throttl": ("Rate limiting - will retry", 3),
            "rate limit": ("Rate limit exceeded - will retry", 3),
            
            # Databricks-specific transient errors
            "cluster": ("Cluster issue - may resolve on retry", 3),
            "cluster terminated": ("Cluster terminated unexpectedly", 2),
            "spot instance": ("Spot instance interruption", 3),
            "instance terminated": ("Instance terminated - transient", 3),
            
            # Service availability errors
            "service unavailable": ("Service unavailable - transient", 3),
            "503": ("Service unavailable (503)", 3),
            "502": ("Bad gateway (502) - transient", 3),
            "504": ("Gateway timeout (504)", 3),
            
            # Timeout errors
            "execution timeout": ("Execution timeout - may succeed on retry", 2),
            "read timeout": ("Read timeout - network issue", 3),
            "write timeout": ("Write timeout - network issue", 3),
            
            # Lock and contention errors
            "lock": ("Lock contention - may resolve on retry", 2),
            "deadlock": ("Deadlock detected - may resolve on retry", 2),
            
            # Transient data source errors
            "temporary failure": ("Temporary failure - will retry", 3),
            "temporary error": ("Temporary error - will retry", 3),
            "retry": ("Error suggests retry", 3),
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
