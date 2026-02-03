"""Tests for Databricks operators plugin."""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Set Airflow environment before importing
os.environ.setdefault('AIRFLOW_HOME', str(Path(__file__).parent.parent))
os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:///airflow.db')

from airflow.utils.context import Context
from operators.databricks_operators import SparkDatabricksOperator


class TestSparkDatabricksOperator:
    """Tests for SparkDatabricksOperator."""

    def test_operator_initialization(self):
        """Test basic operator initialization."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
        )
        assert operator.task_id == 'test_spark_job'
        assert operator.databricks_conn_id == 'databricks_default'
        assert operator.driver_node_type_id == 'i3.xlarge'
        assert operator.worker_node_type_id == 'i3.xlarge'
        assert operator.num_workers == 2

    def test_operator_default_values(self):
        """Test operator default values."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        assert operator.autoscale is False
        assert operator.autoscale_min_workers == 1
        assert operator.availability == "ON_DEMAND"
        assert operator.spot_bid_price_percent == 100
        assert operator.spark_config == {}
        assert operator.spark_env_vars == {}
        assert operator.job_parameters == {}
        assert operator.libraries == []
        assert operator.custom_tags == {}
        assert operator.cluster_init_scripts == []

    def test_operator_with_all_parameters(self):
        """Test operator with all parameters set."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.2xlarge',
            num_workers=4,
            dbr_version='13.3.x-scala2.12',
            spark_config={'spark.sql.adaptive.enabled': 'true'},
            spark_env_vars={'ENV_VAR': 'value'},
            job_parameters={'param1': 'value1'},
            libraries=[{'pypi': {'package': 'pandas'}}],
            autoscale=True,
            autoscale_min_workers=2,
            autoscale_max_workers=8,
            ebs_volume_type='gp3',
            ebs_volume_size=100,
            ebs_volume_count=1,
            execution_timeout=3600,
            custom_tags={'team': 'data-engineering'},
            availability='SPOT',
            spot_bid_price_percent=80,
            cluster_init_scripts=[{'dbfs': {'destination': 'dbfs:/scripts/init.sh'}}],
            service_principal_name='sp@example.com',
        )
        assert operator.num_workers == 4
        assert operator.dbr_version == '13.3.x-scala2.12'
        assert operator.spark_config == {'spark.sql.adaptive.enabled': 'true'}
        assert operator.spark_env_vars == {'ENV_VAR': 'value'}
        assert operator.job_parameters == {'param1': 'value1'}
        assert len(operator.libraries) == 1
        assert operator.autoscale is True
        assert operator.autoscale_min_workers == 2
        assert operator.autoscale_max_workers == 8
        assert operator.ebs_volume_type == 'gp3'
        assert operator.ebs_volume_size == 100
        assert operator.ebs_volume_count == 1
        assert operator.execution_timeout == 3600
        assert operator.custom_tags == {'team': 'data-engineering'}
        assert operator.availability == 'SPOT'
        assert operator.spot_bid_price_percent == 80
        assert len(operator.cluster_init_scripts) == 1
        assert operator.service_principal_name == 'sp@example.com'

    def test_operator_spark_conf_defaults(self):
        """Test that default Spark configuration is set correctly."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        assert 'spark.databricks.delta.preview.enabled' in operator.spark_conf
        assert operator.spark_conf['spark.databricks.delta.preview.enabled'] == 'true'
        assert 'spark.sql.shuffle.partitions' in operator.spark_conf
        assert operator.spark_conf['spark.sql.shuffle.partitions'] == 'auto'

    def test_operator_spark_conf_merge(self):
        """Test that user-provided Spark config merges with defaults."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            spark_config={'spark.sql.adaptive.enabled': 'true', 'custom.setting': 'value'},
        )
        # Defaults should still be present
        assert 'spark.databricks.delta.preview.enabled' in operator.spark_conf
        # User config should be merged
        assert operator.spark_conf['spark.sql.adaptive.enabled'] == 'true'
        assert operator.spark_conf['custom.setting'] == 'value'

    def test_operator_template_fields(self):
        """Test that template fields are set correctly."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        assert 'databricks_conn_id' in operator.template_fields
        assert 'job_parameters' in operator.template_fields
        assert 'spark_config' in operator.template_fields
        assert 'spark_env_vars' in operator.template_fields

    def test_build_cluster_config_basic(self):
        """Test building basic cluster configuration."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
            dbr_version='13.3.x-scala2.12',
        )
        config = operator.build_cluster_config()
        
        assert config['driver_node_type_id'] == 'i3.xlarge'
        assert config['node_type_id'] == 'i3.xlarge'
        assert config['spark_version'] == '13.3.x-scala2.12'
        assert config['num_workers'] == 2
        assert 'spark_conf' in config
        assert 'spark_env_vars' in config

    def test_build_cluster_config_autoscale(self):
        """Test building cluster configuration with autoscaling."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            autoscale=True,
            autoscale_min_workers=2,
            autoscale_max_workers=8,
        )
        config = operator.build_cluster_config()
        
        assert 'autoscale' in config
        assert config['autoscale']['min_workers'] == 2
        assert config['autoscale']['max_workers'] == 8
        assert 'num_workers' not in config

    def test_build_cluster_config_autoscale_requires_num_workers(self):
        """Test that num_workers is required when autoscale is False."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            autoscale=False,
        )
        with pytest.raises(ValueError, match="num_workers is required when autoscale is False"):
            operator.build_cluster_config()

    def test_build_cluster_config_ebs_volumes(self):
        """Test building cluster configuration with EBS volumes."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
            ebs_volume_type='gp3',
            ebs_volume_size=100,
            ebs_volume_count=1,
        )
        config = operator.build_cluster_config()
        
        assert 'aws_attributes' in config
        assert config['aws_attributes']['ebs_volume_type'] == 'gp3'
        assert config['aws_attributes']['ebs_volume_size'] == 100
        assert config['aws_attributes']['ebs_volume_count'] == 1

    def test_build_cluster_config_spot_instances(self):
        """Test building cluster configuration with spot instances."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
            availability='SPOT',
            spot_bid_price_percent=80,
        )
        config = operator.build_cluster_config()
        
        assert 'aws_attributes' in config
        assert config['aws_attributes']['availability'] == 'SPOT'
        assert config['aws_attributes']['spot_bid_price_percent'] == 80

    def test_build_cluster_config_on_demand_instances(self):
        """Test building cluster configuration with on-demand instances."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
            availability='ON_DEMAND',
        )
        config = operator.build_cluster_config()
        
        assert 'aws_attributes' in config
        assert config['aws_attributes']['availability'] == 'ON_DEMAND'

    def test_build_cluster_config_libraries(self):
        """Test building cluster configuration with libraries."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
            libraries=[{'pypi': {'package': 'pandas'}}, {'maven': {'coordinates': 'org.apache.spark:spark-core_2.12:3.0.0'}}],
        )
        config = operator.build_cluster_config()
        
        assert 'libraries' in config
        assert len(config['libraries']) == 2

    def test_build_cluster_config_custom_tags(self):
        """Test building cluster configuration with custom tags."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
            custom_tags={'team': 'data-engineering', 'environment': 'prod'},
        )
        config = operator.build_cluster_config()
        
        assert 'custom_tags' in config
        assert config['custom_tags']['team'] == 'data-engineering'
        assert config['custom_tags']['environment'] == 'prod'

    def test_build_cluster_config_init_scripts(self):
        """Test building cluster configuration with init scripts."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
            cluster_init_scripts=[{'dbfs': {'destination': 'dbfs:/scripts/init.sh'}}],
        )
        config = operator.build_cluster_config()
        
        assert 'init_scripts' in config
        assert len(config['init_scripts']) == 1

    @patch('operators.databricks_operators.BaseHook')
    def test_get_hook(self, mock_base_hook):
        """Test getting the Databricks hook."""
        mock_hook = MagicMock()
        mock_base_hook.get_hook.return_value = mock_hook
        
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        
        hook = operator.get_hook()
        
        assert hook == mock_hook
        mock_base_hook.get_hook.assert_called_once_with(conn_id='databricks_default')

    def test_execute_not_implemented(self):
        """Test that execute raises NotImplementedError."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        context = {'ti': MagicMock()}
        
        with pytest.raises(NotImplementedError):
            operator.execute(context)


class TestRetryHandler:
    """Tests for _retry_handler method."""

    def test_retry_handler_empty_message(self):
        """Test retry handler with empty error message."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("")
        assert instant_fail is False
        assert "Empty error message" in reason
        assert max_retries == 3

    def test_retry_handler_none_message(self):
        """Test retry handler with None error message."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler(None)
        assert instant_fail is False
        assert "Empty error message" in reason
        assert max_retries == 3

    # Non-retryable error tests
    def test_retry_handler_syntax_error(self):
        """Test retry handler with syntax error (non-retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Syntax error in code")
        assert instant_fail is True
        assert "Syntax error" in reason
        assert max_retries == 0

    def test_retry_handler_authentication_error(self):
        """Test retry handler with authentication error (non-retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Authentication failed")
        assert instant_fail is True
        assert "Authentication" in reason
        assert max_retries == 0

    def test_retry_handler_file_not_found(self):
        """Test retry handler with file not found error (non-retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("File not found: /path/to/file")
        assert instant_fail is True
        assert "File not found" in reason
        assert max_retries == 0

    def test_retry_handler_configuration_error(self):
        """Test retry handler with configuration error (non-retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Invalid configuration")
        assert instant_fail is True
        assert "Invalid configuration" in reason or "configuration" in reason.lower()
        assert max_retries == 0

    def test_retry_handler_http_401(self):
        """Test retry handler with HTTP 401 error (non-retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("HTTP 401 Unauthorized")
        assert instant_fail is True
        assert "401" in reason or "Unauthorized" in reason
        assert max_retries == 0

    # Retryable error tests
    def test_retry_handler_connection_error(self):
        """Test retry handler with connection error (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Connection error occurred")
        assert instant_fail is False
        assert "Connection error" in reason
        assert max_retries == 3

    def test_retry_handler_timeout_error(self):
        """Test retry handler with timeout error (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Timeout error")
        assert instant_fail is False
        assert "Timeout" in reason
        assert max_retries == 3

    def test_retry_handler_connection_reset(self):
        """Test retry handler with connection reset error (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Connection reset by peer")
        assert instant_fail is False
        assert "Connection reset" in reason
        assert max_retries == 3

    def test_retry_handler_out_of_memory(self):
        """Test retry handler with out of memory error (retryable with fewer retries)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Out of memory error")
        assert instant_fail is False
        assert "Out of memory" in reason
        assert max_retries == 2

    def test_retry_handler_cluster_terminated(self):
        """Test retry handler with cluster terminated error (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Cluster terminated unexpectedly")
        assert instant_fail is False
        assert "Cluster terminated" in reason
        assert max_retries == 2

    def test_retry_handler_spot_instance(self):
        """Test retry handler with spot instance interruption (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Spot instance interrupted")
        assert instant_fail is False
        assert "Spot instance" in reason
        assert max_retries == 3

    def test_retry_handler_service_unavailable(self):
        """Test retry handler with service unavailable error (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Service unavailable")
        assert instant_fail is False
        assert "Service unavailable" in reason
        assert max_retries == 3

    def test_retry_handler_http_503(self):
        """Test retry handler with HTTP 503 error (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("HTTP 503 Service Unavailable")
        assert instant_fail is False
        # "service unavailable" (longer pattern) matches before "503", but both are retryable
        assert "Service unavailable" in reason or "503" in reason
        assert max_retries == 3

    def test_retry_handler_rate_limit(self):
        """Test retry handler with rate limit error (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Rate limit exceeded")
        assert instant_fail is False
        assert "Rate limit" in reason
        assert max_retries == 3

    def test_retry_handler_lock_contention(self):
        """Test retry handler with lock contention error (retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Lock contention detected")
        assert instant_fail is False
        assert "Lock" in reason
        assert max_retries == 2

    def test_retry_handler_unknown_error(self):
        """Test retry handler with unknown error (defaults to retryable)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        instant_fail, reason, max_retries = operator._retry_handler("Some unknown error message")
        assert instant_fail is False
        assert "Unknown error" in reason
        assert max_retries == 2

    def test_retry_handler_case_insensitive(self):
        """Test that retry handler is case-insensitive."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        # Test uppercase
        instant_fail, reason, max_retries = operator._retry_handler("CONNECTION ERROR")
        assert instant_fail is False
        assert max_retries == 3
        
        # Test mixed case
        instant_fail, reason, max_retries = operator._retry_handler("Syntax Error In Code")
        assert instant_fail is True
        assert max_retries == 0

    def test_retry_handler_pattern_matching(self):
        """Test that retry handler matches patterns within error messages."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        # Pattern should match even if it's part of a larger message
        error_msg = "An error occurred: Connection reset by peer. Please try again."
        instant_fail, reason, max_retries = operator._retry_handler(error_msg)
        assert instant_fail is False
        assert "Connection reset" in reason
        assert max_retries == 3

    def test_retry_handler_custom_patterns_override(self):
        """Test that subclasses can override error patterns."""
        class CustomOperator(SparkDatabricksOperator):
            def _get_retryable_error_patterns(self):
                return {
                    "custom error": ("Custom retryable error", 5),
                }
            
            def _get_non_retryable_error_patterns(self):
                return {
                    "custom failure": "Custom non-retryable error",
                }
        
        operator = CustomOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        
        # Test custom retryable pattern
        instant_fail, reason, max_retries = operator._retry_handler("Custom error occurred")
        assert instant_fail is False
        assert "Custom retryable error" in reason
        assert max_retries == 5
        
        # Test custom non-retryable pattern
        instant_fail, reason, max_retries = operator._retry_handler("Custom failure happened")
        assert instant_fail is True
        assert "Custom non-retryable error" in reason
        assert max_retries == 0

