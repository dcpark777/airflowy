"""Tests for Databricks operators plugin."""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest

# Set Airflow environment before importing
os.environ.setdefault('AIRFLOW_HOME', str(Path(__file__).parent.parent))
os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:///airflow.db')

from airflow.utils.context import Context
from operators.databricks_operators import (
    RETRY_BEHAVIOR_BACKOFF,
    RETRY_BEHAVIOR_IMMEDIATE,
    RETRY_BEHAVIOR_NO_RETRY,
    SparkDatabricksOperator,
)


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

    def test_fetch_logs_default_true(self):
        """Test that fetch_logs defaults to True."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        assert operator.fetch_logs is True

    def test_fetch_logs_explicit_false(self):
        """Test that fetch_logs can be set to False."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            num_workers=2,
            fetch_logs=False,
        )
        assert operator.fetch_logs is False

    def test_default_retry_backoff_and_max_total_retries(self):
        """Test default and custom retry backoff / max total retries via retry_config."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        assert operator.retry_config.enabled is True
        assert operator.retry_config.default_retry_backoff_seconds == 60
        assert operator.retry_config.max_total_retries == 5

        operator2 = SparkDatabricksOperator(
            task_id='test_spark_job2',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            default_retry_backoff_seconds=120,
            max_total_retries=3,
        )
        assert operator2.retry_config.default_retry_backoff_seconds == 120
        assert operator2.retry_config.max_total_retries == 3

    def test_smart_retry_disabled(self):
        """When smart_retry=False, retry_config.enabled is False."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            smart_retry=False,
        )
        assert operator.retry_config.enabled is False

    def test_retry_config_explicit(self):
        """When retry_config is provided, it is used and smart_retry is ignored."""
        from operators.databricks_retry import RetryConfig

        config = RetryConfig(enabled=False, max_total_retries=10)
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            smart_retry=True,
            retry_config=config,
        )
        assert operator.retry_config is config
        assert operator.retry_config.enabled is False
        assert operator.retry_config.max_total_retries == 10


class TestRunWithRetries:
    """Tests for _run_with_retries helper."""

    def test_run_with_retries_success_first_try(self):
        """_run_with_retries returns result when run_fn succeeds."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        result = operator._run_with_retries(
            context={'ti': MagicMock()},
            run_fn=lambda ctx: 42,
        )
        assert result == 42

    def test_run_with_retries_no_retry_raises(self):
        """_run_with_retries re-raises when failure is classified as no retry."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )

        def fail_syntax(_):
            raise ValueError("Syntax error in code")

        with pytest.raises(ValueError, match="Syntax error"):
            operator._run_with_retries(
                context={'ti': MagicMock()},
                run_fn=fail_syntax,
            )

    def test_run_with_retries_disabled_runs_once(self):
        """When smart_retry is False, _run_with_retries runs run_fn once and raises (no retry loop)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            smart_retry=False,
        )
        calls = []

        def fail_connection(_):
            calls.append(1)
            raise RuntimeError("Connection error occurred")

        with pytest.raises(RuntimeError, match="Connection error"):
            operator._run_with_retries(
                context={'ti': MagicMock()},
                run_fn=fail_connection,
            )
        assert len(calls) == 1

    def test_run_with_retries_retry_then_success(self):
        """_run_with_retries retries on retryable error and returns when run_fn succeeds."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            max_total_retries=3,
        )
        calls = []

        def fail_once_then_succeed(ctx):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("Connection error occurred")
            return "ok"

        result = operator._run_with_retries(
            context={'ti': MagicMock()},
            run_fn=fail_once_then_succeed,
        )
        assert result == "ok"
        assert len(calls) == 2

    def test_run_with_retries_max_total_retries_exceeded(self):
        """_run_with_retries re-raises after exceeding max_total_retries."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            max_total_retries=2,
        )
        calls = []

        def always_fail_retryable(_):
            calls.append(1)
            raise RuntimeError("Connection error occurred")

        with pytest.raises(RuntimeError, match="Connection error"):
            operator._run_with_retries(
                context={'ti': MagicMock()},
                run_fn=always_fail_retryable,
            )
        assert len(calls) == 3  # initial + 2 retries

    def test_run_with_retries_backoff_sleeps(self):
        """_run_with_retries sleeps when retry_delay_seconds > 0."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            default_retry_backoff_seconds=2,
            max_total_retries=3,
        )
        calls = []

        def fail_once_then_succeed(_):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("Rate limit exceeded")
            return "ok"

        with patch('operators.databricks_retry.time.sleep') as mock_sleep:
            result = operator._run_with_retries(
                context={'ti': MagicMock()},
                run_fn=fail_once_then_succeed,
            )
        assert result == "ok"
        mock_sleep.assert_called_once_with(2)


class TestJobRunOutputLogging:
    """Tests for _get_run_output and _log_job_run_output (job logs in Airflow task log)."""

    @patch('operators.databricks_operators.BaseHook.get_connection')
    def test_get_run_output_returns_none_when_no_host(self, mock_get_connection):
        """_get_run_output returns None when connection has no host."""
        conn = MagicMock()
        conn.host = None
        conn.password = 'token'
        conn.extra = None
        conn.extra_dejson = {}
        mock_get_connection.return_value = conn

        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        result = operator._get_run_output(12345)
        assert result is None

    @patch('operators.databricks_operators.BaseHook.get_connection')
    def test_get_run_output_returns_none_when_no_token(self, mock_get_connection):
        """_get_run_output returns None when connection has no token."""
        conn = MagicMock()
        conn.host = 'https://myworkspace.cloud.databricks.com'
        conn.password = None
        conn.extra = None
        conn.extra_dejson = {}
        mock_get_connection.return_value = conn

        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        result = operator._get_run_output(12345)
        assert result is None

    @patch('operators.databricks_operators.urllib.request.urlopen')
    @patch('operators.databricks_operators.BaseHook.get_connection')
    def test_get_run_output_returns_output_when_api_succeeds(
        self, mock_get_connection, mock_urlopen
    ):
        """_get_run_output returns dict with metadata and output when API succeeds."""
        conn = MagicMock()
        conn.host = 'https://myworkspace.cloud.databricks.com'
        conn.password = 'secret-token'
        conn.extra = None
        conn.extra_dejson = {}
        mock_get_connection.return_value = conn

        run_get_resp = MagicMock()
        run_get_resp.read.return_value = json.dumps({
            'run_id': 12345,
            'state': {'life_cycle_state': 'TERMINATED', 'result_state': 'SUCCESS'},
            'run_page_url': 'https://myworkspace.cloud.databricks.com/#job/1/run/12345',
        }).encode()
        run_get_resp.__enter__ = lambda self: self
        run_get_resp.__exit__ = lambda self, *a: None

        get_output_resp = MagicMock()
        get_output_resp.read.return_value = json.dumps({
            'notebook_output': {'result': 'ok'},
            'run_page_url': 'https://myworkspace.cloud.databricks.com/#job/1/run/12345',
        }).encode()
        get_output_resp.__enter__ = lambda self: self
        get_output_resp.__exit__ = lambda self, *a: None

        mock_urlopen.side_effect = [run_get_resp, get_output_resp]

        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        result = operator._get_run_output(12345)

        assert result is not None
        assert result.get('metadata', {}).get('run_id') == 12345
        assert result.get('notebook_output') == {'result': 'ok'}
        assert 'run_page_url' in result

    @patch('operators.databricks_operators.urllib.request.urlopen')
    @patch('operators.databricks_operators.BaseHook.get_connection')
    def test_get_run_output_includes_error_when_run_failed(
        self, mock_get_connection, mock_urlopen
    ):
        """_get_run_output includes error message and stack_trace when run failed."""
        conn = MagicMock()
        conn.host = 'https://myworkspace.cloud.databricks.com'
        conn.password = 'secret-token'
        conn.extra = None
        conn.extra_dejson = {}
        mock_get_connection.return_value = conn

        run_get_resp = MagicMock()
        run_get_resp.read.return_value = json.dumps({
            'run_id': 12345,
            'state': {'life_cycle_state': 'TERMINATED', 'result_state': 'FAILED'},
        }).encode()
        run_get_resp.__enter__ = lambda self: self
        run_get_resp.__exit__ = lambda self, *a: None

        get_output_resp = MagicMock()
        get_output_resp.read.return_value = json.dumps({
            'error': {
                'message': 'AnalysisException: table or view not found',
                'stack_trace': '  at line 42 ...',
            },
        }).encode()
        get_output_resp.__enter__ = lambda self: self
        get_output_resp.__exit__ = lambda self, *a: None

        mock_urlopen.side_effect = [run_get_resp, get_output_resp]

        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        result = operator._get_run_output(12345)

        assert result is not None
        assert result.get('error', {}).get('message') == 'AnalysisException: table or view not found'
        assert 'stack_trace' in result.get('error', {})

    def test_log_job_run_output_no_op_when_fetch_logs_false(self):
        """_log_job_run_output does not call _get_run_output when fetch_logs is False."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            fetch_logs=False,
        )
        with patch.object(operator, '_get_run_output') as mock_get:
            operator._log_job_run_output(999)
        mock_get.assert_not_called()

    @patch('operators.databricks_operators.urllib.request.urlopen')
    @patch('operators.databricks_operators.BaseHook.get_connection')
    def test_log_job_run_output_logs_to_airflow(
        self, mock_get_connection, mock_urlopen
    ):
        """_log_job_run_output logs metadata and notebook output to Airflow log."""
        conn = MagicMock()
        conn.host = 'https://myworkspace.cloud.databricks.com'
        conn.password = 'secret-token'
        conn.extra = None
        conn.extra_dejson = {}
        mock_get_connection.return_value = conn

        run_get_resp = MagicMock()
        run_get_resp.read.return_value = json.dumps({
            'run_id': 12345,
            'state': {'life_cycle_state': 'TERMINATED', 'result_state': 'SUCCESS'},
            'run_page_url': 'https://example.com/run/12345',
        }).encode()
        run_get_resp.__enter__ = lambda self: self
        run_get_resp.__exit__ = lambda self, *a: None

        get_output_resp = MagicMock()
        get_output_resp.read.return_value = json.dumps({
            'notebook_output': {'result': 'done'},
            'run_page_url': 'https://example.com/run/12345',
        }).encode()
        get_output_resp.__enter__ = lambda self: self
        get_output_resp.__exit__ = lambda self, *a: None

        mock_urlopen.side_effect = [run_get_resp, get_output_resp]

        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
            fetch_logs=True,
        )
        operator._log_job_run_output(12345)
        # Verify both API calls were made (runs/get and runs/get-output)
        assert mock_urlopen.call_count == 2

    def test_log_job_run_output_logs_exception_when_present(self):
        """_log_job_run_output logs error and stack_trace via log.error when output has error."""
        output = {
            'metadata': {'run_id': 99},
            'error': {
                'message': 'AnalysisException: table not found',
                'stack_trace': '  at org.apache.spark...',
            },
        }
        mock_log = MagicMock()
        with patch('airflow.models.baseoperator.BaseOperator.log', new_callable=PropertyMock) as mock_log_prop:
            mock_log_prop.return_value = mock_log
            operator = SparkDatabricksOperator(
                task_id='test_spark_job',
                databricks_conn_id='databricks_default',
                driver_node_type_id='i3.xlarge',
                worker_node_type_id='i3.xlarge',
                fetch_logs=True,
            )
            with patch.object(operator, '_get_run_output', return_value=output):
                operator._log_job_run_output(99)
        mock_log.error.assert_any_call("--- Databricks run error ---")
        mock_log.error.assert_any_call("Message: %s", "AnalysisException: table not found")
        mock_log.error.assert_any_call("Stack trace:\n%s", "  at org.apache.spark...")


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
        behavior, reason, max_retries, retry_delay = operator._retry_handler("")
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Empty error message" in reason
        assert max_retries == 3
        assert retry_delay == 0

    def test_retry_handler_none_message(self):
        """Test retry handler with None error message."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler(None)
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Empty error message" in reason
        assert max_retries == 3
        assert retry_delay == 0

    # Non-retryable error tests
    def test_retry_handler_syntax_error(self):
        """Test retry handler with syntax error (no retry)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Syntax error in code")
        assert behavior == RETRY_BEHAVIOR_NO_RETRY
        assert "Syntax error" in reason
        assert max_retries == 0
        assert retry_delay == 0

    def test_retry_handler_authentication_error(self):
        """Test retry handler with authentication error (no retry)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Authentication failed")
        assert behavior == RETRY_BEHAVIOR_NO_RETRY
        assert "Authentication" in reason
        assert max_retries == 0

    def test_retry_handler_file_not_found(self):
        """Test retry handler with file not found error (no retry)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("File not found: /path/to/file")
        assert behavior == RETRY_BEHAVIOR_NO_RETRY
        assert "File not found" in reason
        assert max_retries == 0

    def test_retry_handler_configuration_error(self):
        """Test retry handler with configuration error (no retry)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Invalid configuration")
        assert behavior == RETRY_BEHAVIOR_NO_RETRY
        assert "Invalid configuration" in reason or "configuration" in reason.lower()
        assert max_retries == 0

    def test_retry_handler_http_401(self):
        """Test retry handler with HTTP 401 error (no retry)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("HTTP 401 Unauthorized")
        assert behavior == RETRY_BEHAVIOR_NO_RETRY
        assert "401" in reason or "Unauthorized" in reason
        assert max_retries == 0

    # Retryable error tests - immediate (API/network)
    def test_retry_handler_connection_error(self):
        """Test retry handler with connection error (retry immediate)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Connection error occurred")
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Connection error" in reason
        assert max_retries == 3
        assert retry_delay == 0

    def test_retry_handler_timeout_error(self):
        """Test retry handler with timeout error (retry immediate)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Timeout error")
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Timeout" in reason
        assert max_retries == 3
        assert retry_delay == 0

    def test_retry_handler_connection_reset(self):
        """Test retry handler with connection reset (retry immediate)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Connection reset by peer")
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Connection reset" in reason
        assert max_retries == 3
        assert retry_delay == 0

    def test_retry_handler_service_unavailable(self):
        """Test retry handler with service unavailable (retry immediate)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Service unavailable")
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Service unavailable" in reason
        assert max_retries == 3
        assert retry_delay == 0

    def test_retry_handler_http_503(self):
        """Test retry handler with HTTP 503 (retry immediate)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("HTTP 503 Service Unavailable")
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Service unavailable" in reason or "503" in reason
        assert max_retries == 3
        assert retry_delay == 0

    # Retryable error tests - backoff (throttling, resources)
    def test_retry_handler_out_of_memory(self):
        """Test retry handler with out of memory (retry after backoff)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Out of memory error")
        assert behavior == RETRY_BEHAVIOR_BACKOFF
        assert "Out of memory" in reason
        assert max_retries == 2
        assert retry_delay == 60

    def test_retry_handler_cluster_terminated(self):
        """Test retry handler with cluster terminated (retry after backoff)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Cluster terminated unexpectedly")
        assert behavior == RETRY_BEHAVIOR_BACKOFF
        assert "Cluster terminated" in reason
        assert max_retries == 2
        assert retry_delay == 60

    def test_retry_handler_spot_instance(self):
        """Test retry handler with spot instance interruption (retry after backoff)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Spot instance interrupted")
        assert behavior == RETRY_BEHAVIOR_BACKOFF
        assert "Spot instance" in reason
        assert max_retries == 3
        assert retry_delay == 60

    def test_retry_handler_rate_limit(self):
        """Test retry handler with rate limit (retry after backoff)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Rate limit exceeded")
        assert behavior == RETRY_BEHAVIOR_BACKOFF
        assert "Rate limit" in reason
        assert max_retries == 3
        assert retry_delay == 60

    def test_retry_handler_lock_contention(self):
        """Test retry handler with lock contention (retry after backoff)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Lock contention detected")
        assert behavior == RETRY_BEHAVIOR_BACKOFF
        assert "Lock" in reason
        assert max_retries == 2
        assert retry_delay == 60

    def test_retry_handler_unknown_error(self):
        """Test retry handler with unknown error (defaults to retry with backoff)."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, reason, max_retries, retry_delay = operator._retry_handler("Some unknown error message")
        assert behavior == RETRY_BEHAVIOR_BACKOFF
        assert "Unknown error" in reason
        assert max_retries == 2
        assert retry_delay == 60

    def test_retry_handler_case_insensitive(self):
        """Test that retry handler is case-insensitive."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        behavior, _, max_retries, retry_delay = operator._retry_handler("CONNECTION ERROR")
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert max_retries == 3
        assert retry_delay == 0

        behavior, _, max_retries, _ = operator._retry_handler("Syntax Error In Code")
        assert behavior == RETRY_BEHAVIOR_NO_RETRY
        assert max_retries == 0

    def test_retry_handler_pattern_matching(self):
        """Test that retry handler matches patterns within error messages."""
        operator = SparkDatabricksOperator(
            task_id='test_spark_job',
            databricks_conn_id='databricks_default',
            driver_node_type_id='i3.xlarge',
            worker_node_type_id='i3.xlarge',
        )
        error_msg = "An error occurred: Connection reset by peer. Please try again."
        behavior, reason, max_retries, retry_delay = operator._retry_handler(error_msg)
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Connection reset" in reason
        assert max_retries == 3
        assert retry_delay == 0

    def test_retry_handler_custom_patterns_override(self):
        """Test that subclasses can override error patterns (reason, max_retries, delay)."""
        class CustomOperator(SparkDatabricksOperator):
            def _get_retryable_error_patterns(self):
                return {
                    "custom error": ("Custom retryable error", 5, 0),  # immediate
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

        behavior, reason, max_retries, retry_delay = operator._retry_handler("Custom error occurred")
        assert behavior == RETRY_BEHAVIOR_IMMEDIATE
        assert "Custom retryable error" in reason
        assert max_retries == 5
        assert retry_delay == 0

        behavior, reason, max_retries, retry_delay = operator._retry_handler("Custom failure happened")
        assert behavior == RETRY_BEHAVIOR_NO_RETRY
        assert "Custom non-retryable error" in reason
        assert max_retries == 0
        assert retry_delay == 0

