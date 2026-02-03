"""Tests for SQL sensor plugin."""

import os
import pytest
from datetime import timedelta
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Set Airflow environment before importing
os.environ.setdefault('AIRFLOW_HOME', str(Path(__file__).parent.parent))
os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:///airflow.db')

from airflow.utils.context import Context
from sensors.sql_sensor import SqlSensor


class TestSqlSensor:
    """Tests for SqlSensor."""

    def test_sensor_initialization(self):
        """Test basic sensor initialization."""
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT 1',
        )
        assert sensor.task_id == 'test_sql_sensor'
        assert sensor.sql == 'SELECT 1'
        assert sensor.conn_id is None
        assert sensor.save_result is False
        assert sensor.result_key == 'sql_result'

    def test_sensor_with_conn_id(self):
        """Test sensor with connection ID."""
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='my_db_connection',
        )
        assert sensor.conn_id == 'my_db_connection'

    def test_sensor_with_parameters(self):
        """Test sensor with SQL parameters."""
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table WHERE id = %s',
            parameters=[123],
        )
        assert sensor.parameters == [123]

    def test_sensor_save_result(self):
        """Test sensor with save_result enabled."""
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT value FROM config',
            save_result=True,
            result_key='my_result',
        )
        assert sensor.save_result is True
        assert sensor.result_key == 'my_result'

    def test_sensor_default_parameters(self):
        """Test default sensor parameters."""
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT 1',
        )
        assert sensor.poke_interval == 60
        assert sensor.timeout == 60 * 60 * 24 * 7  # 7 days
        assert sensor.mode == 'poke'
        assert sensor.soft_fail is False
        assert sensor.retries == 0
        assert sensor.retry_delay == timedelta(minutes=5)

    def test_sensor_custom_parameters(self):
        """Test custom sensor parameters."""
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT 1',
            poke_interval=120,
            timeout=3600,
            mode='reschedule',
            soft_fail=True,
            retries=3,
            retry_delay=timedelta(minutes=10),
        )
        assert sensor.poke_interval == 120
        assert sensor.timeout == 3600
        assert sensor.mode == 'reschedule'
        assert sensor.soft_fail is True
        assert sensor.retries == 3
        assert sensor.retry_delay == timedelta(minutes=10)

    def test_sensor_template_fields(self):
        """Test that sql and parameters are in template_fields."""
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT 1',
        )
        assert 'sql' in sensor.template_fields
        assert 'parameters' in sensor.template_fields

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_get_records_has_results(self, mock_base_hook):
        """Test poke method with get_records hook that returns results."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [('value1',), ('value2',)]
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is True
        mock_hook.get_records.assert_called_once_with(
            sql='SELECT * FROM table',
            parameters=None
        )

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_get_records_no_results(self, mock_base_hook):
        """Test poke method with get_records hook that returns no results."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table WHERE condition = %s',
            conn_id='test_conn',
            parameters=['value'],
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is False
        mock_hook.get_records.assert_called_once_with(
            sql='SELECT * FROM table WHERE condition = %s',
            parameters=['value']
        )

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_get_first_has_results(self, mock_base_hook):
        """Test poke method with get_first hook that returns results."""
        # Setup mock hook (no get_records, but has get_first)
        mock_hook = MagicMock()
        del mock_hook.get_records  # Remove get_records
        mock_hook.get_first.return_value = ('value1', 'value2')
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is True
        mock_hook.get_first.assert_called_once_with(
            sql='SELECT * FROM table',
            parameters=None
        )

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_get_first_no_results(self, mock_base_hook):
        """Test poke method with get_first hook that returns None."""
        # Setup mock hook
        mock_hook = MagicMock()
        del mock_hook.get_records
        mock_hook.get_first.return_value = None
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is False

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_run_method(self, mock_base_hook):
        """Test poke method with run method fallback."""
        # Setup mock hook (no get_records or get_first, but has run)
        mock_hook = MagicMock()
        del mock_hook.get_records
        del mock_hook.get_first
        mock_hook.run.return_value = [('value1',), ('value2',)]
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is True
        assert mock_hook.run.called
        # Check that run was called with sql, parameters, and handler
        call_args = mock_hook.run.call_args
        assert call_args[1]['sql'] == 'SELECT * FROM table'
        assert call_args[1]['parameters'] is None
        assert callable(call_args[1]['handler'])

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_manual_execution(self, mock_base_hook):
        """Test poke method with manual cursor execution fallback."""
        # Setup mock hook (only has get_conn)
        mock_hook = MagicMock()
        del mock_hook.get_records
        del mock_hook.get_first
        del mock_hook.run

        # Setup mock connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('value1',), ('value2',)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.get_conn.return_value = mock_conn

        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is True
        mock_cursor.execute.assert_called_once_with('SELECT * FROM table', None)
        mock_cursor.fetchall.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_saves_result_to_xcom(self, mock_base_hook):
        """Test that poke saves result to XCom when save_result is True."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [('value1',), ('value2',)]
        mock_base_hook.get_hook.return_value = mock_hook

        # Setup mock task instance
        mock_ti = MagicMock()
        context = {'ti': mock_ti}

        # Create sensor with save_result enabled
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
            save_result=True,
            result_key='my_result',
        )

        # Test poke
        result = sensor.poke(context)

        assert result is True
        mock_ti.xcom_push.assert_called_once_with(
            key='my_result',
            value=[('value1',), ('value2',)]
        )

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_does_not_save_result_when_disabled(self, mock_base_hook):
        """Test that poke does not save result when save_result is False."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [('value1',), ('value2',)]
        mock_base_hook.get_hook.return_value = mock_hook

        # Setup mock task instance
        mock_ti = MagicMock()
        context = {'ti': mock_ti}

        # Create sensor with save_result disabled
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
            save_result=False,
        )

        # Test poke
        result = sensor.poke(context)

        assert result is True
        mock_ti.xcom_push.assert_not_called()

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_callable_sql(self, mock_base_hook):
        """Test poke method with callable SQL query."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [('value',)]
        mock_base_hook.get_hook.return_value = mock_hook

        # Create callable SQL
        def sql_query(context):
            return f"SELECT * FROM table WHERE date = '{context['ds']}'"

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql=sql_query,
            conn_id='test_conn',
        )
        context = {'ti': MagicMock(), 'ds': '2024-01-01'}

        # Test poke
        result = sensor.poke(context)

        assert result is True
        mock_hook.get_records.assert_called_once_with(
            sql="SELECT * FROM table WHERE date = '2024-01-01'",
            parameters=None
        )

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_handles_exception_gracefully(self, mock_base_hook):
        """Test that poke handles exceptions gracefully."""
        # Setup mock hook to raise exception
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = Exception("Database connection failed")
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke - should return False when exception occurs
        # The exception is caught and logged, but we don't fail the sensor
        result = sensor.poke(context)

        assert result is False
        # Verify the hook was called (exception happened during execution)
        mock_hook.get_records.assert_called_once()

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_none_result(self, mock_base_hook):
        """Test poke method when hook returns None."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = None
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is False

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_empty_list_result(self, mock_base_hook):
        """Test poke method when hook returns empty list."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT * FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is False

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_single_value_result(self, mock_base_hook):
        """Test poke method with single value result (not a list)."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = 42
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT COUNT(*) FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is True

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_zero_result(self, mock_base_hook):
        """Test poke method when query returns 0 (should be False)."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = 0
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT COUNT(*) FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is False

    @patch('sensors.sql_sensor.BaseHook')
    def test_poke_with_empty_string_result(self, mock_base_hook):
        """Test poke method when query returns empty string (should be False)."""
        # Setup mock hook
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = ''
        mock_base_hook.get_hook.return_value = mock_hook

        # Create sensor and context
        sensor = SqlSensor(
            task_id='test_sql_sensor',
            sql='SELECT value FROM table',
            conn_id='test_conn',
        )
        context = {'ti': MagicMock()}

        # Test poke
        result = sensor.poke(context)

        assert result is False

