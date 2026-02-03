#!/usr/bin/env python3
"""
DAG Template Generator

Creates a new DAG file from a template with proper tenant isolation and best practices.

Usage:
    python scripts/create_dag.py --name my_dag --tenant my_team
    python scripts/create_dag.py --name daily_etl --tenant data-engineering --schedule "0 2 * * *"
"""

import argparse
import os
from pathlib import Path
from datetime import datetime, timedelta
from string import Template


DAG_TEMPLATE = '''"""
${description}

This DAG is owned by ${tenant} team.
${additional_docs}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': '${owner}',
    'depends_on_past': False,
    'start_date': datetime(${start_date_year}, ${start_date_month}, ${start_date_day}),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': ${retries},
    'retry_delay': timedelta(minutes=${retry_delay_minutes}),
}

with DAG(
    '${dag_id}',
    default_args=default_args,
    description='${description}',
    schedule=${schedule},
    catchup=${catchup},
    max_active_runs=${max_active_runs},
    max_active_tasks=${max_active_tasks},
    tags=${tags},
    doc_md=None,  # Add markdown documentation here if needed
) as dag:

    # Example task
    example_task = BashOperator(
        task_id='example_task',
        bash_command='echo "Hello from ${dag_id}"',
    )

    # Add your tasks here
    # task1 >> task2 >> task3
'''


def create_dag_file(
    dag_name: str,
    tenant: str,
    description: str = None,
    schedule: str = None,
    owner: str = None,
    output_dir: Path = None,
):
    """Create a new DAG file from template."""
    
    # Validate inputs
    if not dag_name:
        raise ValueError("DAG name is required")
    if not tenant:
        raise ValueError("Tenant is required")
    
    # Generate DAG ID with tenant prefix
    dag_id = f"{tenant}_{dag_name}"
    
    # Set defaults
    description = description or f"DAG for {dag_name}"
    owner = owner or tenant
    
    # Format schedule
    if schedule is None:
        schedule = "None  # Manual trigger only"
    elif not (schedule.startswith("'") or schedule.startswith('"')):
        schedule = f"'{schedule}'"
    
    # Calculate start date (today)
    today = datetime.now()
    
    # Prepare template variables
    template_vars = {
        'dag_id': dag_id,
        'tenant': tenant,
        'owner': owner,
        'description': description,
        'schedule': schedule,
        'catchup': 'False',
        'max_active_runs': '1',
        'max_active_tasks': '3',
        'retries': '1',
        'retry_delay_minutes': '5',
        'start_date_year': today.year,
        'start_date_month': today.month,
        'start_date_day': today.day,
        'tags': f"['{tenant}', 'generated']",
        'additional_docs': '',
    }
    
    # Generate DAG content
    template = Template(DAG_TEMPLATE)
    dag_content = template.safe_substitute(**template_vars)
    
    # Determine output directory
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / 'dags' / tenant
    else:
        output_dir = Path(output_dir)
    
    # Create directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write DAG file
    dag_file = output_dir / f"{dag_name}.py"
    
    if dag_file.exists():
        response = input(f"DAG file {dag_file} already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
    
    dag_file.write_text(dag_content)
    
    print(f"✅ Created DAG: {dag_file}")
    print(f"   DAG ID: {dag_id}")
    print(f"   Tenant: {tenant}")
    print(f"   Schedule: {schedule}")
    print(f"\nNext steps:")
    print(f"1. Edit {dag_file} to add your tasks")
    print(f"2. Test your DAG: make test-dags")
    print(f"3. Check best practices: make test-dag-best-practices")


def main():
    parser = argparse.ArgumentParser(
        description='Create a new DAG file from template',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic DAG
  python scripts/create_dag.py --name my_dag --tenant my_team
  
  # DAG with schedule
  python scripts/create_dag.py --name daily_etl --tenant data-engineering --schedule "0 2 * * *"
  
  # DAG with description
  python scripts/create_dag.py --name weekly_report --tenant analytics \\
    --schedule "0 0 * * 1" --description "Generate weekly analytics report"
        """
    )
    
    parser.add_argument(
        '--name',
        required=True,
        help='DAG name (will be prefixed with tenant)',
    )
    parser.add_argument(
        '--tenant',
        required=True,
        help='Tenant/team identifier (e.g., data-engineering, analytics)',
    )
    parser.add_argument(
        '--description',
        help='DAG description (default: auto-generated)',
    )
    parser.add_argument(
        '--schedule',
        help='DAG schedule (cron expression, timedelta, or None for manual)',
    )
    parser.add_argument(
        '--owner',
        help='DAG owner (default: same as tenant)',
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        help='Output directory (default: dags/{tenant}/)',
    )
    
    args = parser.parse_args()
    
    try:
        create_dag_file(
            dag_name=args.name,
            tenant=args.tenant,
            description=args.description,
            schedule=args.schedule,
            owner=args.owner,
            output_dir=args.output_dir,
        )
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())

