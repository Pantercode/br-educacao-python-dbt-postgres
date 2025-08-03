from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "dados",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PG_ENV = " ".join([
    f"PGHOST={os.getenv('POSTGRES_HOST')}",
    f"PGPORT={os.getenv('POSTGRES_PORT','5432')}",
    f"PGUSER={os.getenv('POSTGRES_USER')}",
    f"PGDATABASE={os.getenv('POSTGRES_DB')}",
    f"PGPASSWORD={os.getenv('POSTGRES_PASSWORD')}",
])

with DAG(
    dag_id="inep_full_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["inep"],
) as dag:

    extract_safe = BashOperator(
        task_id="extract_safe",
        bash_command=f'''
        cd /opt/airflow/inep && {PG_ENV} python inep_full_crawler.py --mode SAFE --sleep 0.3
        '''
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command='''
        cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod
        '''
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command='''
        cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod || true
        '''
    )

    extract_safe >> dbt_run >> dbt_test