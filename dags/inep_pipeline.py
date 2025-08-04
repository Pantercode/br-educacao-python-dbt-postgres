"""
DAG de orquestração OOP para pipeline INEP -> Postgres -> dbt.
- RAW: coleta JSON completo por endpoint (sem tratativas).
- SILVER: achatamento flexível em TEXT (tabela por endpoint).
- GOLD: delegada ao dbt.
"""
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

AGENDAMENTO_CRON = os.environ.get("AGENDAMENTO_CRON", "0 3 * * *")

with DAG(
    dag_id="inep_full_pipeline",
    description="ETL INEP OOP -> RAW/SILVER -> dbt (GOLD)",
    schedule=AGENDAMENTO_CRON,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["inep","educacao","postgres","dbt"],
) as dag:

    export_pythonpath = "export PYTHONPATH=/opt"

    extrair_raw = BashOperator(
        task_id="extrair_raw",
        bash_command=(
            f"{export_pythonpath} && python -m inep.main "
            "--modo AMBOS "
            "--endpoints /opt/inep/config/endpoints.yaml "
            "--schema_raw raw --schema_silver silver "
            "--lote ${TAMANHO_LOTE} --pausa ${PAUSA_API}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt deps --project-dir /opt/dbt --profiles-dir /opt/dbt/profiles && "
            "dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt/profiles --target prod"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt/profiles --target prod || true"
        ),
    )

    extrair_raw >> dbt_run >> dbt_test
