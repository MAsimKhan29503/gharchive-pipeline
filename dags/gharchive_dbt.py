from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

RAW_EVENTS = Dataset("postgres://warehouse-db/raw.gh_events")

DBT = "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt"


@dag(
    dag_id="gharchive_dbt",
    start_date=datetime(2026, 10, 1),
    schedule=[RAW_EVENTS],        # run whenever the ingest DAG updates raw.gh_events
    catchup=False,
    max_active_runs=1,            # never run two dbt jobs at once
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["gharchive", "dbt"],
)
def gharchive_dbt():
    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness",
    )
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build",
    )
    snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT} snapshot",
    )

    source_freshness >> build >> snapshot


gharchive_dbt()