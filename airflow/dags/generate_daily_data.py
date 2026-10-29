"""Daily mock activity: grow the MySQL source by 3-5%, then load it into bronze.

Runs source_data/data_generator.py inside the Airflow container, then starts
extract_incremental_loads so the new rows reach ClickHouse the same night.
"""
import os
import subprocess
import sys

import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

GENERATOR_PATH = "/opt/airflow/source_data/data_generator.py"


@dag(
    dag_id="generate_daily_data",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["mock-data", "mysql"],
)
def generate_daily_data():
    @task
    def run_generator():
        # .env says localhost (right on Windows); from the container MySQL is the Windows host
        env = {**os.environ, "MYSQL_LOCAL_HOST": os.environ["MYSQL_HOST"]}
        result = subprocess.run(
            [sys.executable, GENERATOR_PATH], env=env, capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"data_generator.py failed with exit code {result.returncode}")

    load_new_rows = TriggerDagRunOperator(
        task_id="trigger_incremental_loads",
        trigger_dag_id="extract_incremental_loads",
    )

    run_generator() >> load_new_rows


generate_daily_data()
