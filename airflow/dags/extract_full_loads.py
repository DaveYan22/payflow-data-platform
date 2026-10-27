"""Daily full reload of sources that have no change-tracking column.

raw_fx_rates (past rates can be rewritten in MySQL) and the country risk tiers Excel
file are small, so each run empties the bronze table and reloads it completely.
"""
import os

import pandas as pd
import pendulum
from airflow.decorators import dag, task

from fintech_el import BRONZE_DB, check_source, clickhouse_client, copy_query_to_bronze, ensure_bronze_table

FULL_LOAD_TABLES = ["raw_fx_rates"]


@dag(
    dag_id="extract_full_loads",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["el", "bronze", "full"],
)
def extract_full_loads():
    @task
    def check_source_task():
        check_source()

    @task
    def load_mysql_table(table: str, run_id: str = None):
        rows = copy_query_to_bronze(table, f"SELECT * FROM {table}", None, run_id, replace=True)
        print(f"{table}: loaded {rows} rows")
        return rows

    @task
    def load_risk_tiers(run_id: str = None):
        df = pd.read_excel(os.environ["RISK_TIERS_PATH"], dtype=str)
        df = df[["country_code", "region", "compliance_risk_level"]].dropna(subset=["country_code"])
        df["_batch_id"] = run_id

        client = clickhouse_client()
        ensure_bronze_table(client, "risk_tiers")
        client.command(f"TRUNCATE TABLE {BRONZE_DB}.risk_tiers")
        client.insert_df(f"{BRONZE_DB}.risk_tiers", df)
        print(f"risk_tiers: loaded {len(df)} rows")
        return len(df)

    checked = check_source_task()
    checked >> load_mysql_table.expand(table=FULL_LOAD_TABLES)
    checked >> load_risk_tiers()


extract_full_loads()
