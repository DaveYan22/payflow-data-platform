"""Hourly incremental load of MySQL tables into ClickHouse bronze.

Each table has a watermark column: the max value already in bronze. Only rows newer
than it are pulled, so reruns and missed runs catch up without duplicating rows.
Customers and accounts use updated_at, so a changed row lands again as a new version.
"""
from datetime import datetime

import pendulum
from airflow.decorators import dag, task

from fintech_el import BRONZE_DB, check_source, clickhouse_client, copy_query_to_bronze, ensure_bronze_table

WATERMARK_COLUMNS = {
    "raw_customers": "updated_at",
    "raw_accounts": "updated_at",
    "raw_transactions": "created_at",
    "raw_app_events": "created_at",
}
EPOCH = datetime(1970, 1, 1)


@dag(
    dag_id="extract_incremental_loads",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["el", "bronze", "incremental"],
)
def extract_incremental_loads():
    @task
    def check_source_task():
        check_source()

    @task
    def load_delta(table: str, run_id: str = None):
        column = WATERMARK_COLUMNS[table]
        client = clickhouse_client()
        ensure_bronze_table(client, table)
        # query() returns typed values; command() would return the timestamp as a string
        watermark = client.query(f"SELECT max({column}) FROM {BRONZE_DB}.{table}").result_rows[0][0]
        watermark = max(watermark.replace(tzinfo=None), EPOCH)

        rows = copy_query_to_bronze(
            table,
            f"SELECT * FROM {table} WHERE {column} > %s ORDER BY {column}",
            (watermark,),
            run_id,
        )
        print(f"{table}: loaded {rows} rows with {column} newer than {watermark}")
        return rows

    check_source_task() >> load_delta.expand(table=list(WATERMARK_COLUMNS))


extract_incremental_loads()
