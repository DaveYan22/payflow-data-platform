"""Hourly incremental load of MySQL tables into ClickHouse bronze.

Each table has a watermark column: the max value already in bronze. Rows newer than
the watermark minus a short lookback are pulled, so rows committed late (with a
timestamp just before the watermark) are not missed. Rows from the lookback window
that bronze already has are skipped, so reruns never duplicate rows.
Customers, accounts and transactions use updated_at, so a changed row lands again
as a new version.
"""
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task

from fintech_el import BRONZE_DB, check_source, clickhouse_client, copy_query_to_bronze, ensure_bronze_table

# table: (watermark column, primary key)
INCREMENTAL_TABLES = {
    "raw_customers": ("updated_at", "customer_id"),
    "raw_accounts": ("updated_at", "account_id"),
    "raw_transactions": ("updated_at", "transaction_id"),
    "raw_app_events": ("created_at", "event_id"),
}
LOOKBACK = timedelta(minutes=10)
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
        column, key = INCREMENTAL_TABLES[table]
        client = clickhouse_client()
        ensure_bronze_table(client, table)
        # query() returns typed values; command() would return the timestamp as a string
        watermark = client.query(f"SELECT max({column}) FROM {BRONZE_DB}.{table}").result_rows[0][0]
        watermark = max(watermark.replace(tzinfo=None), EPOCH)
        since = max(watermark - LOOKBACK, EPOCH)

        already_loaded = {
            (row_key, row_ts.replace(tzinfo=None))
            for row_key, row_ts in client.query(
                f"SELECT {key}, {column} FROM {BRONZE_DB}.{table} WHERE {column} > %(since)s",
                parameters={"since": since},
            ).result_rows
        }

        rows = copy_query_to_bronze(
            table,
            f"SELECT * FROM {table} WHERE {column} > %s ORDER BY {column}",
            (since,),
            run_id,
            skip_keys=already_loaded,
            key_columns=[key, column],
        )
        print(f"{table}: loaded {rows} rows with {column} newer than {since} (watermark {watermark})")
        return rows

    check_source_task() >> load_delta.expand(table=list(INCREMENTAL_TABLES))


extract_incremental_loads()
