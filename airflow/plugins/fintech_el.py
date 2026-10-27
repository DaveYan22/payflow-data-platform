"""Shared extract/load helpers for the MySQL -> ClickHouse bronze DAGs."""
import os

import clickhouse_connect
import mysql.connector

BRONZE_DB = os.getenv("CLICKHOUSE_BRONZE_DB", "bronze")
CHUNK_SIZE = 10_000

# Bronze landing tables: raw copies plus load metadata, append-only.
BRONZE_DDL = {
    "raw_customers": """
        customer_id String, full_name String, email String, kyc_status String,
        country_code LowCardinality(String), created_at DateTime('UTC'), updated_at DateTime('UTC')
    """,
    "raw_accounts": """
        account_id String, customer_id String, currency_code LowCardinality(String),
        account_status LowCardinality(String), balance Decimal(15, 2), updated_at DateTime('UTC')
    """,
    "raw_transactions": """
        transaction_id String, account_id String, amount Decimal(15, 2), currency_code LowCardinality(String),
        transaction_type LowCardinality(String), merchant_category Nullable(String),
        status LowCardinality(String), created_at DateTime('UTC')
    """,
    "raw_app_events": """
        event_id String, customer_id String, event_type LowCardinality(String), event_payload String,
        client_ip Nullable(String), created_at DateTime('UTC')
    """,
    "raw_fx_rates": """
        rate_date Date, target_currency LowCardinality(String), exchange_rate Decimal(10, 4)
    """,
    "risk_tiers": """
        country_code String, region String, compliance_risk_level String
    """,
}

ORDER_BY = {
    "raw_customers": "(customer_id, _loaded_at)",
    "raw_accounts": "(account_id, _loaded_at)",
    "raw_transactions": "(created_at, transaction_id)",
    "raw_app_events": "(created_at, event_id)",
    "raw_fx_rates": "(rate_date, target_currency, _loaded_at)",
    "risk_tiers": "(country_code, _loaded_at)",
}


def mysql_conn():
    return mysql.connector.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ["MYSQL_PORT"]),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
    )


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_HTTP_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )


def ensure_bronze_table(client, table):
    client.command(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_DB}.{table} (
            {BRONZE_DDL[table]},
            _loaded_at DateTime('UTC') DEFAULT now('UTC'),
            _batch_id String
        )
        ENGINE = MergeTree
        ORDER BY {ORDER_BY[table]}
    """)


def check_source():
    conn = mysql_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchall()
    conn.close()
    clickhouse_client().command("SELECT 1")


def copy_query_to_bronze(table, query, params, batch_id, replace=False):
    """Stream a MySQL query into a bronze table in chunks. Returns the row count.

    replace=True empties the bronze table first, for sources without a change-tracking column.
    """
    client = clickhouse_client()
    ensure_bronze_table(client, table)
    if replace:
        client.command(f"TRUNCATE TABLE {BRONZE_DB}.{table}")

    conn = mysql_conn()
    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [c[0] for c in cursor.description] + ["_batch_id"]

    total = 0
    while True:
        rows = cursor.fetchmany(CHUNK_SIZE)
        if not rows:
            break
        client.insert(f"{BRONZE_DB}.{table}", [list(r) + [batch_id] for r in rows], column_names=columns)
        total += len(rows)

    cursor.close()
    conn.close()
    return total
