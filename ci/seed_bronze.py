"""Create the bronze tables and load a small, fixed sample data set for CI.

The tables are created with the same DDL Airflow uses (fintech_el.ensure_bronze_table), so CI cannot drift from
production. Every date is relative to today (UTC), because several models and tests depend on now(): daily balances
run up to today, and FX freshness is checked against the last 26 hours.

Each row is there to exercise a rule in silver or gold; the comments say which.

Usage: PYTHONPATH=airflow/plugins python ci/seed_bronze.py
Needs CLICKHOUSE_HOST, CLICKHOUSE_HTTP_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD.
"""
import json
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal

from fintech_el import BRONZE_DB, BRONZE_DDL, clickhouse_client, ensure_bronze_table

NOW = datetime.now(timezone.utc).replace(microsecond=0)
TODAY = NOW.date()
FX_DAYS = 40


def day(days_ago, hour=12):
    """A UTC timestamp `days_ago` days before today, at `hour`:00."""
    return datetime.combine(TODAY - timedelta(days=days_ago), time(hour), tzinfo=timezone.utc)


def loaded(days_ago):
    """_loaded_at: when Airflow would have loaded the row."""
    return day(days_ago, 23)


RISK_TIERS = [
    ("USA", "North America", "low"),
    ("CAN", "North America", "low"),
    ("GBR", "Europe", "low"),
    ("DEU", "Europe", "low"),
    ("FRA", "Europe", "medium"),
    ("NLD", "Europe", "low"),
    ("AUS", "Asia Pacific", "medium"),
    ("SGP", "Asia Pacific", "high"),
]

BASE_RATES = {"EUR": Decimal("0.920000"), "GBP": Decimal("0.790000"), "CAD": Decimal("1.350000")}

# (customer_id, full_name, email, kyc_status, country_code, created_at, updated_at, _loaded_at)
CUSTOMERS = [
    # Loaded twice with the same updated_at: silver must keep one copy
    ("cust_ci_0001", "Anna Smith", "anna@example.com", "Verified", "USA", day(60), day(60), loaded(60)),
    ("cust_ci_0001", "Anna Smith", "anna@example.com", "Verified", "USA", day(60), day(60), loaded(59)),
    # SCD2: Verified/NLD -> Pending/NLD -> (only updated_at changes, must be merged) -> Pending/DEU = 3 versions
    ("cust_ci_0002", "Ben Jones", "ben@example.com", "Verified", "NLD", day(50), day(50), loaded(50)),
    ("cust_ci_0002", "Ben Jones", "ben@example.com", "Pending", "NLD", day(50), day(10), loaded(10)),
    ("cust_ci_0002", "Ben Jones", "ben@example.com", "Pending", "NLD", day(50), day(9), loaded(9)),
    ("cust_ci_0002", "Ben Jones", "ben@example.com", "Pending", "DEU", day(50), day(5), loaded(5)),
    # Untidy source text: silver trims and fixes case
    ("cust_ci_0003", " Chloe Brown ", " Chloe@Example.com ", "pending", "gbr", day(45), day(45), loaded(45)),
    ("cust_ci_0004", "Dan Lee", "dan@example.com", "Rejected", "SGP", day(40), day(40), loaded(40)),
]

# (account_id, customer_id, currency_code, account_status, balance, updated_at, _loaded_at)
ACCOUNTS = [
    # SCD2 on accounts: Active 1000 -> Suspended 800
    ("acc_ci_0001", "cust_ci_0001", "USD", "Active", Decimal("1000.00"), day(30), loaded(30)),
    ("acc_ci_0001", "cust_ci_0001", "USD", "Suspended", Decimal("800.00"), day(3), loaded(3)),
    ("acc_ci_0002", "cust_ci_0002", "EUR", "Active", Decimal("500.00"), day(25), loaded(25)),
    # First version after its first transaction: ReportedBalance stays NULL for the early days
    ("acc_ci_0003", "cust_ci_0003", "GBP", "active", Decimal("2000.00"), day(20), loaded(20)),
    ("acc_ci_0004", "cust_ci_0004", "USD", "Closed", Decimal("0.00"), day(15), loaded(15)),
]

# (transaction_id, account_id, amount, currency_code, transaction_type, merchant_category, status,
#  created_at, updated_at, _loaded_at)
TRANSACTIONS = [
    ("tx_ci_0001", "acc_ci_0001", Decimal("300.00"), "USD", "Deposit", "N/A", "Completed", day(28), day(28), loaded(28)),
    ("tx_ci_0002", "acc_ci_0001", Decimal("45.50"), "USD", "Card Payment", "Groceries", "Completed", day(27), day(27), loaded(27)),
    # Pending -> Completed: two bronze rows, silver keeps the latest
    ("tx_ci_0003", "acc_ci_0001", Decimal("120.00"), "USD", "Withdrawal", "N/A", "Pending", day(26), day(26), loaded(26)),
    ("tx_ci_0003", "acc_ci_0001", Decimal("120.00"), "USD", "Withdrawal", "N/A", "Completed", day(26), day(24), loaded(24)),
    # Transfer: never in cash flow
    ("tx_ci_0004", "acc_ci_0001", Decimal("75.00"), "USD", "Transfer", "N/A", "Completed", day(20), day(20), loaded(20)),
    # Failed and Pending: kept, but not in cash flow
    ("tx_ci_0005", "acc_ci_0002", Decimal("60.00"), "EUR", "Card Payment", "Dining", "Failed", day(18), day(18), loaded(18)),
    ("tx_ci_0006", "acc_ci_0002", Decimal("250.00"), "EUR", "Deposit", "N/A", "Pending", day(2), day(2), loaded(2)),
    ("tx_ci_0007", "acc_ci_0002", Decimal("80.25"), "EUR", "Card Payment", "Travel", "Completed", day(12), day(12), loaded(12)),
    # Before acc_ci_0003's first version; empty merchant category becomes NULL
    ("tx_ci_0008", "acc_ci_0003", Decimal("500.00"), "GBP", "Deposit", "", "Completed", day(22), day(22), loaded(22)),
    ("tx_ci_0009", "acc_ci_0003", Decimal("99.99"), "GBP", "card_payment", "software", "Completed", day(8), day(8), loaded(8)),
    ("tx_ci_0010", "acc_ci_0004", Decimal("10.00"), "USD", "Withdrawal", "N/A", "Completed", day(14), day(14), loaded(14)),
    # Today: no FX rate for today yet, which the tests must tolerate
    ("tx_ci_0011", "acc_ci_0002", Decimal("15.00"), "EUR", "Card Payment", "Retail", "Completed", NOW, NOW, NOW),
]


def payload(device, flagged=False):
    return json.dumps(
        {"device": device, "app_version": "v3.0.0", "session_duration_sec": 120, "flagged_for_review": flagged}
    )


# (event_id, customer_id, event_type, event_payload, client_ip, created_at, _loaded_at)
APP_EVENTS = [
    ("evt_ci_0001", "cust_ci_0001", "login", payload("iOS"), "10.0.0.1", day(27), loaded(27)),
    ("evt_ci_0001", "cust_ci_0001", "login", payload("iOS"), "10.0.0.1", day(27), loaded(26)),
    ("evt_ci_0002", "cust_ci_0002", "failed_pin_attempt", payload("Android", True), "10.0.0.2", day(11), loaded(11)),
    ("evt_ci_0003", "cust_ci_0003", "view_balance", payload("Web"), None, day(6), loaded(6)),
    ("evt_ci_0004", "cust_ci_0004", "card_frozen", '{"device": ""}', "10.0.0.4", day(3), loaded(3)),
]


def fx_rows():
    # Every day up to yesterday: today's rate arrives with the 02:00 full load, after the 01:00 generator
    rows = []
    for days_ago in range(FX_DAYS, 0, -1):
        rate_date = TODAY - timedelta(days=days_ago)
        for index, (currency, base) in enumerate(BASE_RATES.items()):
            rate = base + Decimal(days_ago % 7 - 3) * Decimal("0.001") * (index + 1)
            rows.append((rate_date, currency, rate, NOW))
    return rows


def columns(table):
    """Column names from BRONZE_DDL, splitting only on commas outside parentheses (Decimal(15, 2))."""
    definitions, current, depth = [], "", 0
    for char in BRONZE_DDL[table]:
        if char == "," and depth == 0:
            definitions.append(current)
            current = ""
            continue
        depth += {"(": 1, ")": -1}.get(char, 0)
        current += char
    definitions.append(current)
    return [d.split()[0] for d in definitions if d.strip()] + ["_loaded_at", "_batch_id"]


def insert(client, table, rows):
    ensure_bronze_table(client, table)
    client.command(f"TRUNCATE TABLE {BRONZE_DB}.{table}")
    client.insert(f"{BRONZE_DB}.{table}", [list(row) + ["ci_seed"] for row in rows], column_names=columns(table))
    print(f"{BRONZE_DB}.{table}: {len(rows)} rows")


def main():
    client = clickhouse_client()
    insert(client, "risk_tiers", [row + (NOW,) for row in RISK_TIERS])
    insert(client, "raw_fx_rates", fx_rows())
    insert(client, "raw_customers", CUSTOMERS)
    insert(client, "raw_accounts", ACCOUNTS)
    insert(client, "raw_transactions", TRANSACTIONS)
    insert(client, "raw_app_events", APP_EVENTS)


if __name__ == "__main__":
    main()
