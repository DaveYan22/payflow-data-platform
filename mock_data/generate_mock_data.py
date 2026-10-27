"""Create the core_banking_prod schema in MySQL and fill it with fake data.

Run from the host (MySQL is exposed on localhost:3306):
    python mock_data/generate_mock_data.py --customers 100 --transactions 2000
"""
import argparse
import json
import os
import random
import uuid
from datetime import date, datetime, timedelta

import mysql.connector
from dotenv import load_dotenv
from faker import Faker

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

fake = Faker()

# ISO-3 codes, matching source_data/country_risk_tiers.xlsx
COUNTRIES = ["USA", "GBR", "DEU", "FRA", "CAN", "AUS", "SGP", "NLD"]
CURRENCIES = ["USD", "EUR", "GBP", "PLN", "UAH", "GEL", "AMD", "TRY", "AED", "NGN"]
# Approximate units of currency per 1 USD; daily rates drift around these.
BASE_FX = {"USD": 1.0, "EUR": 0.92, "GBP": 0.79, "PLN": 3.98, "UAH": 41.2, "GEL": 2.72,
           "AMD": 387.0, "TRY": 34.3, "AED": 3.67, "NGN": 1550.0}
KYC_STATUSES = ["verified", "verified", "verified", "pending", "rejected"]
ACCOUNT_STATUSES = ["active", "active", "active", "frozen", "closed"]
TRANSACTION_TYPES = ["deposit", "withdrawal", "transfer_in", "transfer_out", "card_payment", "fee"]
MERCHANT_CATEGORIES = ["groceries", "travel", "utilities", "entertainment", "electronics", "crypto", "gambling", None]
TRANSACTION_STATUSES = ["completed", "completed", "completed", "completed", "pending", "failed", "reversed"]
EVENT_TYPES = ["login", "logout", "page_view", "transfer_initiated", "card_added", "password_reset"]

DDL = [
    """
    CREATE TABLE IF NOT EXISTS raw_customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        full_name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        kyc_status VARCHAR(50) NOT NULL,
        country_code VARCHAR(3) NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_customers_updated_at (updated_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_accounts (
        account_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50) NOT NULL,
        currency_code VARCHAR(3) NOT NULL,
        account_status VARCHAR(50) NOT NULL,
        balance DECIMAL(15,2) NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_accounts_updated_at (updated_at),
        FOREIGN KEY (customer_id) REFERENCES raw_customers (customer_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_transactions (
        transaction_id VARCHAR(50) PRIMARY KEY,
        account_id VARCHAR(50) NOT NULL,
        amount DECIMAL(15,2) NOT NULL,
        currency_code VARCHAR(3) NOT NULL,
        transaction_type VARCHAR(50) NOT NULL,
        merchant_category VARCHAR(100),
        status VARCHAR(50) NOT NULL,
        created_at DATETIME NOT NULL,
        INDEX idx_transactions_created_at (created_at),
        FOREIGN KEY (account_id) REFERENCES raw_accounts (account_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_app_events (
        event_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50) NOT NULL,
        event_type VARCHAR(100) NOT NULL,
        event_payload JSON,
        client_ip VARCHAR(45),
        created_at DATETIME NOT NULL,
        INDEX idx_app_events_created_at (created_at),
        FOREIGN KEY (customer_id) REFERENCES raw_customers (customer_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_fx_rates (
        rate_date DATE NOT NULL,
        target_currency VARCHAR(3) NOT NULL,
        exchange_rate DECIMAL(10,4) NOT NULL,
        PRIMARY KEY (rate_date, target_currency)
    )
    """,
]


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_LOCAL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.environ["MYSQL_ADMIN_USER"],
        password=os.environ["MYSQL_ADMIN_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
    )


def random_ts(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def build_customers(n, start, end):
    rows = []
    for _ in range(n):
        created = random_ts(start, end)
        rows.append((
            str(uuid.uuid4()), fake.name(), fake.unique.email(), random.choice(KYC_STATUSES),
            random.choice(COUNTRIES), created, random_ts(created, end),
        ))
    return rows


def build_accounts(customers, end):
    rows = []
    for customer_id, *_, created, _updated in customers:
        for _ in range(random.randint(1, 3)):
            rows.append((
                str(uuid.uuid4()), customer_id, random.choice(CURRENCIES), random.choice(ACCOUNT_STATUSES),
                round(random.uniform(0, 50_000), 2), random_ts(created, end),
            ))
    return rows


def build_transactions(n, accounts, start, end):
    rows = []
    for _ in range(n):
        account_id, _customer_id, currency, *_ = random.choice(accounts)
        tx_type = random.choice(TRANSACTION_TYPES)
        amount = round(random.uniform(1, 5_000), 2)
        if tx_type in ("withdrawal", "transfer_out", "card_payment", "fee"):
            amount = -amount
        merchant = random.choice(MERCHANT_CATEGORIES) if tx_type == "card_payment" else None
        rows.append((
            str(uuid.uuid4()), account_id, amount, currency, tx_type, merchant,
            random.choice(TRANSACTION_STATUSES), random_ts(start, end),
        ))
    return rows


def build_event_payload(event_type):
    if event_type in ("login", "logout"):
        payload = {"device": random.choice(["ios", "android", "web"]), "app_version": fake.numerify("#.#.#")}
    elif event_type == "page_view":
        payload = {"page": random.choice(["/home", "/accounts", "/cards", "/transfers", "/settings"]),
                   "duration_ms": random.randint(200, 60_000)}
    elif event_type == "transfer_initiated":
        payload = {"amount": round(random.uniform(1, 5_000), 2), "currency": random.choice(CURRENCIES),
                   "beneficiary_country": random.choice(COUNTRIES)}
    elif event_type == "card_added":
        payload = {"card_brand": random.choice(["visa", "mastercard", "amex"]), "is_virtual": random.random() < 0.4}
    else:
        payload = {"channel": random.choice(["email", "sms"]), "success": random.random() < 0.9}
    return json.dumps(payload)


def build_app_events(n, customers, start, end):
    rows = []
    for _ in range(n):
        event_type = random.choice(EVENT_TYPES)
        rows.append((
            str(uuid.uuid4()), random.choice(customers)[0], event_type, build_event_payload(event_type),
            fake.ipv4_public() if random.random() < 0.9 else fake.ipv6(), random_ts(start, end),
        ))
    return rows


def build_fx_rates(start_date: date, end_date: date):
    rows = []
    rates = dict(BASE_FX)
    day = start_date
    while day <= end_date:
        for currency in CURRENCIES:
            if currency != "USD":
                rates[currency] *= random.uniform(0.995, 1.005)
            rows.append((day, currency, round(rates[currency], 4)))
        day += timedelta(days=1)
    return rows


def insert(cursor, table, columns, rows, batch_size=5_000):
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    for i in range(0, len(rows), batch_size):
        cursor.executemany(sql, rows[i:i + batch_size])
    print(f"{table}: {len(rows)} rows")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--transactions", type=int, default=20_000)
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--days", type=int, default=365, help="History length in days")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    Faker.seed(args.seed)

    end = datetime.now().replace(microsecond=0)
    start = end - timedelta(days=args.days)

    customers = build_customers(args.customers, start, end)
    accounts = build_accounts(customers, end)
    transactions = build_transactions(args.transactions, accounts, start, end)
    app_events = build_app_events(args.events, customers, start, end)
    fx_rates = build_fx_rates(start.date(), end.date())

    conn = get_connection()
    cursor = conn.cursor()
    for statement in DDL:
        cursor.execute(statement)

    insert(cursor, "raw_customers",
           ["customer_id", "full_name", "email", "kyc_status", "country_code", "created_at", "updated_at"], customers)
    insert(cursor, "raw_accounts",
           ["account_id", "customer_id", "currency_code", "account_status", "balance", "updated_at"], accounts)
    insert(cursor, "raw_transactions",
           ["transaction_id", "account_id", "amount", "currency_code", "transaction_type",
            "merchant_category", "status", "created_at"], transactions)
    insert(cursor, "raw_app_events",
           ["event_id", "customer_id", "event_type", "event_payload", "client_ip", "created_at"], app_events)
    insert(cursor, "raw_fx_rates", ["rate_date", "target_currency", "exchange_rate"], fx_rates)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
