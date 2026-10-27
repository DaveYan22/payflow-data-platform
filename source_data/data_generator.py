import mysql.connector
from faker import Faker
import os
import random
import uuid
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Logins live in the project's .env file (never committed)
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

fake = Faker()

db_config = {
    "host": os.getenv("MYSQL_LOCAL_HOST", "localhost"),
    "user": os.environ["MYSQL_ADMIN_USER"],
    "password": os.environ["MYSQL_ADMIN_PASSWORD"],
    "database": os.environ["MYSQL_DATABASE"],
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# The full load is already in MySQL; each run only adds 3-5% of the rows already in a table
GROWTH_MIN, GROWTH_MAX = 0.03, 0.05
now = datetime.now().replace(microsecond=0)


def batch_size(table):
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    existing = cursor.fetchone()[0]
    if existing == 0:
        raise SystemExit(f"{table} is empty: load the initial full data set first")
    return max(1, round(existing * random.uniform(GROWTH_MIN, GROWTH_MAX)))


def new_rows_start(table, column="created_at"):
    # New rows must be newer than the DAG's watermark column, or the incremental load skips them
    cursor.execute(f"SELECT MAX({column}) FROM {table}")
    latest = cursor.fetchone()[0]
    return min(latest + timedelta(seconds=1), now - timedelta(seconds=1))


print("Starting realistic data generation...")

# ---------------------------------------------------------
# 1. GENERATE FX RATES (~90 rows: 30 days x 3 currencies)
# ---------------------------------------------------------
print("Populating raw_fx_rates...")
fx_sql = """
INSERT INTO raw_fx_rates (rate_date, target_currency, exchange_rate)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE exchange_rate = VALUES(exchange_rate);
"""
currencies = ["EUR", "GBP", "CAD"]
base_rates = {"EUR": 0.92, "GBP": 0.79, "CAD": 1.35}

today = datetime.now().date()
fx_records = []
for day_offset in range(30):
    rate_date = today - timedelta(days=day_offset)
    for curr in currencies:
        # Add small random fluctuation around the base rate
        fluctuation = random.uniform(-0.02, 0.02)
        rate = round(base_rates[curr] + fluctuation, 6)
        fx_records.append((rate_date, curr, rate))

cursor.executemany(fx_sql, fx_records)
conn.commit()

# ---------------------------------------------------------
# 2. GENERATE CUSTOMERS (+3-5%)
# ---------------------------------------------------------
print("Populating raw_customers...")
num_customers = batch_size("raw_customers")
customers_start = new_rows_start("raw_customers", "updated_at")
cust_sql = """
INSERT INTO raw_customers (customer_id, full_name, email, kyc_status, country_code, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
country_codes = ["USA", "GBR", "DEU", "FRA", "CAN", "AUS", "SGP", "NLD"]
kyc_options = ["Verified", "Pending", "Rejected"]
kyc_weights = [0.80, 0.15, 0.05]

customers = []
customer_ids = []
for _ in range(num_customers):
    c_id = f"cust_{uuid.uuid4().hex[:10]}"
    customer_ids.append(c_id)
    created_at = fake.date_time_between(start_date=customers_start, end_date=now)
    updated_at = min(created_at + timedelta(days=random.randint(0, 15)), now)
    customers.append((
        c_id,
        fake.name(),
        fake.unique.email(),
        random.choices(kyc_options, weights=kyc_weights)[0],
        random.choice(country_codes),
        created_at,
        updated_at
    ))

cursor.executemany(cust_sql, customers)
conn.commit()

# ---------------------------------------------------------
# 3. GENERATE ACCOUNTS (1 to 3 per new customer)
# ---------------------------------------------------------
print("Populating raw_accounts...")
acc_sql = """
INSERT INTO raw_accounts (account_id, customer_id, currency_code, account_status, balance, updated_at)
VALUES (%s, %s, %s, %s, %s, %s)
"""
accounts = []
account_ids = []
account_currency_map = {}
accounts_start = new_rows_start("raw_accounts", "updated_at")

for c_id in customer_ids:
    # 1 to 3 accounts per customer
    num_accounts = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
    for _ in range(num_accounts):
        a_id = f"acc_{uuid.uuid4().hex[:10]}"
        account_ids.append(a_id)
        currency = random.choice(["USD", "EUR", "GBP"])
        account_currency_map[a_id] = currency
        balance = round(random.uniform(50.0, 12000.0), 2)
        status = random.choices(["Active", "Suspended", "Closed"], weights=[0.90, 0.07, 0.03])[0]
        updated_at = fake.date_time_between(start_date=accounts_start, end_date=now)
        
        accounts.append((a_id, c_id, currency, status, balance, updated_at))

cursor.executemany(acc_sql, accounts)
conn.commit()

# New transactions and events spread over all customers/accounts, not only the new ones
cursor.execute("SELECT account_id, currency_code FROM raw_accounts")
account_currency_map = dict(cursor.fetchall())
account_ids = list(account_currency_map)
cursor.execute("SELECT customer_id FROM raw_customers")
customer_ids = [row[0] for row in cursor.fetchall()]

# ---------------------------------------------------------
# 4. GENERATE TRANSACTIONS (+3-5%)
# ---------------------------------------------------------
print("Populating raw_transactions...")
num_transactions = batch_size("raw_transactions")
transactions_start = new_rows_start("raw_transactions")
tx_sql = """
INSERT INTO raw_transactions (transaction_id, account_id, amount, currency_code, transaction_type, merchant_category, status, created_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
tx_types = ["Deposit", "Withdrawal", "Card Payment", "Transfer"]
categories = ["Retail", "Dining", "Groceries", "Entertainment", "Utilities", "Travel", "Software"]
statuses = ["Completed", "Pending", "Failed"]
status_weights = [0.92, 0.05, 0.03]

transactions = []
for _ in range(num_transactions):
    t_id = f"tx_{uuid.uuid4().hex[:12]}"
    a_id = random.choice(account_ids)
    curr = account_currency_map[a_id]
    amount = round(random.uniform(2.5, 850.0), 2)
    t_type = random.choice(tx_types)
    cat = random.choice(categories) if t_type == "Card Payment" else "N/A"
    status = random.choices(statuses, weights=status_weights)[0]
    created_at = fake.date_time_between(start_date=transactions_start, end_date=now)

    transactions.append((t_id, a_id, amount, curr, t_type, cat, status, created_at))

cursor.executemany(tx_sql, transactions)
conn.commit()

# ---------------------------------------------------------
# 5. GENERATE APP EVENTS (+3-5%)
# ---------------------------------------------------------
print("Populating raw_app_events...")
num_events = batch_size("raw_app_events")
events_start = new_rows_start("raw_app_events")
event_sql = """
INSERT INTO raw_app_events (event_id, customer_id, event_type, event_payload, client_ip, created_at)
VALUES (%s, %s, %s, %s, %s, %s)
"""
event_types = ["login", "view_balance", "transfer_initiated", "failed_pin_attempt", "card_frozen"]
devices = ["iOS", "Android", "Web"]

app_events = []
for _ in range(num_events):
    e_id = f"evt_{uuid.uuid4().hex[:12]}"
    c_id = random.choice(customer_ids)
    e_type = random.choice(event_types)
    
    payload = {
        "device": random.choice(devices),
        "app_version": f"v{random.choice(['2.1.0', '2.2.1', '3.0.0'])}",
        "session_duration_sec": random.randint(5, 600),
        "flagged_for_review": True if e_type == "failed_pin_attempt" and random.random() < 0.3 else False
    }

    client_ip = fake.ipv4()
    created_at = fake.date_time_between(start_date=events_start, end_date=now)

    app_events.append((e_id, c_id, e_type, json.dumps(payload), client_ip, created_at))

cursor.executemany(event_sql, app_events)
conn.commit()

cursor.close()
conn.close()

print("Mock data generation finished successfully:")
print(f" - raw_fx_rates:     {len(fx_records)} rows")
print(f" - raw_customers:    {len(customers)} rows")
print(f" - raw_accounts:     {len(accounts)} rows")
print(f" - raw_transactions: {len(transactions)} rows")
print(f" - raw_app_events:   {len(app_events)} rows")