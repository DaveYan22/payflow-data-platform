# Building a Fintech Data Platform from Scratch

**A hands-on course based on the PayFlow Data Platform project (26 Oct to 9 Nov 2026)**

This course walks through every step of building an end-to-end ELT data platform: a MySQL banking database, Airflow
ingestion into a ClickHouse warehouse, bronze, silver and gold layers modelled with dbt, automated data tests, and a
Superset dashboard. Each module explains what was built, why, how it works in code, and what went wrong along the way.

Numbers in this course are as of 9 Nov 2026 unless stated otherwise.

---

## Contents

| Module | Topic |
|---|---|
| 0 | [Course overview](#module-0-course-overview) |
| 1 | [The stack and environment](#module-1-the-stack-and-environment) |
| 2 | [The source system: MySQL and Excel](#module-2-the-source-system-mysql-and-excel) |
| 3 | [Data architecture: ELT and the medallion layers](#module-3-data-architecture-elt-and-the-medallion-layers) |
| 4 | [Ingestion with Airflow: incremental and full loads](#module-4-ingestion-with-airflow-incremental-and-full-loads) |
| 5 | [The bronze layer and change-tracking tests](#module-5-the-bronze-layer-and-change-tracking-tests) |
| 6 | [The silver layer with dbt and SCD2](#module-6-the-silver-layer-with-dbt-and-scd2) |
| 7 | [dbt materializations, model by model, and snapshots](#module-7-dbt-materializations-model-by-model-and-snapshots) |
| 8 | [The gold layer: star schemas](#module-8-the-gold-layer-star-schemas) |
| 9 | [Data quality: dbt tests](#module-9-data-quality-dbt-tests) |
| 10 | [Orchestrating dbt from Airflow](#module-10-orchestrating-dbt-from-airflow) |
| 11 | [The FX rate problem: a case study](#module-11-the-fx-rate-problem-a-case-study) |
| 12 | [Superset dashboard](#module-12-superset-dashboard) |
| 13 | [Git workflow and security](#module-13-git-workflow-and-security) |
| 14 | [Troubleshooting and lessons learned](#module-14-troubleshooting-and-lessons-learned) |
| 15 | [Operations cheat sheet](#module-15-operations-cheat-sheet) |
| 16 | [Known limits and next steps](#module-16-known-limits-and-next-steps) |
| A | [Glossary](#appendix-glossary) |

---

## Module 0: Course overview

### What you will build

```text
SOURCES                 INGESTION (EL)            WAREHOUSE (ClickHouse)                    BI
-------                 --------------            ----------------------                    --

Generator (01:00) --+
                    v
MySQL core_banking_prod ---> Airflow ------> BRONZE ------> SILVER ------> GOLD ------> Superset
  5 tables               incremental (hourly)  6 raw tables   8 dbt models   5 dbt models   PayFlow Overview
                         full load (02:00)     append-only    cleaned, SCD2  star schemas   11 charts
Excel risk tiers ------> (full load)                ^              ^              ^
                                                    |              +--- dbt build -+
                                                    |                  (after every incremental load)
                                                    +-- loaded by Python + clickhouse-connect
```

The original design is in `source_data/Data Architecture.jpg`.

### What the platform answers

| Business question | Where the answer comes from |
|---|---|
| How much money moves through the platform each day? | `gold.fact_transactions` → Daily volume chart |
| Is more money coming in or going out? | `gold.fact_transactions` → Net cash flow charts |
| Which regions and risk levels hold the most customers and balance? | `gold.dim_customers` + `gold.dim_accounts` → Risk profile charts |
| What is 1 EUR worth in USD today, and how has it moved? | `silver.fx_rates` → FX converter charts |
| What was a customer's KYC status on a past date? | `silver.customers_history` (SCD2) |

### Timeline

| Dates | Milestone |
|---|---|
| 26 Oct | Docker stack, MySQL source, first bronze loads |
| 27 to 29 Oct | Watermark bug fixed, secrets moved to `.env`, generator DAG, change-tracking tests, incremental hardening |
| 29 Oct | Silver layer: 8 dbt models, SCD2, 28 tests (PR #3) |
| 2 Nov | dbt runs from Airflow (PR #4), FX gap found and fixed |
| 2 Nov | Gold layer: 3 star schemas, tests sorted by layer, generator FX fix (PR #5) |
| 9 Nov | Superset dashboard, PDF snapshot (PR #6) |

---

## Module 1: The stack and environment

### Learning goals

- Know every component, its version and its port.
- Understand why MySQL runs outside Docker.
- Keep credentials out of code.

### Components

| Component | Version | Runs in | Port | Role |
|---|---|---|---|---|
| MySQL | 8 | Windows service `MySQL80` | 3306 | Source OLTP database `core_banking_prod` |
| ClickHouse | 25.8 | Container `clickhouse_dwh` | 8123 (HTTP), 9000 (native) | OLAP warehouse: bronze, silver, gold |
| Airflow | 2.10.3, Python 3.11 | Container `airflow` (standalone) | 8080 | Orchestration: loads and dbt |
| dbt Core | 1.11.12, dbt-clickhouse 1.10.1 | Container `dbt`, plus a venv inside `airflow` | none | Transformations and tests |
| Superset | 4.1.1 | Container `superset` | 8088 | Dashboards |

All containers are defined in `docker-compose.yml`. MySQL stays on the Windows host because it already existed there;
containers reach it at `host.docker.internal`.

> **Watch out:** other projects on the same machine run ClickHouse containers called `clickhouse-dbt`,
> `clickhouse-airbnb` and `clickhouse-airstats` on ports 8124 to 8126. They are not part of this project. This
> project's ClickHouse is `clickhouse_dwh` on port 8123.

### Key compose details

**ClickHouse** uses a named volume `clickhouse_data` and a health check on `/ping`. Every other service waits for it:

```yaml
depends_on:
  clickhouse:
    condition: service_healthy
```

**Airflow** runs in standalone mode (web server, scheduler and triggerer in one container, SQLite metadata database).
Extra Python packages are installed on each start:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "clickhouse-connect==0.8.11 mysql-connector-python==9.1.0 openpyxl==3.1.5 Faker==33.1.0 python-dotenv==1.0.1"
```

Its startup command also installs dbt in a separate virtual environment, only if it is missing (Module 10):

```bash
([ -x /home/airflow/dbt_venv/bin/dbt ] || (python -m venv /home/airflow/dbt_venv &&
  /home/airflow/dbt_venv/bin/pip install -q dbt-core==1.11.12 dbt-clickhouse==1.10.1)) &&
airflow db migrate && ... && airflow standalone
```

Project folders are mounted into Airflow, so code changes are live immediately:

| Host folder | Container path |
|---|---|
| `airflow/dags` | `/opt/airflow/dags` |
| `airflow/plugins` | `/opt/airflow/plugins` |
| `source_data` | `/opt/airflow/source_data` |
| `dbt_transform` | `/opt/airflow/dbt_transform` |

**dbt** has its own container that idles (`tail -f /dev/null`) so you can run commands in it.

**Superset** installs `clickhouse-connect` on start, then initialises its database and admin user.

### Credentials

All logins live in `.env`, which is git-ignored. `.env.example` is the committed template:

| Variable | Used by |
|---|---|
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD` | Airflow loads (`banking_user`) |
| `MYSQL_GENERATOR_USER`, `MYSQL_GENERATOR_PASSWORD` | Data generator (`generator_user`) |
| `MYSQL_ADMIN_USER`, `MYSQL_ADMIN_PASSWORD` | One-time setup script on Windows |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Airflow, dbt, Superset build script |
| `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` | Airflow web login |
| `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_USER`, `SUPERSET_ADMIN_PASSWORD` | Superset |
| `RISK_TIERS_PATH`, `DBT_PROJECT_DIR` | Paths inside the Airflow container |

Separate MySQL logins mean the pipeline never needs `root`.

### Key takeaways

- Pin every version: images, pip packages and dbt.
- Health checks plus `depends_on` give a safe start order.
- Mounted folders make development fast, but mean uncommitted changes are live in Airflow.

---

## Module 2: The source system: MySQL and Excel

### Learning goals

- Know the source schema and how tables relate.
- Understand how mock data is generated and why its timestamps matter.

### Entity relationship diagram

```text
+---------------------+          country_code matches   +-------------------------+
|    raw_customers    |  - - - - - - - - - - - - - - -> |  risk tiers (Excel)     |
|---------------------|          (not a database FK)    |-------------------------|
| PK customer_id      |                                 |    country_code         |
|    full_name        |                                 |    region               |
|    email (UNIQUE)   |                                 |    compliance_risk_level|
|    kyc_status       |                                 +-------------------------+
|    country_code     |
|    created_at       |
|    updated_at       |
+---------------------+
     | 1        | 1
     |          +---------------------------+
     | n                                    | n
+---------------------+          +---------------------+
|    raw_accounts     |          |   raw_app_events    |
|---------------------|          |---------------------|
| PK account_id       |          | PK event_id         |
| FK customer_id      |          | FK customer_id      |
|    currency_code    |          |    event_type       |
|    account_status   |          |    event_payload    |
|    balance          |          |    client_ip        |
|    updated_at       |          |    created_at       |
+---------------------+          +---------------------+
     | 1
     | n
+---------------------+          +---------------------+
|  raw_transactions   |          |    raw_fx_rates     |
|---------------------|          |---------------------|
| PK transaction_id   |          | PK rate_date        |
| FK account_id       |          | PK target_currency  |
|    amount           |          |    exchange_rate    |
|    currency_code    |          +---------------------+
|    transaction_type |          units of currency per 1 USD;
|    merchant_category|          USD itself is not stored;
|    status           |          joined to transactions on
|    created_at       |          date + currency (no FK)
|    updated_at       |
+---------------------+
```

All foreign keys use `ON DELETE CASCADE`. The full ERD is in `source_data/ERD_core_banking.pdf`.

### Table definitions (MySQL 8, `core_banking_prod`)

```sql
CREATE TABLE raw_customers (
  customer_id varchar(50) NOT NULL,
  full_name varchar(255) NOT NULL,
  email varchar(255) NOT NULL,
  kyc_status varchar(50) NOT NULL,
  country_code varchar(3) NOT NULL,
  created_at datetime DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_id),
  UNIQUE KEY email (email)
);

CREATE TABLE raw_accounts (
  account_id varchar(50) NOT NULL,
  customer_id varchar(50) NOT NULL,
  currency_code varchar(3) NOT NULL,
  account_status varchar(50) NOT NULL,
  balance decimal(15,2) DEFAULT '0.00',
  updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (account_id),
  FOREIGN KEY (customer_id) REFERENCES raw_customers (customer_id) ON DELETE CASCADE
);

CREATE TABLE raw_transactions (
  transaction_id varchar(50) NOT NULL,
  account_id varchar(50) NOT NULL,
  amount decimal(15,2) NOT NULL,
  currency_code varchar(3) NOT NULL,
  transaction_type varchar(50) NOT NULL,
  merchant_category varchar(100) DEFAULT NULL,
  status varchar(50) NOT NULL,
  created_at datetime DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (transaction_id),
  KEY idx_transactions_updated_at (updated_at),
  FOREIGN KEY (account_id) REFERENCES raw_accounts (account_id) ON DELETE CASCADE
);

CREATE TABLE raw_app_events (
  event_id varchar(50) NOT NULL,
  customer_id varchar(50) NOT NULL,
  event_type varchar(100) NOT NULL,
  event_payload json NOT NULL,
  client_ip varchar(45) DEFAULT NULL,
  created_at datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (event_id),
  FOREIGN KEY (customer_id) REFERENCES raw_customers (customer_id) ON DELETE CASCADE
);

CREATE TABLE raw_fx_rates (
  rate_date date NOT NULL,
  target_currency varchar(3) NOT NULL,
  exchange_rate decimal(10,6) NOT NULL,
  PRIMARY KEY (rate_date, target_currency)
);
```

`raw_transactions.updated_at` did not exist at first. It was added by `mysql/001_incremental_hardening.sql`
(Module 5).

### Row counts (9 Nov 2026)

| Table | Rows | Change tracking column |
|---|---|---|
| `raw_customers` | 133 | `updated_at` |
| `raw_accounts` | 221 | `updated_at` |
| `raw_transactions` | 2,624 | `updated_at` |
| `raw_app_events` | 5,284 | `created_at` only (events never change) |
| `raw_fx_rates` | 225 (75 days × 3 currencies) | none |

### Values in the data

| Column | Values |
|---|---|
| `kyc_status` | Verified (80%), Pending (15%), Rejected (5%) |
| `country_code` (ISO-3) | USA, GBR, DEU, FRA, CAN, AUS, SGP, NLD |
| `currency_code` (accounts) | USD, EUR, GBP |
| `account_status` | Active (90%), Suspended (7%), Closed (3%) |
| `transaction_type` | Deposit, Withdrawal, Card Payment, Transfer |
| `status` (transactions) | Completed (92%), Pending (5%), Failed (3%) |
| `merchant_category` | Retail, Dining, Groceries, Entertainment, Utilities, Travel, Software for Card Payment; `N/A` otherwise |
| `event_type` | login, view_balance, transfer_initiated, failed_pin_attempt, card_frozen |
| `event_payload` | JSON: `device`, `app_version`, `session_duration_sec`, `flagged_for_review` |
| FX currencies | EUR, GBP, CAD, quoted as units per 1 USD |

### Excel: country risk tiers

`source_data/country_risk_tiers.xlsx` is manually managed reference data with columns `country_code`, `region` and
`compliance_risk_level` (low, medium, high). It holds the same 8 ISO-3 countries as the generator.

### The data generator

| Script | When | What it does |
|---|---|---|
| `mock_data/generate_mock_data.py` | Once, on Windows | Creates the schema and the initial data set |
| `source_data/data_generator.py` | Daily at 01:00 UTC through Airflow, or by hand | Grows each table by 3 to 5% |

How `data_generator.py` behaves:

1. **FX rates:** fills any missing date in the last 30 days for EUR, GBP and CAD. Existing rates are never changed
   (`ON DUPLICATE KEY UPDATE exchange_rate = exchange_rate`).
2. **Customers:** adds 3 to 5% new customers.
3. **Accounts:** 1 to 3 accounts per new customer, in USD, EUR or GBP.
4. **Transactions:** adds 3 to 5%, spread over all accounts. The transaction currency is always the account's currency.
5. **App events:** adds 3 to 5%, spread over all customers.

Two rules make the generator safe for incremental loading:

```python
# Always UTC (stored as naive DATETIME), the same on Windows and in the Airflow container
now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

def new_rows_start(table, column="created_at"):
    # New rows must be newer than the DAG's watermark column, or the incremental load skips them
    cursor.execute(f"SELECT MAX({column}) FROM {table}")
    latest = cursor.fetchone()[0]
    return min(latest + timedelta(seconds=1), now - timedelta(seconds=1))
```

New transactions get `updated_at = now`, so they are always newer than the load watermark.

### The `generate_daily_data` DAG

```text
run_generator (PythonOperator, runs data_generator.py) --> trigger_incremental_loads (TriggerDagRunOperator)
```

Schedule `0 1 * * *`, `retries = 0`. It overrides `MYSQL_LOCAL_HOST` with `MYSQL_HOST`, because `localhost` inside
the container is not the Windows host.

### Key takeaways

- Know which column tracks change in every source table, before writing any load.
- Generated data must respect the same rules as real data: UTC, and new rows newer than the watermark.

---

## Module 3: Data architecture: ELT and the medallion layers

### Learning goals

- Understand ELT and the bronze, silver and gold layers.
- Know what belongs in each layer.

### ELT, not ETL

Data is **extracted** from MySQL and **loaded** raw into ClickHouse first; **transformation** happens afterwards,
inside the warehouse, with dbt. The benefit: silver and gold can always be rebuilt from bronze, and a transformation
bug never loses source data.

### The three layers

| Layer | Schema | Built by | Contents | Rules |
|---|---|---|---|---|
| Bronze | `bronze` | Airflow (Python) | Raw copies plus `_loaded_at` and `_batch_id` | Append-only, no transformation, keeps every loaded version |
| Silver | `silver` | dbt | Cleaned, deduplicated, typed data; SCD2 history | One row per real version or per record |
| Gold | `gold` | dbt | Business-ready star schemas in USD | Built for questions and dashboards |

### Why bronze is not a mirror of MySQL

MySQL overwrites a row when it changes. If a balance goes from 500 to 800, MySQL only has 800. Bronze is append-only,
so each loaded version is kept:

```text
MySQL:   1 row   (latest state only)
Bronze:  4 rows  (first load + 3 updates, each stored exactly as it came)
```

That history is what silver needs to build SCD2. So bronze row counts are higher than MySQL row counts, by design.
Compare **distinct IDs**, not total rows:

```sql
SELECT
  COUNT(*) AS TotalRows,
  COUNT(DISTINCT customer_id) AS CustomerCount
FROM
  bronze.raw_customers
```

### Each layer: what it is for and what we did

#### Bronze: keep everything, exactly as it came

**What it is for:** a safe, complete copy of the source, including every version of a row. If silver or gold is wrong,
it can be rebuilt from bronze without touching MySQL. Nobody reports from bronze.

**What we did:**

- Created 6 tables in the `bronze` database from Python (`fintech_el.py`), each with `_loaded_at` and `_batch_id`
  columns, and sort keys chosen per table.
- Loaded 4 MySQL tables incrementally (hourly, watermark plus 10-minute lookback, rows already loaded are skipped), and
  FX rates plus the Excel risk tiers in full (daily, truncate and reload).
- Ran 7 change-tracking tests. They led to these fixes:
  - transactions are now loaded by `updated_at`
  - MySQL runs on UTC
  - FX rates keep 6 decimals
- Found and fixed the watermark-as-text bug and the doubled full snapshots.
- Covered in Modules 4 and 5.

#### Silver: clean, one version of the truth, with history

**What it is for:** cleaned and deduplicated data in a consistent format (PascalCase names, UTC timestamps, trimmed
text, consistent case), plus SCD2 history for things that change over time. Every later model builds on silver, never
on bronze.

**What we did:**

- Set up the dbt project:
  - a profile for ClickHouse
  - a schema macro, so models land in `silver` and `gold`
  - a `sources.yml` declaring bronze
- Built 8 models:
  - SCD2 `customers_history` and `accounts_history`, which merge versions with no real change
  - `_current` views on top of both
  - `transactions`, keeping the latest state, with direction and signed amount
  - `app_events`, with the JSON unpacked
  - `fx_rates`, with USD = 1 added
  - `risk_tiers`
- Added 38 tests: allowed values, links between tables, valid SCD2 ranges, FX coverage, and every bronze ID reaching
  silver.
- Fixed the FX gap: 1,273 transactions had no rate.
- Covered in Modules 6, 7 and 11.

#### Gold: answers for the business

**What it is for:** tables shaped for questions and dashboards. Business rules are applied once, amounts are converted
to USD, and the shape is a set of star schemas, so a dashboard needs at most a simple join.

**What we did:**

- Built a galaxy schema with 2 dimensions and 3 facts:
  - `dim_customers`, with region and risk level
  - `dim_accounts`, with `BalanceUsd`
  - `fact_transactions`, with USD amounts and the cash-flow rule
  - `fact_app_events`
  - `fact_daily_balances`, one row per account per day, built with an `ASOF` join and a running total
- Wrote the `to_usd` macro, so a missing rate is `NULL`, never 0.
- Added 30 tests:
  - row counts match silver
  - USD values are present
  - the cash-flow rules hold
  - daily totals match the transactions
  - the running total adds up
- Reconciled the net cash flow, -289,080.24 USD, against an independent calculation from silver.
- Connected Superset to gold for the PayFlow Overview dashboard.
- Covered in Modules 7, 8 and 12.

### Table count by layer

| Layer | Count | Why the number changes |
|---|---|---|
| MySQL | 5 tables | |
| Bronze | 6 tables | Adds `risk_tiers` from Excel |
| Silver | 8 models (6 tables, 2 views) | Customers and accounts each split into `_history` and `_current` |
| Gold | 5 models | 2 dimensions, 3 facts |

### Key takeaways

- Raw data first, transformations later.
- Bronze is a change log, not a copy.
- Each layer has one job.

---

## Module 4: Ingestion with Airflow: incremental and full loads

### Learning goals

- Write a watermark-based incremental load that never duplicates or skips rows.
- Know when a full reload is the right choice.

### The three DAGs

| DAG | Schedule (UTC) | Tables | Strategy |
|---|---|---|---|
| `generate_daily_data` | 01:00 daily | all MySQL tables | Grows the source, then triggers the incremental load |
| `extract_incremental_loads` | hourly | customers, accounts, transactions, app events | Watermark, then `dbt build` |
| `extract_full_loads` | 02:00 daily | FX rates, risk tiers (Excel) | Truncate and reload |

All DAGs use `catchup=False` and `max_active_runs=1`. The load DAGs retry twice, 5 minutes apart.

### Shared plugin: `airflow/plugins/fintech_el.py`

The plugin holds everything both load DAGs share:

- **Connections:** `mysql_conn()` and `clickhouse_client()`, built from environment variables.
- **Bronze DDL:** column types for each table, plus two load columns added to every table:

  ```sql
  _loaded_at DateTime('UTC') DEFAULT now('UTC'),
  _batch_id String
  ```

  `_batch_id` is the Airflow run ID, so every row can be traced back to the run that loaded it.

- **Sort keys** (`ORDER BY`), chosen for how each table is queried:

  | Table | ORDER BY |
  |---|---|
  | `raw_customers` | `(customer_id, _loaded_at)` |
  | `raw_accounts` | `(account_id, _loaded_at)` |
  | `raw_transactions` | `(created_at, transaction_id)` |
  | `raw_app_events` | `(created_at, event_id)` |
  | `raw_fx_rates` | `(rate_date, target_currency, _loaded_at)` |
  | `risk_tiers` | `(country_code, _loaded_at)` |

- **`check_source()`:** runs `SELECT 1` on both databases, so a run fails fast if either is down.
- **`copy_query_to_bronze()`:** streams a MySQL query into ClickHouse in chunks of 10,000 rows. It can truncate first
  (full load) and skip rows already in bronze (incremental load).

### Incremental load, step by step

```python
# table: (watermark column, primary key)
INCREMENTAL_TABLES = {
    "raw_customers": ("updated_at", "customer_id"),
    "raw_accounts": ("updated_at", "account_id"),
    "raw_transactions": ("updated_at", "transaction_id"),
    "raw_app_events": ("created_at", "event_id"),
}
LOOKBACK = timedelta(minutes=10)
```

For each table, as a mapped Airflow task (`load_delta.expand(...)`):

1. **Read the watermark:** the latest timestamp already in bronze.

   ```python
   # query() returns typed values; command() would return the timestamp as a string
   watermark = client.query(f"SELECT max({column}) FROM {BRONZE_DB}.{table}").result_rows[0][0]
   ```

2. **Step back 10 minutes:** `since = watermark - LOOKBACK`. A row committed late, with a timestamp just before the
   watermark, is still picked up.
3. **Find what bronze already has in that window:** a set of `(key, timestamp)` pairs.
4. **Copy newer rows from MySQL, skipping the pairs bronze already has:**

   ```python
   rows = copy_query_to_bronze(
       table,
       f"SELECT * FROM {table} WHERE {column} > %s ORDER BY {column}",
       (since,),
       run_id,
       skip_keys=already_loaded,
       key_columns=[key, column],
   )
   ```

The result: a rerun never duplicates rows, a late row is not missed, and a changed row (new `updated_at`) lands as a
new version.

```text
check_source_task --> load_delta [4 mapped tasks] --> dbt_build
```

### Full load

FX rates and risk tiers have no column that tracks change, and past FX rates can be corrected in the source, so they
are emptied and reloaded on every run:

```python
rows = copy_query_to_bronze(table, f"SELECT * FROM {table}", None, run_id, replace=True)
```

Risk tiers are read from Excel with pandas (`dtype=str`, rows without `country_code` dropped), then the bronze table is
truncated and the data frame inserted.

The cost of full loads: **no history**. If a rate is corrected, the old value is gone from bronze.

### Incremental vs full: when to use which

| Use incremental when | Use full reload when |
|---|---|
| The table has a reliable change column (`updated_at`, `created_at`) | There is no change column |
| The table is large or growing | The table is small (here: 225 and 8 rows) |
| History of versions matters | Only the current values matter |

### Key takeaways

- A watermark load needs three things: a typed watermark, a lookback window and a duplicate check.
- Full reloads are simple and correct for small reference data, but keep no history.

---

## Module 5: The bronze layer and change-tracking tests

### Learning goals

- Prove that changes in the source reach bronze correctly.
- Recognise the traps of querying bronze directly.

### Bronze tables (9 Nov 2026)

| Table | Rows | Note |
|---|---|---|
| `raw_customers` | 136 | 133 customers, some with several versions |
| `raw_accounts` | 222 | 221 accounts |
| `raw_transactions` | 2,624 | |
| `raw_app_events` | 5,284 | |
| `raw_fx_rates` | 225 | Replaced on each full load |
| `risk_tiers` | 8 | Replaced on each full load |

Timestamps are `DateTime('UTC')`, money is `Decimal(15, 2)` and FX rates are `Decimal(12, 6)`.

### Change-tracking tests

On 29 Oct, each type of change was made in MySQL, loaded, and checked in bronze. Full details are in
`tests/bronze_change_tracking_tests.md`.

| # | Change in MySQL | Reached bronze? | Outcome |
|---|---|---|---|
| 1 | Customer KYC Verified → Pending | Yes, new row | Correct |
| 2 | Same customer, only `updated_at` changed | Yes, new row | Extra version with no real change: silver must merge it |
| 3 | Customer country NLD → DEU | Yes, new row | Correct |
| 4 | Account Active → Suspended, balance 3,162.81 → 2,500.00 | Yes, new row | Correct |
| 5 | Transaction Pending → Completed | **No** | Bug: transactions loaded by `created_at` |
| 6 | App event payload changed | No | Accepted: events are an append-only log |
| 7 | FX rate corrected to 1.4 | Yes, replaced | Showed precision bug: bronze had `Decimal(10, 4)` |
| 8 | Risk tier SGP medium → high (Excel) | Not run yet | |

### Fixes that came out of the tests

`mysql/001_incremental_hardening.sql` (run once as root):

```sql
SET PERSIST time_zone = '+00:00';

ALTER TABLE core_banking_prod.raw_transactions
  ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at,
  ADD INDEX idx_transactions_updated_at (updated_at);

UPDATE core_banking_prod.raw_transactions
SET updated_at = created_at;
```

| Finding | Fix |
|---|---|
| Transaction status changes never reached bronze | Added `updated_at`, load transactions by it |
| `ON UPDATE CURRENT_TIMESTAMP` used server local time (UTC+4), while the pipeline uses UTC | MySQL set to UTC |
| FX rates cut from 6 to 4 decimals | Bronze column changed to `Decimal(12, 6)`, table reloaded |
| Late-committed rows could be skipped | 10-minute lookback window |

### The double-counting trap

A customer with 3 accounts, one updated once, has 4 rows in bronze. Summing bronze counts that account twice:

| Source | Total balance |
|---|---|
| MySQL (current state) | 13,464.20 |
| Bronze (all versions) | 16,627.01 (wrong) |

**Never sum customers or accounts in bronze.** Use the current version from silver.

### Key takeaways

- Test change tracking with real updates before trusting a pipeline.
- Bronze is for loading and rebuilding, not for reporting.

---

## Module 6: The silver layer with dbt and SCD2

### Learning goals

- Set up a dbt project on ClickHouse.
- Build SCD2 history from an append-only bronze layer.
- Apply consistent cleaning rules.

### dbt project setup

```text
dbt_transform/
  dbt_project.yml
  profiles.yml
  macros/
    generate_schema_name.sql
    to_usd.sql
  models/
    bronze/sources.yml
    silver/  (8 models + schema.yml)
    gold/    (5 models + schema.yml)
  tests/
    bronze/  silver/  gold/
```

**Profile** (`profiles.yml`): ClickHouse over HTTP, host and credentials from environment variables, 4 threads.

**Schema names:** by default dbt would create `analytics_silver`. A custom macro uses the layer name as is:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

**Defaults** in `dbt_project.yml`: silver and gold models are `table`, in schemas `silver` and `gold`.

**Sources:** `models/bronze/sources.yml` declares the 6 bronze tables. Bronze has no dbt models, because Airflow, not
dbt, creates it. Silver reads bronze with `{{ source('bronze', 'raw_customers') }}`, which gives dbt full lineage.

### Silver models

| Model | Type | One row per | Built from |
|---|---|---|---|
| `customers_history` | table | Real change of a customer | `bronze.raw_customers` |
| `customers_current` | view | Customer | `customers_history` |
| `accounts_history` | table | Real change of an account | `bronze.raw_accounts` |
| `accounts_current` | view | Account | `accounts_history` |
| `transactions` | table | Transaction (latest state) | `bronze.raw_transactions` |
| `app_events` | table | Event | `bronze.raw_app_events` |
| `fx_rates` | table | Currency per date, plus USD = 1 | `bronze.raw_fx_rates` |
| `risk_tiers` | table | Country | `bronze.risk_tiers` |

### Cleaning rules applied everywhere

- **Names:** columns renamed to PascalCase, such as `customer_id` → `CustomerId`.
- **Text:** trimmed and set to consistent case, such as `initcap(LOWER(TRIM(kyc_status)))` → `Verified`, and
  `UPPER(TRIM(country_code))`.
- **Time:** timestamps read explicitly as UTC: `toDateTime(updated_at, 'UTC')`.
- **Duplicates:** if the same version was loaded twice, the most recently loaded copy wins:

  ```sql
  ORDER BY
    _loaded_at DESC
  LIMIT 1 BY customer_id, updated_at
  ```

### SCD2, step by step (`customers_history`)

SCD2 (slowly changing dimension, type 2) keeps every version of a record, with the dates each version was valid.

```sql
WITH CustomerVersions AS (
  -- 1. One row per customer and updated_at, cleaned
  SELECT ... FROM {{ source('bronze', 'raw_customers') }}
  ORDER BY _loaded_at DESC
  LIMIT 1 BY customer_id, updated_at
),
ComparedVersions AS (
  -- 2. Compare each version with the previous one
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY CustomerId ORDER BY UpdatedAt) AS VersionNumber,
    lagInFrame(tuple(FullName, Email, KycStatus, CountryCode)) OVER (
      PARTITION BY CustomerId
      ORDER BY UpdatedAt
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS PreviousAttributes
  FROM CustomerVersions
),
RealChanges AS (
  -- 3. Keep the first version and every version where a tracked column changed
  SELECT * FROM ComparedVersions
  WHERE
    VersionNumber = 1
    OR tuple(FullName, Email, KycStatus, CountryCode) != PreviousAttributes
),
VersionRanges AS (
  -- 4. A version is valid until the next one starts
  SELECT
    *,
    leadInFrame(UpdatedAt, 1, toDateTime('2100-01-01 00:00:00', 'UTC')) OVER (...) AS NextValidFrom
  FROM RealChanges
)
SELECT
  ...,
  UpdatedAt AS ValidFrom,
  NextValidFrom AS ValidTo,
  NextValidFrom = toDateTime('2100-01-01 00:00:00', 'UTC') AS IsCurrent
FROM VersionRanges
```

| Tracked columns | Model |
|---|---|
| `FullName`, `Email`, `KycStatus`, `CountryCode` | `customers_history` |
| `CustomerId`, `CurrencyCode`, `AccountStatus`, `Balance` | `accounts_history` |

A real example, the customer from the change-tracking tests:

| Version | KycStatus | CountryCode | ValidFrom | ValidTo | IsCurrent |
|---|---|---|---|---|---|
| 1 | Verified | NLD | 2026-05-20 07:47 | 2026-10-29 15:11 | 0 |
| 2 | Pending | NLD | 2026-10-29 15:11 | 2026-10-29 15:35 | 0 |
| 3 | Pending | DEU | 2026-10-29 15:35 | 2100-01-01 | 1 |

Bronze had 4 rows for this customer; the version at 15:14 changed nothing and was merged.

**Point-in-time lookup:** `ValidFrom <= date AND ValidTo > date`.

**Current state:** `customers_current` and `accounts_current` are views:

```sql
SELECT ... FROM {{ ref('customers_history') }}
WHERE
  IsCurrent
```

One source of truth, and no way to double-count.

### Other silver models

**`transactions`:** keeps only the latest state of each transaction, then adds business columns:

| Rule | Logic |
|---|---|
| Latest state | `ORDER BY updated_at DESC, _loaded_at DESC LIMIT 1 BY transaction_id` |
| Amount | `ABS(amount)`, always positive |
| Direction | `in` for Deposit and Transfer In, `out` for everything else |
| SignedAmount | positive for `in`, negative for `out` |
| MerchantCategory | `''` and `N/A` become `NULL` |
| TransactionType | `initcap(replaceAll(LOWER(TRIM(...)), '_', ' '))`, such as `Card Payment` |
| CreatedDateVilnius | `toDate(toTimeZone(CreatedAt, 'Europe/Vilnius'))` for local reporting |

**`app_events`:** unpacks the JSON payload into columns and keeps the raw JSON:

```sql
nullIf(JSONExtractString(event_payload, 'device'), '') AS Device,
nullIf(JSONExtractString(event_payload, 'app_version'), '') AS AppVersion,
JSONExtract(event_payload, 'session_duration_sec', 'Nullable(UInt32)') AS SessionDurationSec,
JSONExtract(event_payload, 'flagged_for_review', 'Nullable(Bool)') AS FlaggedForReview,
```

**`fx_rates`:** renames columns to `RateDate`, `CurrencyCode` and `UnitsPerUsd`, and adds a `USD = 1` row for every
date, so USD transactions convert with the same join. That is why silver has 300 rows and bronze 225.

**`risk_tiers`:** cleaned country code, region and lowercase risk level.

### Key takeaways

- SCD2 needs: deduplicate, compare with the previous version, drop non-changes, set validity ranges.
- `_current` views over `_history` give one source of truth.
- Clean once, in silver, so gold never repeats cleaning logic.

---

## Module 7: dbt materializations, model by model, and snapshots

### Learning goals

- Choose the right materialization for each model.
- Understand dbt snapshots and why this project does not use them.

### The four materializations

| Type | What it does | Used here | Why |
|---|---|---|---|
| `table` | Rebuilds the whole table each run | 6 silver, 5 gold models | Simple, correct, and the full build takes seconds |
| `view` | Stores only the query | `customers_current`, `accounts_current` | A filter over history, so it cannot drift |
| `incremental` | Processes only new or changed rows | none | Data is too small to pay off; SCD2 is hard to do incrementally |
| `ephemeral` | Not stored; inlined as a CTE | none | Shared logic lives in a macro (`to_usd`) instead |

**When to switch to incremental:** when `dbt build` takes a meaningful share of the hourly run. `app_events` would go
first (append-only, so the simplest), then `transactions` (needs `delete+insert` by `TransactionId`, because statuses
change). The models would filter on `_loaded_at`, not business dates, so late rows are not missed. A full rebuild
stays available with `dbt build --full-refresh`.

### Where the materialization is set in code

dbt reads the settings in this order; the last one wins:

1. **Project defaults** in `dbt_project.yml`: every model in a folder gets the same settings.

   ```yaml
   models:
     dbt_transform:
       silver:
         +schema: silver
         +materialized: table
       gold:
         +schema: gold
         +materialized: table
   ```

2. **A `config()` block at the top of a model** overrides the folder default for that one model:

   ```sql
   {{ config(materialized='view') }}
   ```

   Only `customers_current` and `accounts_current` do this. The other 11 models only set `order_by`.

### What `table` and `view` mean in ClickHouse

| Setting | What dbt-clickhouse creates | What happens on each `dbt build` |
|---|---|---|
| `materialized='table'` | A `MergeTree` table with the data stored on disk | Runs the `SELECT` into a new table, then swaps it with the old one, so readers never see a half-built table |
| `materialized='view'` | A ClickHouse `VIEW`: only the query is stored | Replaces the view definition; data is read from `customers_history` or `accounts_history` at query time |
| `order_by='(AccountId, ValidFrom)'` | The `ORDER BY` (sort key) of the `MergeTree` table | Data is stored sorted by these columns, so filters on them read less data |

Each sort key is chosen for how the model is queried: entity ID first for dimensions and history tables, date first
for facts that are filtered by date range (as the dashboard does).

### dbt lineage: what feeds what

```text
BRONZE (sources)        SILVER                                  GOLD
----------------        ------                                  ----
raw_customers    --> customers_history --> customers_current --+
risk_tiers       --> risk_tiers -----------------------------+--+--> dim_customers

raw_accounts     --> accounts_history  --> accounts_current ----+--> dim_accounts
raw_fx_rates     --> fx_rates -----------------------------------+
raw_transactions --> transactions -------------------------------+--> fact_transactions
                                                                 |          |
                     accounts_history, accounts_current, fx_rates+--> fact_daily_balances

raw_app_events   --> app_events ----------------------------------> fact_app_events
```

`{{ source(...) }}` points at a bronze table; `{{ ref(...) }}` points at another dbt model. dbt reads these to work
out the build order, so `fact_daily_balances` always runs after `fact_transactions`.

### Model by model: what the code does

#### Bronze: no models, only `sources.yml`

Airflow creates and fills bronze, so dbt only **declares** it:

| What `sources.yml` gives | Example |
|---|---|
| A name to read from | `{{ source('bronze', 'raw_customers') }}` |
| Descriptions in dbt docs | "Incremental by updated_at; a changed customer lands again as a new row." |
| Freshness rules (run with `dbt source freshness`, not by `dbt build`) | `raw_transactions`: warn after 2 hours, error after 6 |

#### Silver: 8 models

| Model | Stored as | Sort key | Reads | What the code does, step by step |
|---|---|---|---|---|
| `customers_history` | table | `(CustomerId, ValidFrom)` | `bronze.raw_customers` | 1. Renames and cleans columns (trim, lowercase email, `initcap` KYC status, uppercase country, timestamps as UTC). 2. `LIMIT 1 BY customer_id, updated_at` keeps one copy of each version, the latest loaded. 3. `lagInFrame` compares each version with the previous one. 4. Keeps the first version and real changes only. 5. `leadInFrame` sets `ValidTo` to the next version's start, or 2100-01-01. 6. `IsCurrent` is true for the open version |
| `customers_current` | **view** | none | `customers_history` | `WHERE IsCurrent`; renames `ValidFrom` to `UpdatedAt` |
| `accounts_history` | table | `(AccountId, ValidFrom)` | `bronze.raw_accounts` | Same 6 steps as customers; tracked columns are `CustomerId`, `CurrencyCode`, `AccountStatus`, `Balance` |
| `accounts_current` | **view** | none | `accounts_history` | `WHERE IsCurrent` |
| `transactions` | table | `(CreatedAt, TransactionId)` | `bronze.raw_transactions` | 1. `ORDER BY updated_at DESC, _loaded_at DESC LIMIT 1 BY transaction_id` keeps only the latest state, so a Pending → Completed change replaces the old row. 2. Cleans type, status and currency text. 3. `ABS(amount)`. 4. `''` and `N/A` merchant category become `NULL`. 5. Adds `Direction` and `SignedAmount`. 6. Adds `CreatedDateVilnius` |
| `app_events` | table | `(CreatedAt, EventId)` | `bronze.raw_app_events` | 1. `LIMIT 1 BY event_id` drops duplicate loads. 2. `JSONExtractString` / `JSONExtract` turn the payload into `Device`, `AppVersion`, `SessionDurationSec`, `FlaggedForReview`. 3. Keeps the raw JSON as `EventPayload` |
| `fx_rates` | table | `(RateDate, CurrencyCode)` | `bronze.raw_fx_rates` | 1. `SourceRates`: renames, casts the rate to `Decimal(18, 6)`. 2. `UsdRates`: one `USD = 1` row per date. 3. `UNION ALL` of both |
| `risk_tiers` | table | `CountryCode` | `bronze.risk_tiers` | Cleans text (uppercase country, lowercase risk level), `LIMIT 1 BY country_code` |

Why history is a table but current is a view: history does the heavy work (window functions over all versions), so it
is stored. Current is a simple filter on top, and as a view it can never disagree with history.

#### Gold: 5 models, all tables

| Model | Sort key | Reads | What the code does, step by step |
|---|---|---|---|
| `dim_customers` | `CustomerId` | `customers_current`, `risk_tiers` | 1. `RiskTiers` CTE makes `Region` and `ComplianceRiskLevel` `Nullable`, so a country missing from the Excel file shows as `NULL`, not an empty string. 2. `LEFT JOIN` on `CountryCode` |
| `dim_accounts` | `AccountId` | `accounts_current`, `fx_rates` | 1. `LatestRates` CTE: `argMax(UnitsPerUsd, RateDate)` picks the newest rate per currency. 2. `LEFT JOIN` on currency. 3. `to_usd()` gives `BalanceUsd` |
| `fact_transactions` | `(TransactionDate, TransactionId)` | `transactions`, `accounts_current`, `fx_rates` | 1. `FxRates` CTE makes the rate `Nullable`. 2. `INNER JOIN accounts_current` adds `CustomerId`. 3. `LEFT JOIN` rates on UTC date and currency. 4. `to_usd()` gives `AmountUsd` and the signed USD amount. 5. `IsInCashFlow` = Completed and not Transfer. 6. `NetFlowUsd` = signed USD amount if in cash flow, else 0 |
| `fact_app_events` | `(EventDate, EventId)` | `app_events` | Selects the event columns (the raw JSON and IP are dropped) and adds `EventDate` (UTC) |
| `fact_daily_balances` | `(AccountId, BalanceDate)` | `accounts_history`, `accounts_current`, `fact_transactions`, `fx_rates` | 1. `AccountStartDates`: earliest of first version and first transaction. 2. `AccountDays`: `arrayJoin(range(...))` makes one row per day up to today. 3. `AccountVersions` + `DailyReportedBalances`: `ASOF LEFT JOIN` finds the balance valid at 23:59:59 that day. 4. `DailyFlows`: `sumIf` gives inflow and outflow, `SUM` the day's net, cash-flow rows only. 5. `Combined`: joins it all, missing days become 0, converts the reported balance at that day's rate. 6. Final `SELECT`: window `SUM` gives `RunningNetFlowUsd` |

#### Macros used by the models

| Macro | Used in | What it does |
|---|---|---|
| `generate_schema_name` | every model, automatically | Puts models in `silver` and `gold`, not `analytics_silver` |
| `to_usd(amount, rate)` | `dim_accounts`, `fact_transactions`, `fact_daily_balances` | `amount / rate` as `Nullable(Decimal(18, 6))`; a missing rate gives `NULL`, never 0 |

### dbt snapshots, and why we do not use them

A **dbt snapshot** stores SCD2 history by comparing a source table with the previous snapshot each time it runs.
History only exists from the first run onwards, and if a snapshot run is missed, changes in between are lost.

This project builds SCD2 **from bronze** instead:

| | dbt snapshots | SCD2 from bronze (this project) |
|---|---|---|
| Where history is stored | A snapshot table dbt writes to | Bronze, written by the loader |
| Can it be rebuilt? | No: a dropped snapshot loses its history | Yes: rerun dbt, history comes back from bronze |
| Missed run | Changes in between are lost | Nothing lost, bronze has every loaded version |

The `snapshots/` folder exists in the project but is empty.

### Dashboard snapshot

The word "snapshot" also appears for the dashboard: `docs/dashboard/payflow-overview.pdf` is a PDF export of the
Superset dashboard taken on 9 Nov 2026 (Module 12).

---

## Module 8: The gold layer: star schemas

### Learning goals

- Choose between star, snowflake, galaxy and one big table (OBT).
- Convert currencies safely.
- Build a daily fact table with as-of joins.

### The design: a galaxy schema

Three star schemas share two dimensions. This is also called a fact constellation.

```text
                  +-------------------+
          +-------| fact_transactions |-------+
          |       +-------------------+       |
          v                                   v
+--------------+                     +---------------+
| dim_accounts |                     | dim_customers |
+--------------+                     +---------------+
          ^                                ^     ^
          |       +---------------------+  |     |
          +-------| fact_daily_balances |--+     |
                  +---------------------+        |
                                                 |
                  +-----------------+            |
                  | fact_app_events |------------+
                  +-----------------+
```

| Option | What it is | Verdict here |
|---|---|---|
| Single star | One fact table for everything | No: transactions, balances and events have different grains |
| Snowflake | Dimensions chained to other dimensions | No: facts carry both `AccountId` and `CustomerId`, so one join per dimension |
| OBT | One wide table, everything copied onto each row | No as the core; can be added later as a view on top |
| **Galaxy** | Several stars sharing dimensions | **Yes**: one meaning per fact table, one definition of "customer" |

### Gold models (9 Nov 2026)

| Model | Grain | Rows | Main columns |
|---|---|---|---|
| `dim_customers` | Customer | 133 | `CustomerId`, `KycStatus`, `CountryCode`, `Region`, `ComplianceRiskLevel` |
| `dim_accounts` | Account | 221 | `AccountId`, `CustomerId`, `CurrencyCode`, `AccountStatus`, `Balance`, `BalanceUsd` |
| `fact_transactions` | Transaction | 2,624 | `Amount`, `AmountUsd`, `IsInCashFlow`, `NetFlowUsd`, `TransactionDate` |
| `fact_app_events` | Event | 5,284 | `EventType`, `Device`, `AppVersion`, `EventDate` |
| `fact_daily_balances` | Account per day | 12,042 | `InflowUsd`, `OutflowUsd`, `NetFlowUsd`, `RunningNetFlowUsd`, `ReportedBalance`, `ReportedBalanceUsd` |

### USD conversion: the `to_usd` macro

```sql
{#- Convert to USD with a units-per-USD rate. A missing amount or rate stays NULL, never 0. -#}
{% macro to_usd(amount, rate) -%}
  CAST(CAST({{ amount }} AS Nullable(Decimal(38, 6))) / {{ rate }} AS Nullable(Decimal(18, 6)))
{%- endmacro %}
```

Rates are units per USD, so `AmountUsd = Amount / UnitsPerUsd`, using the rate for the transaction's UTC date.

> **ClickHouse trap:** a `LEFT JOIN` with no match fills columns with default values (0 for numbers), not `NULL`.
> A missing rate would silently become 0. The fix: make the rate `Nullable` before the join
> (`toNullable(UnitsPerUsd)`), so a missing match stays `NULL` and tests can catch it.

### Business rules in `fact_transactions`

| Rule | Decision | Why |
|---|---|---|
| `IsInCashFlow` | `Status = 'Completed' AND TransactionType != 'Transfer'` | Failed and pending moved no money; Transfer has no direction in the source |
| `NetFlowUsd` | Signed USD amount when in cash flow, else 0 | Summing it gives net cash flow directly |
| All statuses kept | Yes | Failed and pending are still worth analysing |
| `CustomerId` on the fact | From `accounts_current` | Keeps it a star, not a snowflake |

### `fact_daily_balances`: as-of joins

One row per account per UTC day, from the account's first transaction or version to today.

1. **Generate the days** with an array:

   ```sql
   arrayJoin(
     arrayMap(
       DayOffset -> StartDate + DayOffset,
       range(toUInt32(dateDiff('day', StartDate, toDate(now(), 'UTC')) + 1))
     )
   ) AS BalanceDate
   ```

2. **Reported balance at the end of each day** with ClickHouse's `ASOF JOIN`, which picks the latest version at or
   before the given time:

   ```sql
   ASOF LEFT JOIN AccountVersions av ON av.AccountId = ad.AccountId
   AND ad.EndOfDay >= av.ValidFrom
   ```

3. **Daily inflow, outflow and net flow** from `fact_transactions`.
4. **Running total:** `SUM(NetFlowUsd) OVER (PARTITION BY AccountId ORDER BY BalanceDate ROWS BETWEEN UNBOUNDED
   PRECEDING AND CURRENT ROW)`.

Two balance columns are kept separate on purpose: transactions do not update `Balance` in the source, so
`RunningNetFlowUsd` and `ReportedBalanceUsd` are not expected to match.

### Verified results (2 Nov 2026)

Gold totals were checked against an independent calculation from silver:

| Transaction type | Net (USD) | In cash flow |
|---|---|---|
| Card Payment | -275,557.39 | Yes |
| Deposit | 256,175.69 | Yes |
| Withdrawal | -269,698.53 | Yes |
| Transfer | 0 (volume 255,860.17) | No |
| **Net cash flow** | **-289,080.24** | |

### Key takeaways

- Pick the schema by grain: different grains need different fact tables.
- Never let a missing rate become 0.
- `ASOF JOIN` is the natural tool for "value as of a date" in ClickHouse.

---

## Module 9: Data quality: dbt tests

### Learning goals

- Know the two kinds of dbt tests and where each lives.
- Test what each layer can break.

### Two kinds of tests

| Kind | Where | Example |
|---|---|---|
| Generic (column) tests | `schema.yml` next to the models | `unique`, `not_null`, `accepted_values`, `relationships` |
| Singular (custom SQL) tests | `tests/<layer>/*.sql` | Any query; the test fails if it returns rows |

### Tests by layer: 69 in total

| Layer | Tests | Custom SQL tests in `tests/<layer>/` |
|---|---|---|
| Bronze | 1 | `assert_fx_rates_loaded_recently` (warning only) |
| Silver | 38 | `assert_one_current_version_per_entity`, `assert_scd2_ranges_are_valid`, `assert_fx_rate_exists_for_transactions`, `assert_fx_rates_unique_per_date_and_currency`, `assert_transaction_currency_matches_account`, `assert_bronze_ids_reach_silver` |
| Gold | 30 | `assert_fact_row_counts_match_silver`, `assert_usd_values_present`, `assert_cash_flow_rules`, `assert_daily_balances_unique_per_account_and_date`, `assert_daily_net_flow_matches_transactions`, `assert_running_net_flow_is_cumulative` |

A full `dbt build` runs 82 nodes (13 models and 69 tests).

### What each layer tests

| Layer | Risk | Tests |
|---|---|---|
| Bronze | Source stopped loading | FX rates loaded in the last 26 hours |
| Silver | Bad values, broken links, broken SCD2 | Allowed values for statuses and types; links between customers, accounts and transactions; one current version; valid date ranges; every bronze ID reaches silver |
| Silver | FX gaps | Every transaction has a rate for its date; one rate per date and currency |
| Gold | Joins multiply or drop rows | Row counts match silver; unique keys |
| Gold | Broken conversion or rules | USD values present; Transfer and non-completed never in cash flow |
| Gold | Inconsistent numbers | Daily totals match transaction totals; running total adds up day by day |

### Examples

A singular test returns the rows that break the rule:

```sql
SELECT
  RateDate,
  CurrencyCode,
  COUNT(*) AS RateCount
FROM
  {{ ref('fx_rates') }}
GROUP BY
  RateDate,
  CurrencyCode
HAVING
  RateCount > 1
```

A warning-only test sets its severity:

```sql
{{ config(severity='warn') }}

SELECT
  MAX(_loaded_at) AS LastLoadedAt
FROM
  {{ source('bronze', 'raw_fx_rates') }}
HAVING
  LastLoadedAt < now() - INTERVAL 26 HOUR
```

### Designing tests for timing

Today's FX rate arrives with the 02:00 UTC full load, after the 01:00 generator has already added today's
transactions. So for up to about 2 hours each night, the newest transactions have no rate. The FX and USD tests ignore
today's rows, so this expected gap does not turn the DAG red:

```sql
WHERE
  toDate(st.CreatedAt) < toDate(now(), 'UTC')
```

### What happens when a test fails

- The `dbt_build` task fails, so the DAG run turns red.
- Models downstream of the failing test are skipped.
- Tables already rebuilt in that run stay as they are, so bad data can be visible until fixed.

### Beyond dbt tests: how results were verified

- **Run twice:** the DAG was run twice in a row; row counts were identical.
- **Independent reconciliation:** gold totals were recalculated straight from silver with different SQL.
- **Rolled-back tests:** the generator fix was tested inside a MySQL transaction that was rolled back.

### Key takeaways

- Test in each layer what that layer can break; do not repeat upstream tests.
- Design tests around known timing gaps, or they will cry wolf.

---

## Module 10: Orchestrating dbt from Airflow

### Learning goals

- Run dbt as part of a DAG.
- Isolate dbt's Python packages from Airflow's.

### The problem

At first, silver only updated when someone ran `dbt build` by hand. New rows reached bronze every hour but silver was
4 days old.

### The solution: a `dbt_build` task

```python
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"

@task(retries=0)
def dbt_build():
    project_dir = os.environ["DBT_PROJECT_DIR"]
    result = subprocess.run(
        [
            DBT_BIN, "build",
            "--project-dir", project_dir,
            "--profiles-dir", project_dir,
            "--target-path", "/tmp/dbt_target",
            "--log-path", "/tmp/dbt_logs",
        ],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt build failed with exit code {result.returncode}")

check_source_task() >> load_delta.expand(table=list(INCREMENTAL_TABLES)) >> dbt_build()
```

| Choice | Why |
|---|---|
| dbt in its own venv | Airflow and dbt need different `clickhouse-connect` versions |
| `--target-path` and `--log-path` in `/tmp` | The `dbt` container owns `target/` and `logs/` in the shared folder |
| `retries=0` | A failing model or test will not fix itself; the next hourly run tries again |
| Runs after all 4 loads | Default trigger rule: only if every load succeeded |

### Same DAG or a separate one?

| Same DAG (chosen) | Separate DAG |
|---|---|
| Order guaranteed with no extra trigger | Load and transform fail separately |
| One place to see load and build | Can rerun dbt without reloading |
| A dbt failure marks the whole run red | One more DAG to manage |

Move to a separate DAG when several DAGs must trigger the build, or when the build becomes slow enough to delay the
hourly load.

---

## Module 11: The FX rate problem: a case study

This is a worked example of finding, sizing and fixing a data gap.

### 1. Discovery

Before building gold, a coverage check joined transactions to rates:

```sql
SELECT
  COUNT(*) AS MissingRateTransactions
FROM
  silver.transactions st
  LEFT ANTI JOIN silver.fx_rates fr ON fr.RateDate = toDate(st.CreatedAt)
  AND fr.CurrencyCode = st.CurrencyCode
```

Result: **1,273 of 2,435 transactions (52%) had no rate.** All 36 dbt tests still passed, because no test looked for
it.

### 2. Diagnosis

The rate table itself was complete; its date range was too short:

```text
Transactions:  27 Aug ---------------------------------------- 2 Nov
Rates:                    27 Sep ------------- 29 Oct
                  ^                                    ^
          no rates exist (1,015)              in MySQL, not loaded (258)
```

| Gap | Cause |
|---|---|
| 258 newest transactions | `extract_full_loads` was paused, so bronze stopped at 29 Oct |
| 1,015 oldest transactions | MySQL had no rates before 27 Sep |

### 3. A third problem found while fixing

The generator rewrote the last 30 days of rates with new random values on every run
(`ON DUPLICATE KEY UPDATE exchange_rate = VALUES(exchange_rate)`). Past USD values would change every night.

### 4. Fixes

| Step | Action | Result |
|---|---|---|
| 1 | Unpaused `extract_full_loads` | 258 fixed |
| 2 | One-time insert of 93 rates (27 Aug to 26 Sep, 31 days × 3 currencies) | 1,015 fixed |
| 3 | Generator keeps existing rates: `ON DUPLICATE KEY UPDATE exchange_rate = exchange_rate` | Past rates stable |
| 4 | Tests: FX coverage, FX uniqueness, USD values present | A future gap is caught |

The missing count went from 1,273 to 0. On 9 Nov, the first real nightly run confirmed the fix: an existing rate
(EUR on 15 Oct, 0.901249) was unchanged, and only the 7 missing days were added.

### Lessons

- Passing tests only prove what they test.
- Check the coverage of reference data before building on it.
- A full reload of correct data still fails if the source keeps rewriting history.

---

## Module 12: Superset dashboard

### Learning goals

- Connect Superset to ClickHouse.
- Build datasets, charts and filters through the API, as code.
- Export a dashboard for sharing.

### Connection

Superset uses the `clickhouse-connect` driver:

```text
clickhousedb://<user>:<password>@clickhouse:8123/gold
```

`clickhouse` is the Docker service name, so Superset reaches the warehouse on the internal network.

### Datasets

| Dataset | Type | Source |
|---|---|---|
| `fact_transactions` | Physical | `gold.fact_transactions` |
| `fx_rates` | Physical | `silver.fx_rates` |
| `customer_profile` | SQL (virtual) | `dim_customers` + account counts and USD balances + failed PIN attempts |
| `fx_latest` | SQL (virtual) | Latest rate per currency, both ways (`UnitsPerUsd`, `UsdPerUnit`) |

### The PayFlow Overview dashboard

| Section | Charts | Chart type |
|---|---|---|
| Overview | Completed volume, net cash flow, completed transactions, customers | Big number |
| Transaction volume | Daily volume by type | Stacked bar |
| Net cash flow | Daily net cash flow; cumulative net cash flow | Bar; line with cumulative sum |
| Risk profiles | Customers by risk level; risk profile by region | Pie; table |
| FX converter | Latest rates; rates over time | Table; line |

### Filters

| Filter | Applies to | Behaviour |
|---|---|---|
| Date range | All time charts | Default: Last month |
| Transaction currency | Transaction KPIs and charts | Single select, default USD; amounts shown in that currency |
| FX currency | FX charts | Multi-select |

**Design decision:** amounts in different currencies must never be added together. With USD conversion, every chart
could show USD; the choice made instead was to show transactions in their own currency, one currency at a time, so
selecting EUR shows only EUR transactions, in euros.

> **Lesson:** the first version scoped the currency filter to the FX charts only, so selecting a currency changed
> nothing on the transaction charts. A filter's scope must be part of its design, and visible to users.

### Dashboard as code

`superset/build_dashboard.py` runs inside the Superset container and uses the REST API:

1. Log in, get a CSRF token.
2. Create the ClickHouse connection (tested first) or reuse it.
3. Create or update the 4 datasets.
4. For each chart: run its query through `/api/v1/chart/data` to prove it returns data, then create or update it.
5. Lay out the dashboard (`position_json`) and set the filters (`json_metadata`).

If the dashboard exists, charts and filters are updated in place, keeping the layout and the Date range default set in
the UI.

```powershell
# Set CLICKHOUSE_USER and CLICKHOUSE_PASSWORD from .env in the shell first
Get-Content superset/build_dashboard.py | docker exec -i -e CLICKHOUSE_USER -e CLICKHOUSE_PASSWORD superset python -
```

Verified on 9 Nov: chart totals matched direct ClickHouse queries exactly for USD (381,722), EUR (306,252) and GBP
(333,349) completed volume.

### Sharing the dashboard

The dashboard itself lives in Superset's metadata database (`superset_home` volume), not in Git. To share it:

- **Export:** dashboard menu **⋯ → Download → Export to PDF** or **Download as Image**.
- **Snapshot:** `docs/dashboard/payflow-overview.pdf` (9 Nov 2026, Last month, USD), linked from the README.

---

## Module 13: Git workflow and security

### Branches and pull requests

Every change went on a feature branch, branched from the latest `origin/main`, merged by pull request:

| PR | Branch | Content |
|---|---|---|
| #1 | `feature/daily-data-generator` | Generator DAG, FX precision fix, change-tracking test results |
| #2 | `fix/incremental-hardening` | Transactions by `updated_at`, lookback window, MySQL on UTC |
| #3 | `feature/dbt-silver-models` | Silver layer |
| #4 | `feature/dbt-build-in-incremental-dag` | dbt runs from Airflow |
| #5 | `feature/gold-layer` | Gold layer, generator FX fix, tests by layer, documentation |
| #6 | `feature/superset-dashboard` | Dashboard build script, PDF snapshot |

After merging, branches were deleted locally (`git branch -d`, which refuses unmerged work) and on GitHub, after
checking each was an ancestor of `origin/main`.

### Security practices

- `.env` holds every password and is git-ignored; `.env.example` is the template.
- Before the first public push, passwords were found in code and moved to `.env`; they never entered Git history.
- Staged changes were scanned for passwords before every commit.
- Separate MySQL logins for loading and generating; the pipeline never uses root.
- The dashboard PDF was checked before committing: it holds totals only, no names or emails.

**Still open:** change the MySQL root password (it was hardcoded and passed through chat) and make `banking_user`
read-only (last seen with `ALL PRIVILEGES`).

---

## Module 14: Troubleshooting and lessons learned

### Every issue met, and how it was fixed

| # | Issue | How it showed up | Fix |
|---|---|---|---|
| 1 | Watermark read as text, so every run restarted from 1970 | Incremental tables doubled | `client.query()` returns a typed value |
| 2 | Full snapshots doubled customers and accounts | 332 rows vs 166 | Moved to incremental by `updated_at` |
| 3 | New rows dated before the watermark would be skipped | Code review | Generator dates new rows after the latest ones |
| 4 | Country codes did not match (ISO-2 vs ISO-3) | Code review | Rebuilt the risk tiers file |
| 5 | Local time on Windows, UTC in the container | DAG review | Generator always uses UTC |
| 6 | FX rates cut to 4 decimals | Change-tracking test | `Decimal(12, 6)`, reloaded |
| 7 | Transaction status changes never reached bronze | Change-tracking test | `updated_at` added and used |
| 8 | `updated_at` filled in server local time (UTC+4) | Schema check | MySQL set to UTC |
| 9 | Passwords in code, no `.gitignore` | Search before first push | `.env`, `.gitignore` |
| 10 | `README.md` had a UTF-16 line | Read before first commit | Rewritten as UTF-8 |
| 11 | `DBT_PROJECT_DIR` pointed `dbt debug` at the Airflow path | Running `dbt debug` | Overridden in the dbt container |
| 12 | Airflow web page would not open | Stale `airflow-webserver.pid` pointed at another process | Deleted the file, started the web server; permanent fix open |
| 13 | New rows never reached silver | Silver 4 days older than bronze | `dbt_build` task in the DAG |
| 14 | 1,273 transactions had no FX rate | Coverage check | Module 11 |
| 15 | Generator rewrote past FX rates | Code review | Keep existing rates |
| 16 | `fact_daily_balances` failed with a nested aggregate error | First gold build | Renamed an alias that shadowed its source column |
| 17 | Currency filter did nothing on transaction charts | User testing | Separate Transaction currency filter, scoped to those charts |

### Process lessons

- **Check side effects before saying nothing changed.** A cancelled command had in fact run.
- **Verify facts before stating them.** The MySQL time zone was assumed to be UTC+2; it was UTC+4.
- **Run new DAGs twice before trusting them.** The watermark bug showed up as duplicated data in a live pipeline.
- **Run migrations before deploying code that needs them.** Mounted DAG files go live immediately.
- **Check existing tools first.** The first compose file had 6 services where 4 were needed.
- **Reconcile against an independent calculation,** not only against tests written by the same person.

### ClickHouse-specific gotchas

| Gotcha | What to do |
|---|---|
| `LEFT JOIN` fills non-matches with defaults, not `NULL` | Make joined columns `Nullable`, or set `join_use_nulls` |
| An alias with the same name as a column replaces it in the same `SELECT` | Use distinct alias names (issue 16) |
| `client.command()` returns strings | Use `client.query()` for typed values |
| `LIMIT 1 BY` | Fast deduplication: keep the first row per key after `ORDER BY` |

---

## Module 15: Operations cheat sheet

### Start and stop

```powershell
docker compose start clickhouse airflow dbt superset   # keeps Airflow run history
docker compose stop                                     # stops everything
docker compose up -d                                    # only when the config changed: recreates containers
```

> `docker compose up -d` after a config change recreates Airflow, which wipes its SQLite run history and pauses all
> DAGs. Unpause them afterwards.

### Links

| Tool | URL |
|---|---|
| Airflow | http://localhost:8080 |
| ClickHouse SQL editor | http://localhost:8123/play |
| ClickHouse health | http://localhost:8123/ping (should say `Ok.`) |
| Superset dashboard | http://localhost:8088/superset/dashboard/payflow-overview/ |

### Airflow

```powershell
docker exec airflow airflow dags trigger extract_incremental_loads
docker exec airflow airflow dags trigger extract_full_loads
docker exec airflow airflow dags list-import-errors
```

**If the Airflow page does not open** but the container is running:

```powershell
docker exec airflow rm -f /opt/airflow/airflow-webserver.pid
docker exec -d airflow airflow webserver
```

### dbt

```powershell
docker compose exec dbt dbt debug
docker compose exec dbt dbt build
docker compose exec dbt dbt build --select gold
docker compose exec dbt dbt test --select silver
```

### Useful checks

```sql
SELECT
  COUNT(*) AS MissingRateTransactions
FROM
  silver.transactions st
  LEFT ANTI JOIN silver.fx_rates fr ON fr.RateDate = toDate(st.CreatedAt)
  AND fr.CurrencyCode = st.CurrencyCode
```

```sql
SELECT
  database,
  name,
  metadata_modification_time
FROM
  system.tables
WHERE
  database IN ('silver', 'gold')
ORDER BY
  metadata_modification_time
```

---

## Module 16: Known limits and next steps

### Known limits

| Limit | Impact |
|---|---|
| Rows deleted in MySQL stay in bronze | Watermark loads cannot see deletes |
| Several changes between two loads keep only the last | Hourly granularity for history |
| Changes that do not update `updated_at` are not seen | Relies on the source setting it |
| FX rates and risk tiers keep no history | Corrections replace old values |
| `raw_accounts` has no `created_at` | About 62% of `fact_daily_balances` rows have no `ReportedBalance`; `RunningNetFlowUsd` is not affected |
| No high-risk customers in the data | The risk charts show low and medium only |
| About 2 hours each night without today's FX rate | Tests ignore today's rows |
| Airflow standalone uses SQLite | Recreating the container loses run history |
| Superset has no amount-input box | The FX converter is a rate lookup, not a calculator |

### Next steps

- [ ] Change the MySQL root password; make `banking_user` read-only.
- [ ] Add the PID file removal to Airflow's startup command.
- [ ] Run the risk tiers test (SGP medium → high).
- [ ] Decide whether to trim about 10 redundant gold tests.
- [ ] Switch `app_events`, then `transactions`, to incremental when builds slow down.
- [ ] Add CI checks (`dbt build` on pull requests) and README screenshots.

---

## Appendix: Glossary

| Term | Meaning |
|---|---|
| **ELT** | Extract, Load, Transform: load raw data first, transform inside the warehouse |
| **Medallion architecture** | Bronze (raw), silver (clean), gold (business-ready) layers |
| **Watermark** | The latest timestamp already loaded; the next load starts from it |
| **Lookback window** | Re-reading a short period before the watermark to catch late rows |
| **Idempotent** | Running it twice gives the same result as running it once |
| **SCD2** | Slowly changing dimension type 2: one row per version, with valid-from and valid-to dates |
| **Grain** | What one row of a table represents |
| **Fact table** | Events or measurements, such as transactions |
| **Dimension** | Descriptive context, such as customers |
| **Star schema** | A fact table joined directly to its dimensions |
| **Snowflake schema** | Dimensions joined to further dimensions |
| **Galaxy schema** | Several star schemas sharing dimensions |
| **OBT** | One Big Table: everything denormalised into one wide table |
| **Materialization** | How dbt stores a model: table, view, incremental or ephemeral |
| **dbt snapshot** | dbt's built-in way to record SCD2 history by comparing runs |
| **Generic test** | A reusable dbt column test, such as `unique` |
| **Singular test** | A custom SQL query that fails if it returns rows |
| **ASOF JOIN** | A ClickHouse join that matches the latest row at or before a given value |
| **Native filter** | A Superset dashboard filter applied to charts in its scope |
