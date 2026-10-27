# PayFlow Data Platform: Global Fintech Analytics Pipeline

An ELT pipeline that moves core-banking data from MySQL (OLTP) into a ClickHouse warehouse and models it in
bronze, silver and gold layers with dbt. Airflow orchestrates the loads. See `source_data/Data Architecture.jpg`
and `source_data/ERD_core_banking.pdf`.

```
MySQL core_banking_prod ─┐
                         ├─ Airflow (Python + clickhouse-connect) ─► ClickHouse bronze ─► dbt silver ─► dbt gold ─► Superset
country_risk_tiers.xlsx ─┘
```

## Layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | ClickHouse, Airflow (standalone), dbt Core, Superset. MySQL runs on the host (MySQL80 service) |
| `.env.example` | Template for `.env`, which holds every login and password and is never committed |
| `mock_data/generate_mock_data.py` | Creates the `core_banking_prod` schema and loads the initial data set |
| `source_data/data_generator.py` | Adds 3-5% new rows to each MySQL table, dated after the existing ones |
| `source_data/country_risk_tiers.xlsx` | Manually managed reference data: `country_code`, `region`, `compliance_risk_level` |
| `airflow/dags/extract_incremental_loads.py` | Hourly watermark loads: customers, accounts (`updated_at`), transactions, app events (`created_at`) |
| `airflow/dags/extract_full_loads.py` | Daily replace loads: FX rates and risk tiers (no change-tracking column) |
| `airflow/plugins/fintech_el.py` | Shared MySQL/ClickHouse connections, bronze DDL and chunked copy |
| `dbt_transform/` | dbt project (ClickHouse adapter): `models/bronze`, `silver`, `gold`, plus seeds/tests/macros |
| `commands for docker images.txt` | Day-to-day commands: start/stop, links, loading, checks |

## Quick start

```powershell
# 1. Create your own .env from the template and fill in the passwords
copy .env.example .env

# 2. In MySQL, create the database and a read-only login for Airflow (values from .env)
#    CREATE DATABASE core_banking_prod;
#    CREATE USER 'banking_user'@'%' IDENTIFIED BY '<MYSQL_PASSWORD>';
#    GRANT SELECT ON core_banking_prod.* TO 'banking_user'@'%';

# 3. Start the stack: Airflow http://localhost:8080, Superset http://localhost:8088 (logins from .env)
docker compose up -d

# 4. Local Python env for the data generators
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 5. Initial data set, then add 3-5% new rows whenever you want fresh data
python mock_data/generate_mock_data.py
python source_data/data_generator.py

# 6. In Airflow, unpause and trigger extract_incremental_loads and extract_full_loads

# 7. Run dbt against ClickHouse (inside the dbt container)
docker compose exec dbt dbt debug
docker compose exec dbt dbt build
```

## Load strategy

- **Incremental loads** read the max watermark (`updated_at` or `created_at`) already in bronze and pull only
  newer rows from MySQL, in chunks of 10,000 rows. A changed customer or account lands again as a new version,
  which silver uses for SCD2 history.
- **Replace loads** empty and reload FX rates and risk tiers on every run, because they have no column that
  tracks changes.
- Bronze tables use plain `MergeTree`, tagged with `_batch_id` and `_loaded_at`.
- Timestamps are stored as `DateTime('UTC')`.
- Rows deleted in MySQL stay in bronze: watermark loads cannot see deletes.
