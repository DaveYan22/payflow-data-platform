# Bronze change-tracking tests

Manual tests of how changes in the MySQL source reach the ClickHouse bronze layer. Each test updates one row
in MySQL, runs the Airflow load, and checks what bronze recorded.

Run on 2026-10-29 against `core_banking_prod` (MySQL 8, Windows host) and `clickhouse_dwh` (ClickHouse 25.8).

## Summary

| # | Table | Change in MySQL | Load | Captured in bronze? | History kept? |
|---|---|---|---|---|---|
| 1 | `raw_customers` | KYC status Verified → Pending | `extract_incremental_loads` | ✅ Yes | ✅ New row per version |
| 2 | `raw_customers` | Same status again, only `updated_at` changed | `extract_incremental_loads` | ✅ Yes | ⚠️ Extra version with no real change |
| 3 | `raw_customers` | Country NLD → DEU | `extract_incremental_loads` | ✅ Yes | ✅ New row per version |
| 4 | `raw_accounts` | Status Active → Suspended, balance 3162.81 → 2500.00 | `extract_incremental_loads` | ✅ Yes | ✅ New row per version |
| 5 | `raw_transactions` | Status Pending → Completed | `extract_incremental_loads` | ❌ No | ❌ Bronze stays Pending |
| 6 | `raw_app_events` | Payload device Android → iOS | `extract_incremental_loads` | ❌ No (by design) | ❌ |
| 7 | `raw_fx_rates` | CAD rate on 2026-10-29: 1.338868 → 1.4 | `extract_full_loads` | ✅ Yes | ❌ Old value replaced |
| 8 | `risk_tiers` (Excel) | SGP medium → high | `extract_full_loads` | Not run | Expected: replaced, no history |

## How to run

1. Run the `UPDATE` in MySQL Workbench. Always set `updated_at = UTC_TIMESTAMP()` (see [Findings](#findings)).
2. Load it into ClickHouse, or wait for the schedule:
   ```powershell
   docker exec airflow airflow dags trigger extract_incremental_loads
   docker exec airflow airflow dags trigger extract_full_loads
   ```
3. Run the check query in ClickHouse (http://localhost:8123/play).

## Tests

### 1-3. Customer: KYC status and country change

```sql
UPDATE core_banking_prod.raw_customers
SET
  kyc_status = 'Pending',
  updated_at = UTC_TIMESTAMP()
WHERE
  customer_id = 'cust_000a5f52d6';
```

The same `UPDATE` was run a second time (test 2), then the country was changed (test 3):

```sql
UPDATE core_banking_prod.raw_customers
SET
  country_code = 'DEU',
  updated_at = UTC_TIMESTAMP()
WHERE
  customer_id = 'cust_000a5f52d6';
```

Check:

```sql
SELECT
  kyc_status AS KycStatus,
  country_code AS CountryCode,
  updated_at AS UpdatedAt,
  _loaded_at AS LoadedAt,
  _batch_id AS BatchId
FROM
  bronze.raw_customers
WHERE
  customer_id = 'cust_000a5f52d6'
ORDER BY
  updated_at
```

Result: MySQL has 1 row (Pending, DEU). Bronze has 4:

| KycStatus | CountryCode | UpdatedAt | LoadedAt | BatchId |
|---|---|---|---|---|
| Verified | NLD | 2026-05-20 07:47:32 | 2026-10-27 15:08:07 | incr_check_1 |
| Pending | NLD | 2026-10-29 15:11:47 | 2026-10-29 15:12:06 | scd2_test_191147 |
| Pending | NLD | 2026-10-29 15:14:57 | 2026-10-29 15:16:27 | manual__2026-10-29T15:16:13 |
| Pending | DEU | 2026-10-29 15:35:26 | 2026-10-29 16:00:15 | scheduled__2026-10-29T15:00:00 |

Rows 2 and 3 differ only in `updated_at`. Silver must merge consecutive versions with no change in the tracked
columns, so the SCD2 history has 3 real versions: Verified/NLD, Pending/NLD, Pending/DEU.

### 4. Account: status and balance change

```sql
UPDATE core_banking_prod.raw_accounts
SET
  account_status = 'Suspended',
  balance = 2500.00,
  updated_at = UTC_TIMESTAMP()
WHERE
  account_id = 'acc_0658c7fb45';
```

Check:

```sql
SELECT
  account_status AS AccountStatus,
  balance AS Balance,
  updated_at AS UpdatedAt,
  _loaded_at AS LoadedAt
FROM
  bronze.raw_accounts
WHERE
  account_id = 'acc_0658c7fb45'
ORDER BY
  updated_at
```

Result:

| AccountStatus | Balance | UpdatedAt | LoadedAt |
|---|---|---|---|
| Active | 3162.81 | 2026-10-05 12:38:45 | 2026-10-27 15:08:14 |
| Suspended | 2500.00 | 2026-10-29 15:36:03 | 2026-10-29 16:00:22 |

Picked up by the regular hourly run, no manual trigger needed.

**Double-counting trap:** the customer has 3 accounts, so filtering bronze by customer returns 4 rows. Summing
them counts the updated account twice:

```sql
SELECT
  SUM(balance) AS TotalBalance
FROM
  bronze.raw_accounts
WHERE
  customer_id = 'cust_000a5f52d6'
```

| Source | Total balance |
|---|---|
| MySQL (current state) | 13,464.20 |
| Bronze (all versions) | 16,627.01 ❌ |

Balances must be summed from the latest version per account (silver `accounts_current`), never from bronze.

### 5. Transaction: Pending → Completed

```sql
UPDATE core_banking_prod.raw_transactions
SET
  status = 'Completed'
WHERE
  transaction_id = 'tx_3c93ed9908d0';
```

Result: MySQL shows `Completed`, bronze still shows `Pending` (loaded 2026-10-27, never reloaded).
Transactions load by `created_at`, which an update does not change, and the table has no `updated_at` column.

### 6. App event: payload change

```sql
UPDATE core_banking_prod.raw_app_events
SET
  event_payload = JSON_SET(event_payload, '$.device', 'iOS')
WHERE
  event_id = 'evt_afcd9b7d7d37';
```

Result: MySQL shows `iOS`, bronze still shows `Android`. Accepted: app events are an append-only log and should
never change after they are written.

### 7. FX rate correction

```sql
UPDATE core_banking_prod.raw_fx_rates
SET
  exchange_rate = 1.400000
WHERE
  rate_date = '2026-10-29'
  AND target_currency = 'CAD';
```

Check:

```sql
SELECT
  rate_date AS RateDate,
  target_currency AS TargetCurrency,
  exchange_rate AS ExchangeRate,
  toTypeName(exchange_rate) AS ColumnType
FROM
  bronze.raw_fx_rates
WHERE
  rate_date = '2026-10-29'
  AND target_currency = 'CAD'
```

Result: `1.4`, `Decimal(12, 6)`. The table is replaced on every full load, so the previous rate is not kept.

### 8. Risk tier change (Excel)

Change `SGP` from `medium` to `high` in `source_data/country_risk_tiers.xlsx`, save, run `extract_full_loads`.

```sql
SELECT
  country_code AS CountryCode,
  compliance_risk_level AS ComplianceRiskLevel
FROM
  bronze.risk_tiers
WHERE
  country_code = 'SGP'
```

Not run yet: the file was not edited. Expected: `high`, with no history of the old level.

## Findings

1. **Customers and accounts are tracked correctly.** Every update with a new `updated_at` lands as a new row in
   bronze; old versions are never changed. Bronze is a change log, silver turns it into SCD2.
2. **Updates that change nothing still create a version** (test 2). Silver must merge consecutive identical
   versions.
3. **Transaction status changes are lost** (test 5). Real transactions do change (Pending → Completed/Failed),
   so `raw_transactions` needs an `updated_at` column and the incremental load should use it.
4. **FX rates and risk tiers keep no history** (tests 7-8). Fine for corrections; if past values matter, bronze
   would need to append snapshots instead of replacing.
5. **Bronze must never be summed directly for customers or accounts** (test 4): use the current version.
6. **Timezone:** `updated_at` has `ON UPDATE CURRENT_TIMESTAMP`, which uses the MySQL server time zone
   (Europe/Vilnius, UTC+2) while the pipeline uses UTC. An update that does not set `updated_at` explicitly moves
   the watermark 2 hours ahead, and new UTC rows are skipped until real time catches up. Fix: run MySQL on UTC
   (`SET PERSIST time_zone = '+00:00';`).
7. **Bug found and fixed:** bronze `raw_fx_rates.exchange_rate` was `Decimal(10, 4)` and cut MySQL's 6 decimals
   (1.338868 became 1.3388). Changed to `Decimal(12, 6)`; the table was dropped and reloaded.

## Follow-ups

- [ ] Add `updated_at` to `raw_transactions` and load transactions by it (finding 3).
- [ ] Set MySQL `time_zone` to UTC (finding 6).
- [ ] Lookback window of a few minutes on the incremental load for late-committed rows.
- [ ] Silver: SCD2 with merging of unchanged versions, `*_current` views (findings 2, 5).
- [ ] Run test 8 (risk tiers Excel).
