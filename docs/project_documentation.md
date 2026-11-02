# PayFlow Data Platform: Project Documentation

*Last updated: 2026-11-02*

The pipeline runs end to end from MySQL to gold, and every transaction converts to USD. Superset dashboards are the main work left.

## Current status

| Check | Result |
|---|---|
| Silver and gold refresh | Automatic after each incremental load |
| dbt build | 82 of 82 passed (13 models, 69 tests) |
| Transactions missing an FX rate | 0 of 2,435, down from 1,273 |
| FX rates in MySQL and bronze | 204 rows, 27 Aug to 2 Nov |
| Gold layer | Built on `feature/gold-layer`, not merged yet |
| Superset dashboards | Not started |

## Architecture

```text
Generator (daily 01:00 UTC)
    |
    v
MySQL (5 tables) ---> Incremental load (hourly) -------+
    |                                                  |
    +---------------> Full load (daily 02:00 UTC) -----+
                          ^                            |
Excel (risk tiers) -------+                            v
                                                Bronze (6 tables)
                                                       |
                                                       | dbt build
                                                       v
                                                Silver (8 models)
                                                       |
                                                       | dbt build
                                                       v
                                                Gold (5 models)
                                                       :
                                                       : planned
                                                       v
                                                    Superset
```

| Layer | Built by | Purpose |
|---|---|---|
| Bronze | Airflow | Raw copy of every loaded version, with no transformation |
| Silver | dbt | Cleaning, deduplication and SCD2 history |
| Gold | dbt | Business-ready star schemas in USD |

## What we built, in order

1. **Stack:** Docker Compose with ClickHouse, Airflow, dbt and Superset, with MySQL on the Windows host.
2. **Ingestion:** `extract_incremental_loads` (hourly) and `extract_full_loads` (daily) load 6 bronze tables.
3. **Secrets and repo:** logins moved to `.env`, then the first push to GitHub.
4. **Daily data:** `generate_daily_data` grows MySQL by 3 to 5% and triggers the incremental load.
5. **Change-tracking tests:** these found the FX precision and transaction status bugs.
6. **Hardening:** transactions load by `updated_at`, with a 10-minute lookback and MySQL on UTC.
7. **Silver:** 8 dbt models with SCD2 for customers and accounts, cleaned facts and 28 tests.
8. **Automation:** `dbt_build` refreshes silver after each incremental load.
9. **FX gap:** the full load was unpaused and 93 older rates were added, so all transactions can convert to USD.
10. **Stable FX rates:** the generator no longer rewrites existing rates, so past USD values stay the same.
11. **Gold:** 5 models in 3 star schemas, with tests sorted into bronze, silver and gold folders.

## Key decisions

| Decision | Why |
|---|---|
| ELT: raw data in bronze, transformations in dbt | Silver can always be rebuilt from bronze |
| Bronze is append-only, loaded by watermark | Reruns add no duplicates, and a changed row lands as a new version |
| 10-minute lookback, skipping loaded rows | Catches late-committed rows without duplicates |
| FX rates and risk tiers fully replaced on each load | No change-tracking column, and past FX rates can be rewritten |
| SCD2 built from bronze, not dbt snapshots | Bronze already holds every loaded version, so a rebuild cannot drift |
| `*_current` models are views over `*_history` | One source of truth, and protection against double counting |
| Silver models are `table` or `view`, not incremental | The full build takes about 3 seconds, so incremental adds risk for no gain yet |
| `dbt_build` is a task in the incremental DAG | Runs only after all tables load, with one place to check |
| dbt has its own venv inside Airflow | Its packages cannot clash with Airflow's |
| Separate MySQL logins, everything in UTC | The pipeline does not need root, and watermarks share one clock |

## Issues and fixes

| Issue | How it showed up | Fix |
|---|---|---|
| Watermark read as text, so runs restarted from 1970 | Incremental tables doubled | Read it as a typed value with `client.query()` |
| Full snapshots doubled customers and accounts | 332 rows vs 166 | Moved both to incremental by `updated_at` |
| New rows dated before the watermark would be skipped | Code review | Generator dates new rows after the latest ones |
| Country codes did not match (ISO-2 vs ISO-3) | Code review | Rebuilt the risk tiers file with the generator's countries |
| Local time on Windows, UTC in the container | DAG review | Generator always uses UTC |
| FX rates cut to 4 decimals in bronze | Change-tracking test | `Decimal(12, 6)`, table reloaded |
| Transaction status changes never reached bronze | Change-tracking test | Added `updated_at` and loaded by it |
| `updated_at` filled in server local time | MySQL schema check | Set MySQL to UTC |
| Passwords in code, no `.gitignore` | Search before the public push | Moved to `.env`, added `.gitignore` |
| `README.md` had a UTF-16 line | Read before first commit | Rewrote as UTF-8 |
| `DBT_PROJECT_DIR` pointed `dbt debug` at the Airflow path | Running `dbt debug` | Overridden in the dbt container |
| Airflow page would not open | Stale PID file blocked the web server | Deleted the file and restarted the web server by hand; permanent fix still open |
| New rows never reached silver | Silver last built 4 days before bronze | Added `dbt_build` to the incremental DAG |
| 1,273 transactions had no FX rate | Comparing FX and transaction date ranges | Unpaused the full load (258) and inserted 93 rates for 27 Aug to 26 Sep (1,015) |
| Generator rewrote the last 30 days of FX rates on every run | Reading the generator code | Existing rates are now kept, and only missing dates are added |
| `fact_daily_balances` failed with a nested aggregate error | First gold build | Renamed the `DailyFlows` alias that clashed with its source column |

**Lessons:**

- Check a side effect before saying a cancelled command changed nothing, and verify facts before stating them.
- Test DAGs by running them twice before first delivery, and run migrations before their code goes live.
- Check existing tools and local images first, to avoid extra services and duplicate scripts.

## Gold layer

**Approach: 3 star schemas that share 2 dimensions, and no snowflakes.** This is known as a galaxy schema. It has 5 models in total: 2 dimensions and 3 fact tables.

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

### What each star does

| Star | Fact table | One row per | Dimensions | Answers |
|---|---|---|---|---|
| Transactions | `fact_transactions` | Transaction | `dim_accounts`, `dim_customers` | Transaction volume and net cash flow |
| Daily balances | `fact_daily_balances` | Account per day | `dim_accounts`, `dim_customers` | Daily inflow and outflow, running net flow, balances over time |
| App activity | `fact_app_events` | App event | `dim_customers` | Logins, failed PIN attempts, frozen cards and other app behaviour |

### Models

| Model | Type | Main columns | Built from |
|---|---|---|---|
| `dim_customers` | Dimension | `CustomerId`, `FullName`, `Email`, `KycStatus`, `CountryCode`, `Region`, `ComplianceRiskLevel` | `customers_current`, `risk_tiers` |
| `dim_accounts` | Dimension | `AccountId`, `CustomerId`, `CurrencyCode`, `AccountStatus`, `Balance`, `BalanceUsd` | `accounts_current`, `fx_rates` |
| `fact_transactions` | Fact | `TransactionId`, `AccountId`, `CustomerId`, `TransactionType`, `Status`, `CurrencyCode`, `Amount`, `AmountUsd`, `IsInCashFlow`, `NetFlowUsd`, `CreatedAt` | `transactions`, `accounts_current`, `fx_rates` |
| `fact_daily_balances` | Fact | `BalanceDate`, `AccountId`, `CustomerId`, `InflowUsd`, `OutflowUsd`, `NetFlowUsd`, `RunningNetFlowUsd`, `ReportedBalance`, `ReportedBalanceUsd` | `fact_transactions`, `accounts_history`, `fx_rates` |
| `fact_app_events` | Fact | `EventId`, `CustomerId`, `EventType`, `Device`, `AppVersion`, `SessionDurationSec`, `FlaggedForReview`, `CreatedAt` | `app_events` |

### Design choices

| Choice | Decision | Why |
|---|---|---|
| Star or snowflake | Star: facts carry both `AccountId` and `CustomerId` | One join per dimension, and simpler Superset datasets |
| `fact_events` from the original plan | Split into `fact_transactions` and `fact_app_events` | They have different grains and columns, so combining them leaves many empty columns |
| `dim_accounts` | Added | Transactions and balances are recorded per account |
| Dimension history | Current values only | Simple joins; past states stay in the silver `*_history` models |
| Date dimension | None for now | Superset handles time grains, and this project has no fiscal calendar |
| Materialization | `table` for all 5 | Fast dashboard reads, and full rebuilds are cheap at this size |
| USD conversion | `AmountUsd = Amount / UnitsPerUsd`, using the rate for the transaction's UTC date | Matches the FX coverage check |
| Missing rate | `AmountUsd` is `NULL`, never 0 | A missing rate must stay visible rather than understate totals |
| `Transfer` | Kept in volume, excluded from net cash flow | All 579 rows are plain `Transfer` with no direction, so counting them as money out would be a guess |
| Net cash flow status | `Completed` only; the fact table keeps every status | Failed and pending transactions did not move money, but are still worth analysing |
| Running balance | Two separate columns: `RunningNetFlowUsd` and `ReportedBalanceUsd` | Transactions do not update `Balance` in the source (203 accounts, 204 versions), so the two will not match |
| `fact_daily_balances` dates | From the account's first transaction or version | `ReportedBalance` is `NULL` before its first version, so the balance is never guessed |

### Dashboards

| Dashboard | Source |
|---|---|
| Daily transaction volume | `fact_transactions` |
| Net cash flow | `fact_transactions` or `fact_daily_balances` |
| Risk profiles | `dim_customers`, joined to the facts |
| FX converter | `silver.fx_rates`, as it is a reference lookup rather than part of a star |

## Tests

All 69 tests run as part of `dbt build`. Custom SQL tests are in `dbt_transform/tests/<layer>/`, and column tests are in each layer's `schema.yml`.

| Layer | Tests | What they check |
|---|---|---|
| Bronze | 1 | FX rates were loaded in the last 26 hours (warning only) |
| Silver | 38 | Keys, allowed values, links, SCD2 ranges, FX coverage and uniqueness, transaction currency, bronze IDs reach silver |
| Gold | 30 | Keys, links between facts and dimensions, row counts match silver, USD values present, cash flow rules, one row per account per day, daily totals match transactions, running total adds up |

The FX coverage and USD tests ignore today's rows, because today's rate arrives with the 02:00 UTC full load.

## Open items

**Next:**

- [ ] Push `feature/gold-layer`, open a PR and merge it
- [ ] Build the Superset dashboards

**Clean-up:**

- [ ] Change the MySQL root password, which was hardcoded in two files and passed through chat
- [ ] Make `banking_user` read-only
- [ ] Add the PID file removal to Airflow's startup command
- [ ] Test the dbt install line on the next Airflow recreate, which also wipes run history and pauses DAGs
- [ ] Update local `main` and delete merged branches
- [ ] Update the outdated `README.md` and test document findings
- [ ] Run the risk tiers test (SGP medium to high)
- [ ] Decide whether to keep `mock_data/generate_mock_data.py`

## Known limits

- Rows deleted in MySQL stay in bronze.
- Several changes between two loads keep only the last state, and changes that skip `updated_at` are not seen.
- For up to about 2 hours each night, the newest transactions have no FX rate until the 02:00 UTC full load and the next dbt build.
- Airflow uses SQLite in standalone mode, so recreating the container loses run history and pauses the DAGs.
- `raw_accounts` has no `created_at`, and an account's first version comes from `updated_at`. So 1,516 of 2,435 transactions predate their account's first version, and 62.5% of `fact_daily_balances` rows have no `ReportedBalance`. `RunningNetFlowUsd` is not affected.
- `dim_customers` currently has only low and medium risk customers (109 and 14), so a risk dashboard will show no high-risk group yet.
