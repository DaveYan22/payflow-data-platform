SELECT
  'fact_transactions' AS Model,
  TransactionId AS RowKey,
  TransactionDate AS RowDate
FROM
  {{ ref('fact_transactions') }}
WHERE
  AmountUsd IS NULL
  AND TransactionDate < toDate(now(), 'UTC')
UNION ALL
SELECT
  'fact_daily_balances' AS Model,
  AccountId AS RowKey,
  BalanceDate AS RowDate
FROM
  {{ ref('fact_daily_balances') }}
WHERE
  ReportedBalance IS NOT NULL
  AND ReportedBalanceUsd IS NULL
  AND BalanceDate < toDate(now(), 'UTC')
