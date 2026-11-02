SELECT
  AccountId,
  BalanceDate,
  COUNT(*) AS RowCount
FROM
  {{ ref('fact_daily_balances') }}
GROUP BY
  AccountId,
  BalanceDate
HAVING
  RowCount > 1
