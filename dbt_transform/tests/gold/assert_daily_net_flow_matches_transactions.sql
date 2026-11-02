WITH TransactionTotals AS (
  SELECT
    AccountId,
    SUM(NetFlowUsd) AS TransactionNetFlowUsd
  FROM
    {{ ref('fact_transactions') }}
  GROUP BY
    AccountId
),
DailyTotals AS (
  SELECT
    AccountId,
    SUM(NetFlowUsd) AS DailyNetFlowUsd
  FROM
    {{ ref('fact_daily_balances') }}
  GROUP BY
    AccountId
)
SELECT
  tt.AccountId AS AccountId,
  tt.TransactionNetFlowUsd AS TransactionNetFlowUsd,
  dt.DailyNetFlowUsd AS DailyNetFlowUsd
FROM
  TransactionTotals tt
  LEFT JOIN DailyTotals dt ON dt.AccountId = tt.AccountId
WHERE
  COALESCE(tt.TransactionNetFlowUsd, 0) != COALESCE(dt.DailyNetFlowUsd, 0)
