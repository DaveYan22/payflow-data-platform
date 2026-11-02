WITH PreviousDays AS (
  SELECT
    AccountId,
    BalanceDate,
    NetFlowUsd,
    RunningNetFlowUsd,
    lagInFrame(RunningNetFlowUsd) OVER (
      PARTITION BY AccountId
      ORDER BY BalanceDate
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS PreviousRunningNetFlowUsd
  FROM
    {{ ref('fact_daily_balances') }}
)
SELECT
  AccountId,
  BalanceDate,
  NetFlowUsd,
  RunningNetFlowUsd,
  PreviousRunningNetFlowUsd
FROM
  PreviousDays
WHERE
  RunningNetFlowUsd != PreviousRunningNetFlowUsd + NetFlowUsd
