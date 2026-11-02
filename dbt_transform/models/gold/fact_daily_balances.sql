{{ config(order_by='(AccountId, BalanceDate)') }}

WITH AccountStartDates AS (
  SELECT
    AccountId,
    MIN(StartDate) AS StartDate
  FROM
    (
      SELECT
        AccountId,
        toDate(MIN(ValidFrom)) AS StartDate
      FROM
        {{ ref('accounts_history') }}
      GROUP BY
        AccountId
      UNION ALL
      SELECT
        AccountId,
        MIN(TransactionDate) AS StartDate
      FROM
        {{ ref('fact_transactions') }}
      GROUP BY
        AccountId
    )
  GROUP BY
    AccountId
),
AccountDays AS (
  SELECT
    AccountId,
    arrayJoin(
      arrayMap(
        DayOffset -> StartDate + DayOffset,
        range(toUInt32(dateDiff('day', StartDate, toDate(now(), 'UTC')) + 1))
      )
    ) AS BalanceDate
  FROM
    AccountStartDates
),
AccountVersions AS (
  SELECT
    AccountId,
    ValidFrom,
    toNullable(Balance) AS ReportedBalance
  FROM
    {{ ref('accounts_history') }}
),
DailyReportedBalances AS (
  SELECT
    ad.AccountId AS AccountId,
    ad.BalanceDate AS BalanceDate,
    av.ReportedBalance AS ReportedBalance
  FROM
    (
      SELECT
        AccountId,
        BalanceDate,
        toDateTime(BalanceDate, 'UTC') + 86399 AS EndOfDay
      FROM
        AccountDays
    ) ad
    ASOF LEFT JOIN AccountVersions av ON av.AccountId = ad.AccountId
    AND ad.EndOfDay >= av.ValidFrom
),
DailyFlows AS (
  SELECT
    AccountId,
    TransactionDate AS BalanceDate,
    sumIf(NetFlowUsd, NetFlowUsd > 0) AS InflowUsd,
    sumIf(-NetFlowUsd, NetFlowUsd < 0) AS OutflowUsd,
    SUM(NetFlowUsd) AS DayNetFlowUsd
  FROM
    {{ ref('fact_transactions') }}
  WHERE
    IsInCashFlow
  GROUP BY
    AccountId,
    TransactionDate
),
FxRates AS (
  SELECT
    RateDate,
    CurrencyCode,
    toNullable(UnitsPerUsd) AS UnitsPerUsd
  FROM
    {{ ref('fx_rates') }}
),
Combined AS (
  SELECT
    drb.BalanceDate AS BalanceDate,
    drb.AccountId AS AccountId,
    ac.CustomerId AS CustomerId,
    ac.CurrencyCode AS CurrencyCode,
    CAST(COALESCE(df.InflowUsd, 0) AS Decimal(18, 6)) AS InflowUsd,
    CAST(COALESCE(df.OutflowUsd, 0) AS Decimal(18, 6)) AS OutflowUsd,
    CAST(COALESCE(df.DayNetFlowUsd, 0) AS Decimal(18, 6)) AS NetFlowUsd,
    drb.ReportedBalance AS ReportedBalance,
    {{ to_usd('drb.ReportedBalance', 'fr.UnitsPerUsd') }} AS ReportedBalanceUsd
  FROM
    DailyReportedBalances drb
    INNER JOIN {{ ref('accounts_current') }} ac ON ac.AccountId = drb.AccountId
    LEFT JOIN DailyFlows df ON df.AccountId = drb.AccountId
    AND df.BalanceDate = drb.BalanceDate
    LEFT JOIN FxRates fr ON fr.RateDate = drb.BalanceDate
    AND fr.CurrencyCode = ac.CurrencyCode
)
SELECT
  BalanceDate,
  AccountId,
  CustomerId,
  CurrencyCode,
  InflowUsd,
  OutflowUsd,
  NetFlowUsd,
  SUM(NetFlowUsd) OVER (
    PARTITION BY AccountId
    ORDER BY BalanceDate
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS RunningNetFlowUsd,
  ReportedBalance,
  ReportedBalanceUsd
FROM
  Combined
