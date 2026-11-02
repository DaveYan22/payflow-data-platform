{{ config(order_by='AccountId') }}

WITH LatestRates AS (
  SELECT
    CurrencyCode,
    toNullable(argMax(UnitsPerUsd, RateDate)) AS UnitsPerUsd
  FROM
    {{ ref('fx_rates') }}
  GROUP BY
    CurrencyCode
)
SELECT
  ac.AccountId AS AccountId,
  ac.CustomerId AS CustomerId,
  ac.CurrencyCode AS CurrencyCode,
  ac.AccountStatus AS AccountStatus,
  ac.Balance AS Balance,
  {{ to_usd('ac.Balance', 'lr.UnitsPerUsd') }} AS BalanceUsd,
  ac.UpdatedAt AS UpdatedAt
FROM
  {{ ref('accounts_current') }} ac
  LEFT JOIN LatestRates lr ON lr.CurrencyCode = ac.CurrencyCode
