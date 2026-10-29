{{ config(order_by='(RateDate, CurrencyCode)') }}

WITH SourceRates AS (
  SELECT
    rate_date AS RateDate,
    CAST(UPPER(TRIM(target_currency)) AS String) AS CurrencyCode,
    CAST(exchange_rate AS Decimal(18, 6)) AS UnitsPerUsd
  FROM
    {{ source('bronze', 'raw_fx_rates') }}
  WHERE
    UPPER(TRIM(target_currency)) != 'USD'
),
UsdRates AS (
  SELECT DISTINCT
    RateDate,
    'USD' AS CurrencyCode,
    CAST(1 AS Decimal(18, 6)) AS UnitsPerUsd
  FROM
    SourceRates
)
SELECT
  RateDate,
  CurrencyCode,
  UnitsPerUsd
FROM
  SourceRates
UNION ALL
SELECT
  RateDate,
  CurrencyCode,
  UnitsPerUsd
FROM
  UsdRates
