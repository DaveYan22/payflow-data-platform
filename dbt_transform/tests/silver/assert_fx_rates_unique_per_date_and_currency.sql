SELECT
  RateDate,
  CurrencyCode,
  COUNT(*) AS RateCount
FROM
  {{ ref('fx_rates') }}
GROUP BY
  RateDate,
  CurrencyCode
HAVING
  RateCount > 1
