WITH RowCounts AS (
  SELECT
    'fact_transactions' AS Model,
    (
      SELECT
        COUNT(*)
      FROM
        {{ ref('transactions') }}
    ) AS SilverRows,
    (
      SELECT
        COUNT(*)
      FROM
        {{ ref('fact_transactions') }}
    ) AS GoldRows
  UNION ALL
  SELECT
    'fact_app_events' AS Model,
    (
      SELECT
        COUNT(*)
      FROM
        {{ ref('app_events') }}
    ) AS SilverRows,
    (
      SELECT
        COUNT(*)
      FROM
        {{ ref('fact_app_events') }}
    ) AS GoldRows
)
SELECT
  Model,
  SilverRows,
  GoldRows
FROM
  RowCounts
WHERE
  SilverRows != GoldRows
