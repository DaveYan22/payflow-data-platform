{{ config(order_by='(CreatedAt, TransactionId)') }}

WITH LatestVersions AS (
  SELECT
    transaction_id AS TransactionId,
    account_id AS AccountId,
    initcap(replaceAll(LOWER(TRIM(transaction_type)), '_', ' ')) AS TransactionType,
    ABS(amount) AS Amount,
    UPPER(TRIM(currency_code)) AS CurrencyCode,
    IF(
      TRIM(merchant_category) IN ('', 'N/A'),
      NULL,
      initcap(LOWER(TRIM(merchant_category)))
    ) AS MerchantCategory,
    initcap(LOWER(TRIM(status))) AS Status,
    toDateTime(created_at, 'UTC') AS CreatedAt,
    toDateTime(updated_at, 'UTC') AS UpdatedAt
  FROM
    {{ source('bronze', 'raw_transactions') }}
  ORDER BY
    updated_at DESC,
    _loaded_at DESC
  LIMIT 1 BY transaction_id
)
SELECT
  TransactionId,
  AccountId,
  TransactionType,
  IF(
    TransactionType IN ('Deposit', 'Transfer In'),
    'in',
    'out'
  ) AS Direction,
  Amount,
  IF(
    TransactionType IN ('Deposit', 'Transfer In'),
    Amount,
    -Amount
  ) AS SignedAmount,
  CurrencyCode,
  MerchantCategory,
  Status,
  CreatedAt,
  UpdatedAt,
  toDate(toTimeZone(CreatedAt, 'Europe/Vilnius')) AS CreatedDateVilnius
FROM
  LatestVersions
