{{ config(order_by='(AccountId, ValidFrom)') }}

WITH AccountVersions AS (
  SELECT
    account_id AS AccountId,
    customer_id AS CustomerId,
    UPPER(TRIM(currency_code)) AS CurrencyCode,
    initcap(LOWER(TRIM(account_status))) AS AccountStatus,
    balance AS Balance,
    toDateTime(updated_at, 'UTC') AS UpdatedAt
  FROM
    {{ source('bronze', 'raw_accounts') }}
  ORDER BY
    _loaded_at DESC
  LIMIT 1 BY account_id, updated_at
),
ComparedVersions AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY AccountId
      ORDER BY UpdatedAt
    ) AS VersionNumber,
    lagInFrame(tuple(CustomerId, CurrencyCode, AccountStatus, Balance)) OVER (
      PARTITION BY AccountId
      ORDER BY UpdatedAt
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS PreviousAttributes
  FROM
    AccountVersions
),
RealChanges AS (
  SELECT
    *
  FROM
    ComparedVersions
  WHERE
    VersionNumber = 1
    OR tuple(CustomerId, CurrencyCode, AccountStatus, Balance) != PreviousAttributes
),
VersionRanges AS (
  SELECT
    *,
    leadInFrame(UpdatedAt, 1, toDateTime('2100-01-01 00:00:00', 'UTC')) OVER (
      PARTITION BY AccountId
      ORDER BY UpdatedAt
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS NextValidFrom
  FROM
    RealChanges
)
SELECT
  AccountId,
  CustomerId,
  CurrencyCode,
  AccountStatus,
  Balance,
  UpdatedAt AS ValidFrom,
  NextValidFrom AS ValidTo,
  NextValidFrom = toDateTime('2100-01-01 00:00:00', 'UTC') AS IsCurrent
FROM
  VersionRanges
