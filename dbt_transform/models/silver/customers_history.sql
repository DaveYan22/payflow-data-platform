{{ config(order_by='(CustomerId, ValidFrom)') }}

WITH CustomerVersions AS (
  SELECT
    customer_id AS CustomerId,
    TRIM(full_name) AS FullName,
    LOWER(TRIM(email)) AS Email,
    initcap(LOWER(TRIM(kyc_status))) AS KycStatus,
    UPPER(TRIM(country_code)) AS CountryCode,
    toDateTime(created_at, 'UTC') AS CreatedAt,
    toDateTime(updated_at, 'UTC') AS UpdatedAt
  FROM
    {{ source('bronze', 'raw_customers') }}
  ORDER BY
    _loaded_at DESC
  LIMIT 1 BY customer_id, updated_at
),
ComparedVersions AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY CustomerId
      ORDER BY UpdatedAt
    ) AS VersionNumber,
    lagInFrame(tuple(FullName, Email, KycStatus, CountryCode)) OVER (
      PARTITION BY CustomerId
      ORDER BY UpdatedAt
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS PreviousAttributes
  FROM
    CustomerVersions
),
RealChanges AS (
  SELECT
    *
  FROM
    ComparedVersions
  WHERE
    VersionNumber = 1
    OR tuple(FullName, Email, KycStatus, CountryCode) != PreviousAttributes
),
VersionRanges AS (
  SELECT
    *,
    leadInFrame(UpdatedAt, 1, toDateTime('2100-01-01 00:00:00', 'UTC')) OVER (
      PARTITION BY CustomerId
      ORDER BY UpdatedAt
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS NextValidFrom
  FROM
    RealChanges
)
SELECT
  CustomerId,
  FullName,
  Email,
  KycStatus,
  CountryCode,
  CreatedAt,
  UpdatedAt AS ValidFrom,
  NextValidFrom AS ValidTo,
  NextValidFrom = toDateTime('2100-01-01 00:00:00', 'UTC') AS IsCurrent
FROM
  VersionRanges
