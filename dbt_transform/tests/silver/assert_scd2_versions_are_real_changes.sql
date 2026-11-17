WITH CustomerVersions AS (
  SELECT
    CustomerId AS EntityId,
    ValidFrom,
    ROW_NUMBER() OVER (
      PARTITION BY CustomerId
      ORDER BY ValidFrom
    ) AS VersionNumber,
    tuple(FullName, Email, KycStatus, CountryCode) AS Attributes,
    lagInFrame(tuple(FullName, Email, KycStatus, CountryCode)) OVER (
      PARTITION BY CustomerId
      ORDER BY ValidFrom
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS PreviousAttributes
  FROM
    {{ ref('customers_history') }}
),
AccountVersions AS (
  SELECT
    AccountId AS EntityId,
    ValidFrom,
    ROW_NUMBER() OVER (
      PARTITION BY AccountId
      ORDER BY ValidFrom
    ) AS VersionNumber,
    tuple(CustomerId, CurrencyCode, AccountStatus, Balance) AS Attributes,
    lagInFrame(tuple(CustomerId, CurrencyCode, AccountStatus, Balance)) OVER (
      PARTITION BY AccountId
      ORDER BY ValidFrom
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS PreviousAttributes
  FROM
    {{ ref('accounts_history') }}
)
SELECT
  'customer' AS Entity,
  EntityId,
  ValidFrom
FROM
  CustomerVersions
WHERE
  VersionNumber > 1
  AND Attributes = PreviousAttributes
UNION ALL
SELECT
  'account' AS Entity,
  EntityId,
  ValidFrom
FROM
  AccountVersions
WHERE
  VersionNumber > 1
  AND Attributes = PreviousAttributes
