SELECT
  'customer' AS Entity,
  CustomerId AS EntityId,
  SUM(IsCurrent) AS CurrentVersions
FROM
  {{ ref('customers_history') }}
GROUP BY
  CustomerId
HAVING
  CurrentVersions != 1
UNION ALL
SELECT
  'account' AS Entity,
  AccountId AS EntityId,
  SUM(IsCurrent) AS CurrentVersions
FROM
  {{ ref('accounts_history') }}
GROUP BY
  AccountId
HAVING
  CurrentVersions != 1
