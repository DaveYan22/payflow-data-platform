SELECT
  'customer' AS Entity,
  CustomerId AS EntityId,
  ValidFrom,
  ValidTo
FROM
  {{ ref('customers_history') }}
WHERE
  ValidFrom >= ValidTo
UNION ALL
SELECT
  'account' AS Entity,
  AccountId AS EntityId,
  ValidFrom,
  ValidTo
FROM
  {{ ref('accounts_history') }}
WHERE
  ValidFrom >= ValidTo
