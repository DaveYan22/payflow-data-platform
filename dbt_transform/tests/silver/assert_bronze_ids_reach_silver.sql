SELECT
  'customer' AS Entity,
  bc.customer_id AS EntityId
FROM
  {{ source('bronze', 'raw_customers') }} bc
  LEFT ANTI JOIN {{ ref('customers_current') }} cc ON cc.CustomerId = bc.customer_id
UNION ALL
SELECT
  'account' AS Entity,
  ba.account_id AS EntityId
FROM
  {{ source('bronze', 'raw_accounts') }} ba
  LEFT ANTI JOIN {{ ref('accounts_current') }} ac ON ac.AccountId = ba.account_id
UNION ALL
SELECT
  'transaction' AS Entity,
  bt.transaction_id AS EntityId
FROM
  {{ source('bronze', 'raw_transactions') }} bt
  LEFT ANTI JOIN {{ ref('transactions') }} st ON st.TransactionId = bt.transaction_id
UNION ALL
SELECT
  'event' AS Entity,
  be.event_id AS EntityId
FROM
  {{ source('bronze', 'raw_app_events') }} be
  LEFT ANTI JOIN {{ ref('app_events') }} ae ON ae.EventId = be.event_id
