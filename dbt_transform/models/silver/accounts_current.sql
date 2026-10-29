{{ config(materialized='view') }}

SELECT
  AccountId,
  CustomerId,
  CurrencyCode,
  AccountStatus,
  Balance,
  ValidFrom AS UpdatedAt
FROM
  {{ ref('accounts_history') }}
WHERE
  IsCurrent
