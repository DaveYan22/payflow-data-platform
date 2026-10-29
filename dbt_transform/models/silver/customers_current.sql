{{ config(materialized='view') }}

SELECT
  CustomerId,
  FullName,
  Email,
  KycStatus,
  CountryCode,
  CreatedAt,
  ValidFrom AS UpdatedAt
FROM
  {{ ref('customers_history') }}
WHERE
  IsCurrent
