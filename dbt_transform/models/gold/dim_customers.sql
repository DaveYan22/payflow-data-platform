{{ config(order_by='CustomerId') }}

WITH RiskTiers AS (
  SELECT
    CountryCode,
    toNullable(Region) AS Region,
    toNullable(ComplianceRiskLevel) AS ComplianceRiskLevel
  FROM
    {{ ref('risk_tiers') }}
)
SELECT
  cc.CustomerId AS CustomerId,
  cc.FullName AS FullName,
  cc.Email AS Email,
  cc.KycStatus AS KycStatus,
  cc.CountryCode AS CountryCode,
  rt.Region AS Region,
  rt.ComplianceRiskLevel AS ComplianceRiskLevel,
  cc.CreatedAt AS CreatedAt,
  cc.UpdatedAt AS UpdatedAt
FROM
  {{ ref('customers_current') }} cc
  LEFT JOIN RiskTiers rt ON rt.CountryCode = cc.CountryCode
