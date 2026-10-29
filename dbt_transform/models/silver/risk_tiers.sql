{{ config(order_by='CountryCode') }}

SELECT
  UPPER(TRIM(country_code)) AS CountryCode,
  TRIM(region) AS Region,
  LOWER(TRIM(compliance_risk_level)) AS ComplianceRiskLevel
FROM
  {{ source('bronze', 'risk_tiers') }}
ORDER BY
  _loaded_at DESC
LIMIT 1 BY country_code
