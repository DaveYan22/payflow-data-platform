{{ config(severity='warn') }}

SELECT
  MAX(_loaded_at) AS LastLoadedAt
FROM
  {{ source('bronze', 'raw_fx_rates') }}
HAVING
  LastLoadedAt < now() - INTERVAL 26 HOUR
