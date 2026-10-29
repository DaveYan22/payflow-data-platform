{{ config(order_by='(CreatedAt, EventId)') }}

SELECT
  event_id AS EventId,
  customer_id AS CustomerId,
  LOWER(TRIM(event_type)) AS EventType,
  nullIf(JSONExtractString(event_payload, 'device'), '') AS Device,
  nullIf(JSONExtractString(event_payload, 'app_version'), '') AS AppVersion,
  JSONExtract(event_payload, 'session_duration_sec', 'Nullable(UInt32)') AS SessionDurationSec,
  JSONExtract(event_payload, 'flagged_for_review', 'Nullable(Bool)') AS FlaggedForReview,
  event_payload AS EventPayload,
  client_ip AS ClientIp,
  toDateTime(created_at, 'UTC') AS CreatedAt,
  toDate(toTimeZone(toDateTime(created_at, 'UTC'), 'Europe/Vilnius')) AS CreatedDateVilnius
FROM
  {{ source('bronze', 'raw_app_events') }}
ORDER BY
  _loaded_at DESC
LIMIT 1 BY event_id
