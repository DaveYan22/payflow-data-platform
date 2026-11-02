{{ config(order_by='(EventDate, EventId)') }}

SELECT
  EventId,
  CustomerId,
  EventType,
  Device,
  AppVersion,
  SessionDurationSec,
  FlaggedForReview,
  CreatedAt,
  toDate(CreatedAt) AS EventDate,
  CreatedDateVilnius
FROM
  {{ ref('app_events') }}
