SELECT
  TransactionId,
  TransactionType,
  Status,
  IsInCashFlow,
  NetFlowUsd
FROM
  {{ ref('fact_transactions') }}
WHERE
  (
    IsInCashFlow
    AND (
      TransactionType = 'Transfer'
      OR Status != 'Completed'
    )
  )
  OR (
    NOT IsInCashFlow
    AND NetFlowUsd != 0
  )
