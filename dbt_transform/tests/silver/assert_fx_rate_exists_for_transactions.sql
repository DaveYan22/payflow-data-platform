SELECT
  st.TransactionId AS TransactionId,
  st.CurrencyCode AS CurrencyCode,
  toDate(st.CreatedAt) AS TransactionDate
FROM
  {{ ref('transactions') }} st
  LEFT ANTI JOIN {{ ref('fx_rates') }} fr ON fr.RateDate = toDate(st.CreatedAt)
  AND fr.CurrencyCode = st.CurrencyCode
WHERE
  toDate(st.CreatedAt) < toDate(now(), 'UTC')
