SELECT
  st.TransactionId AS TransactionId,
  st.CurrencyCode AS TransactionCurrency,
  ac.CurrencyCode AS AccountCurrency
FROM
  {{ ref('transactions') }} st
  INNER JOIN {{ ref('accounts_current') }} ac ON ac.AccountId = st.AccountId
WHERE
  st.CurrencyCode != ac.CurrencyCode
