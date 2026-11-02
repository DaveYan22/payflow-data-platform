{{ config(order_by='(TransactionDate, TransactionId)') }}

WITH FxRates AS (
  SELECT
    RateDate,
    CurrencyCode,
    toNullable(UnitsPerUsd) AS UnitsPerUsd
  FROM
    {{ ref('fx_rates') }}
),
Converted AS (
  SELECT
    st.TransactionId AS TransactionId,
    st.AccountId AS AccountId,
    ac.CustomerId AS CustomerId,
    st.TransactionType AS TransactionType,
    st.Direction AS Direction,
    st.Status AS Status,
    st.MerchantCategory AS MerchantCategory,
    st.CurrencyCode AS CurrencyCode,
    st.Amount AS Amount,
    {{ to_usd('st.Amount', 'fr.UnitsPerUsd') }} AS AmountUsd,
    {{ to_usd('st.SignedAmount', 'fr.UnitsPerUsd') }} AS SignedAmountUsd,
    st.Status = 'Completed'
    AND st.TransactionType != 'Transfer' AS IsInCashFlow,
    st.CreatedAt AS CreatedAt,
    toDate(st.CreatedAt) AS TransactionDate,
    st.CreatedDateVilnius AS CreatedDateVilnius
  FROM
    {{ ref('transactions') }} st
    INNER JOIN {{ ref('accounts_current') }} ac ON ac.AccountId = st.AccountId
    LEFT JOIN FxRates fr ON fr.RateDate = toDate(st.CreatedAt)
    AND fr.CurrencyCode = st.CurrencyCode
)
SELECT
  TransactionId,
  AccountId,
  CustomerId,
  TransactionType,
  Direction,
  Status,
  MerchantCategory,
  CurrencyCode,
  Amount,
  AmountUsd,
  IsInCashFlow,
  IF(
    IsInCashFlow,
    SignedAmountUsd,
    CAST(0 AS Nullable(Decimal(18, 6)))
  ) AS NetFlowUsd,
  CreatedAt,
  TransactionDate,
  CreatedDateVilnius
FROM
  Converted
