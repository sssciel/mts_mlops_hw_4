SELECT *
FROM {{ ref('mart_fraud_by_state') }}
WHERE fraud_transactions > total_transactions
