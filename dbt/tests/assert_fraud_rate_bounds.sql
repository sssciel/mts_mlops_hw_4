SELECT *
FROM {{ ref('mart_fraud_by_category') }}
WHERE fraud_rate > 100 OR fraud_rate < 0
