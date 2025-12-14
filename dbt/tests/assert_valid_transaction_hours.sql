SELECT *
FROM {{ ref('stg_transactions') }}
WHERE transaction_hour < 0 OR transaction_hour > 23
