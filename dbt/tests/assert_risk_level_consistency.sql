SELECT *
FROM {{ ref('mart_customer_risk_profile') }}
WHERE 
    (fraud_rate_pct >= 10 AND risk_level != 'HIGH')
    OR (fraud_rate_pct >= 5 AND fraud_rate_pct < 10 AND risk_level != 'MEDIUM')
    OR (fraud_rate_pct < 5 AND risk_level != 'LOW')
