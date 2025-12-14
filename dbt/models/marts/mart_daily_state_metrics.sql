{{
    config(
        materialized='table'
    )
}}

WITH daily_state_agg AS (
    SELECT
        transaction_date,
        state,
        
        -- Количественные метрики
        count(*) AS total_transactions,
        countIf(is_fraud = 1) AS fraud_transactions,
        countIf(is_large_transaction = 1) AS large_transactions,
        
        -- Суммовые метрики
        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount,
        
        -- Средние значения
        avg(amount) AS avg_amount,
        avgIf(amount, is_fraud = 1) AS avg_fraud_amount,
        
        -- Перцентили
        quantile(0.5)(amount) AS median_amount,
        quantile(0.95)(amount) AS p95_amount,
        quantile(0.99)(amount) AS p99_amount,
        
        -- Уникальные клиенты и мерчанты
        uniqExact(customer_id) AS unique_customers,
        uniqExact(merchant_name) AS unique_merchants,
        
        -- Среднее расстояние
        avg(distance_km) AS avg_distance_km

    FROM {{ ref('stg_transactions') }}
    GROUP BY transaction_date, state
)

SELECT
    transaction_date,
    state,
    total_transactions,
    fraud_transactions,
    large_transactions,
    
    -- Доли
    round(large_transactions / total_transactions * 100, 2) AS large_transaction_rate,
    round(fraud_transactions / total_transactions * 100, 4) AS fraud_rate,
    
    -- Суммы
    round(total_amount, 2) AS total_amount,
    round(fraud_amount, 2) AS fraud_amount,
    
    -- Средние
    round(avg_amount, 2) AS avg_check,
    round(avg_fraud_amount, 2) AS avg_fraud_check,
    
    -- Перцентили
    round(median_amount, 2) AS median_amount,
    round(p95_amount, 2) AS p95_amount,
    round(p99_amount, 2) AS p99_amount,
    
    -- Уникальные значения
    unique_customers,
    unique_merchants,
    
    -- Расстояние
    round(avg_distance_km, 2) AS avg_distance_km

FROM daily_state_agg
ORDER BY transaction_date DESC, total_transactions DESC
