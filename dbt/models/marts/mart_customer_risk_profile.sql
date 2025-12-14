{{
    config(
        materialized='table'
    )
}}

WITH customer_stats AS (
    SELECT
        customer_id,
        customer_full_name,
        any(gender) AS gender,
        any(state) AS state,
        
        -- Транзакции
        count(*) AS total_transactions,
        countIf(is_fraud = 1) AS fraud_transactions,
        countIf(is_fraud = 0) AS legit_transactions,
        
        -- Суммы
        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount,
        sumIf(amount, is_fraud = 0) AS legit_amount,
        
        -- Средние и медианы
        avg(amount) AS avg_amount,
        avgIf(amount, is_fraud = 1) AS avg_fraud_amount,
        quantile(0.5)(amount) AS median_amount,
        max(amount) AS max_amount,
        min(amount) AS min_amount,
        
        -- Крупные транзакции
        countIf(is_large_transaction = 1) AS large_transactions,
        
        -- Категории
        uniqExact(category) AS unique_categories,
        uniqExactIf(category, is_fraud = 1) AS fraud_categories,
        
        -- Мерчанты
        uniqExact(merchant_name) AS unique_merchants,
        uniqExactIf(merchant_name, is_fraud = 1) AS fraud_merchants,
        
        -- Расстояние
        avg(distance_km) AS avg_distance_km,
        max(distance_km) AS max_distance_km,
        
        -- Временной период
        min(transaction_date) AS first_transaction_date,
        max(transaction_date) AS last_transaction_date,
        dateDiff('day', min(transaction_date), max(transaction_date)) AS days_active,
        
        -- Часовой паттерн
        avgIf(transaction_hour, is_fraud = 1) AS avg_fraud_hour

    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id, customer_full_name
),

customer_risk AS (
    SELECT
        *,
        -- Fraud rate
        fraud_transactions / total_transactions AS fraud_rate,
        
        -- Уровень риска
        {{ risk_level('fraud_transactions / total_transactions') }} AS risk_level

    FROM customer_stats
)

SELECT
    customer_id,
    customer_full_name,
    gender,
    state,
    risk_level,
    
    -- Транзакции
    total_transactions,
    fraud_transactions,
    legit_transactions,
    round(fraud_rate * 100, 4) AS fraud_rate_pct,
    
    -- Суммы
    round(total_amount, 2) AS total_amount,
    round(fraud_amount, 2) AS fraud_amount,
    round(legit_amount, 2) AS legit_amount,
    
    -- Средние
    round(avg_amount, 2) AS avg_check,
    round(avg_fraud_amount, 2) AS avg_fraud_check,
    round(median_amount, 2) AS median_check,
    round(max_amount, 2) AS max_transaction,
    round(min_amount, 2) AS min_transaction,
    
    -- Крупные транзакции
    large_transactions,
    round(large_transactions / total_transactions * 100, 2) AS large_transaction_rate,
    
    -- Разнообразие
    unique_categories,
    fraud_categories,
    unique_merchants,
    fraud_merchants,
    
    -- Расстояние
    round(avg_distance_km, 2) AS avg_distance_km,
    round(max_distance_km, 2) AS max_distance_km,
    
    -- Активность
    first_transaction_date,
    last_transaction_date,
    days_active,
    round(total_transactions / nullIf(days_active, 0), 2) AS transactions_per_day,
    
    -- Часовой паттерн фрода
    round(avg_fraud_hour, 1) AS avg_fraud_hour

FROM customer_risk
ORDER BY fraud_rate DESC, total_transactions DESC
