{{
    config(
        materialized='table'
    )
}}

WITH category_stats AS (
    SELECT
        category,
        
        -- Общие метрики
        count(*) AS total_transactions,
        countIf(is_fraud = 1) AS fraud_transactions,
        countIf(is_fraud = 0) AS legit_transactions,
        
        -- Суммы
        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount,
        sumIf(amount, is_fraud = 0) AS legit_amount,
        
        -- Средние суммы
        avg(amount) AS avg_amount,
        avgIf(amount, is_fraud = 1) AS avg_fraud_amount,
        avgIf(amount, is_fraud = 0) AS avg_legit_amount,
        
        -- Перцентили
        quantile(0.5)(amount) AS median_amount,
        quantileIf(0.5)(amount, is_fraud = 1) AS median_fraud_amount,
        
        -- Уникальные значения
        uniqExact(customer_id) AS unique_customers,
        uniqExact(merchant_name) AS unique_merchants,
        
        -- Среднее расстояние
        avg(distance_km) AS avg_distance_km,
        avgIf(distance_km, is_fraud = 1) AS avg_fraud_distance_km
        
    FROM {{ ref('stg_transactions') }}
    GROUP BY category
)

SELECT
    category,
    
    -- Транзакции
    total_transactions,
    fraud_transactions,
    legit_transactions,
    
    -- Fraud Rate
    round(fraud_transactions / total_transactions * 100, 4) AS fraud_rate,
    
    -- Доля от всех фродов
    round(fraud_transactions / sum(fraud_transactions) OVER () * 100, 2) AS fraud_share,
    
    -- Суммы
    round(total_amount, 2) AS total_amount,
    round(fraud_amount, 2) AS fraud_amount,
    round(legit_amount, 2) AS legit_amount,
    
    -- Средние суммы
    round(avg_amount, 2) AS avg_amount,
    round(avg_fraud_amount, 2) AS avg_fraud_amount,
    round(avg_legit_amount, 2) AS avg_legit_amount,
    
    -- Медианы
    round(median_amount, 2) AS median_amount,
    round(median_fraud_amount, 2) AS median_fraud_amount,
    
    -- Разница средних (фрод vs норма)
    round(avg_fraud_amount - avg_legit_amount, 2) AS avg_amount_diff,
    
    -- Уникальные значения
    unique_customers,
    unique_merchants,
    
    -- Расстояние
    round(avg_distance_km, 2) AS avg_distance_km,
    round(avg_fraud_distance_km, 2) AS avg_fraud_distance_km,
    
    -- Ранг по fraud_rate
    row_number() OVER (ORDER BY fraud_transactions / total_transactions DESC) AS fraud_rate_rank

FROM category_stats
ORDER BY fraud_rate DESC
