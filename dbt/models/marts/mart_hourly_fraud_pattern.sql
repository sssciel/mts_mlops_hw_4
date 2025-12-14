{{
    config(
        materialized='table'
    )
}}

WITH hourly_dow_stats AS (
    SELECT
        day_of_week,
        transaction_hour,
        
        -- Транзакции
        count(*) AS total_transactions,
        countIf(is_fraud = 1) AS fraud_transactions,
        countIf(is_fraud = 0) AS legit_transactions,
        
        -- Суммы
        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount,
        
        -- Средние
        avg(amount) AS avg_amount,
        avgIf(amount, is_fraud = 1) AS avg_fraud_amount,
        
        -- Крупные транзакции
        countIf(is_large_transaction = 1) AS large_transactions,
        
        -- Уникальные клиенты
        uniqExact(customer_id) AS unique_customers,
        uniqExactIf(customer_id, is_fraud = 1) AS fraud_customers,
        
        -- Расстояние
        avgIf(distance_km, is_fraud = 1) AS avg_fraud_distance

    FROM {{ ref('stg_transactions') }}
    GROUP BY day_of_week, transaction_hour
),

overall_fraud_rate AS (
    SELECT 
        countIf(is_fraud = 1) / count(*) AS global_fraud_rate
    FROM {{ ref('stg_transactions') }}
)

SELECT
    h.day_of_week,
    CASE h.day_of_week
        WHEN 1 THEN 'Понедельник'
        WHEN 2 THEN 'Вторник'
        WHEN 3 THEN 'Среда'
        WHEN 4 THEN 'Четверг'
        WHEN 5 THEN 'Пятница'
        WHEN 6 THEN 'Суббота'
        WHEN 7 THEN 'Воскресенье'
    END AS day_name,
    h.transaction_hour,
    
    -- Транзакции
    h.total_transactions,
    h.fraud_transactions,
    h.legit_transactions,
    
    -- Fraud Rate
    round(h.fraud_transactions / h.total_transactions * 100, 4) AS fraud_rate,
    
    -- Сравнение с глобальным fraud_rate
    round((h.fraud_transactions / h.total_transactions) / o.global_fraud_rate, 2) AS fraud_rate_multiplier,
    
    -- Флаг повышенного риска (fraud rate выше среднего в 1.5 раза)
    CASE 
        WHEN (h.fraud_transactions / h.total_transactions) > (o.global_fraud_rate * 1.5) THEN 1 
        ELSE 0 
    END AS is_high_risk_period,
    
    -- Доля от всех фродов
    round(h.fraud_transactions / sum(h.fraud_transactions) OVER () * 100, 2) AS fraud_share_pct,
    
    -- Суммы
    round(h.total_amount, 2) AS total_amount,
    round(h.fraud_amount, 2) AS fraud_amount,
    
    -- Средние
    round(h.avg_amount, 2) AS avg_amount,
    round(h.avg_fraud_amount, 2) AS avg_fraud_amount,
    
    -- Крупные транзакции
    h.large_transactions,
    round(h.large_transactions / h.total_transactions * 100, 2) AS large_transaction_rate,
    
    -- Клиенты
    h.unique_customers,
    h.fraud_customers,
    
    -- Расстояние при фроде
    round(h.avg_fraud_distance, 2) AS avg_fraud_distance_km,
    
    -- Ранг по fraud_rate в рамках дня недели
    row_number() OVER (PARTITION BY h.day_of_week ORDER BY h.fraud_transactions / h.total_transactions DESC) AS hour_fraud_rank_in_day

FROM hourly_dow_stats h
CROSS JOIN overall_fraud_rate o
ORDER BY h.day_of_week, h.transaction_hour
