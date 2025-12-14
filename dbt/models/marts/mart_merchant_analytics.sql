{{
    config(
        materialized='table'
    )
}}

WITH merchant_stats AS (
    SELECT
        merchant_name,
        merchant_clean_name,
        
        -- Транзакции
        count(*) AS total_transactions,
        countIf(is_fraud = 1) AS fraud_transactions,
        countIf(is_fraud = 0) AS legit_transactions,
        
        -- Суммы
        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount,
        sumIf(amount, is_fraud = 0) AS legit_amount,
        
        -- Средние
        avg(amount) AS avg_amount,
        avgIf(amount, is_fraud = 1) AS avg_fraud_amount,
        avgIf(amount, is_fraud = 0) AS avg_legit_amount,
        quantile(0.5)(amount) AS median_amount,
        max(amount) AS max_amount,
        
        -- Крупные транзакции
        countIf(is_large_transaction = 1) AS large_transactions,
        
        -- Уникальные клиенты
        uniqExact(customer_id) AS unique_customers,
        uniqExactIf(customer_id, is_fraud = 1) AS fraud_customers,
        
        -- Категории
        uniqExact(category) AS categories_count,
        groupArrayDistinct(category) AS categories,
        
        -- Штаты
        uniqExact(state) AS states_count,
        
        -- Расстояние
        avg(distance_km) AS avg_distance_km,
        avgIf(distance_km, is_fraud = 1) AS avg_fraud_distance_km,
        
        -- Временной анализ
        min(transaction_date) AS first_transaction,
        max(transaction_date) AS last_transaction,
        dateDiff('day', min(transaction_date), max(transaction_date)) AS days_active,
        
        -- Часовой паттерн
        avgIf(transaction_hour, is_fraud = 1) AS avg_fraud_hour,
        
        -- Гендер
        countIf(gender = 'M') AS male_transactions,
        countIf(gender = 'F') AS female_transactions

    FROM {{ ref('stg_transactions') }}
    GROUP BY merchant_name, merchant_clean_name
),

overall_stats AS (
    SELECT 
        countIf(is_fraud = 1) / count(*) AS global_fraud_rate,
        avg(amount) AS global_avg_amount
    FROM {{ ref('stg_transactions') }}
)

SELECT
    m.merchant_name,
    m.merchant_clean_name,
    
    -- Транзакции
    m.total_transactions,
    m.fraud_transactions,
    m.legit_transactions,
    
    -- Fraud Rate
    round(m.fraud_transactions / m.total_transactions * 100, 4) AS fraud_rate,
    
    -- Сравнение с глобальным fraud_rate
    round((m.fraud_transactions / m.total_transactions) / o.global_fraud_rate, 2) AS fraud_rate_multiplier,
    
    -- Флаг подозрительного мерчанта
    CASE 
        WHEN (m.fraud_transactions / m.total_transactions) > (o.global_fraud_rate * 2) 
             AND m.fraud_transactions >= 5 THEN 1 
        ELSE 0 
    END AS is_suspicious,
    
    -- Уровень риска мерчанта
    {{ risk_level('m.fraud_transactions / m.total_transactions') }} AS risk_level,
    
    -- Суммы (оборот)
    round(m.total_amount, 2) AS total_turnover,
    round(m.fraud_amount, 2) AS fraud_amount,
    round(m.legit_amount, 2) AS legit_amount,
    
    -- Средние
    round(m.avg_amount, 2) AS avg_check,
    round(m.avg_fraud_amount, 2) AS avg_fraud_check,
    round(m.avg_legit_amount, 2) AS avg_legit_check,
    round(m.median_amount, 2) AS median_check,
    round(m.max_amount, 2) AS max_transaction,
    
    -- Сравнение среднего чека с глобальным
    round(m.avg_amount / o.global_avg_amount, 2) AS avg_check_multiplier,
    
    -- Крупные транзакции
    m.large_transactions,
    round(m.large_transactions / m.total_transactions * 100, 2) AS large_transaction_rate,
    
    -- Клиенты
    m.unique_customers,
    m.fraud_customers,
    round(m.fraud_customers / m.unique_customers * 100, 2) AS customer_fraud_rate,
    round(m.total_transactions / m.unique_customers, 2) AS avg_transactions_per_customer,
    
    -- Категории и география
    m.categories_count,
    m.states_count,
    
    -- Расстояние
    round(m.avg_distance_km, 2) AS avg_distance_km,
    round(m.avg_fraud_distance_km, 2) AS avg_fraud_distance_km,
    
    -- Активность
    m.first_transaction,
    m.last_transaction,
    m.days_active,
    round(m.total_transactions / nullIf(m.days_active, 0), 2) AS transactions_per_day,
    round(m.total_amount / nullIf(m.days_active, 0), 2) AS turnover_per_day,
    
    -- Часовой паттерн фрода
    round(m.avg_fraud_hour, 1) AS avg_fraud_hour,
    
    -- Гендерное распределение
    m.male_transactions,
    m.female_transactions,
    round(m.male_transactions / m.total_transactions * 100, 2) AS male_pct,
    
    -- Ранги
    row_number() OVER (ORDER BY m.fraud_transactions / m.total_transactions DESC) AS fraud_rate_rank,
    row_number() OVER (ORDER BY m.total_amount DESC) AS turnover_rank

FROM merchant_stats m
CROSS JOIN overall_stats o
ORDER BY fraud_rate DESC, total_transactions DESC
