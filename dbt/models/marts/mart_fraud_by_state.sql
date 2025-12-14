{{
    config(
        materialized='table'
    )
}}

WITH state_stats AS (
    SELECT
        state,
        
        -- Транзакции
        count(*) AS total_transactions,
        countIf(is_fraud = 1) AS fraud_transactions,
        countIf(is_fraud = 0) AS legit_transactions,
        
        -- Суммы
        sum(amount) AS total_amount,
        sumIf(amount, is_fraud = 1) AS fraud_amount,
        sumIf(amount, is_fraud = 0) AS legit_amount,
        
        -- Средние значения
        avg(amount) AS avg_amount,
        avgIf(amount, is_fraud = 1) AS avg_fraud_amount,
        
        -- Уникальные клиенты и мерчанты
        uniqExact(customer_id) AS unique_customers,
        uniqExactIf(customer_id, is_fraud = 1) AS fraud_customers,
        uniqExact(merchant_name) AS unique_merchants,
        uniqExactIf(merchant_name, is_fraud = 1) AS fraud_merchants,
        
        -- Расстояние
        avg(distance_km) AS avg_distance_km,
        avgIf(distance_km, is_fraud = 1) AS avg_fraud_distance_km,
        
        -- Категории с фродом
        uniqExactIf(category, is_fraud = 1) AS fraud_categories,
        
        -- Гендер
        countIf(gender = 'M') AS male_transactions,
        countIf(gender = 'F') AS female_transactions,
        countIf(is_fraud = 1 AND gender = 'M') AS male_fraud,
        countIf(is_fraud = 1 AND gender = 'F') AS female_fraud

    FROM {{ ref('stg_transactions') }}
    GROUP BY state
)

SELECT
    state,
    
    -- Транзакции
    total_transactions,
    fraud_transactions,
    legit_transactions,
    
    -- Fraud Rate
    round(fraud_transactions / total_transactions * 100, 4) AS fraud_rate,
    
    -- Доля штата от всех фродов
    round(fraud_transactions / sum(fraud_transactions) OVER () * 100, 2) AS fraud_share_pct,
    
    -- Суммы
    round(total_amount, 2) AS total_amount,
    round(fraud_amount, 2) AS fraud_amount,
    round(legit_amount, 2) AS legit_amount,
    
    -- Средние
    round(avg_amount, 2) AS avg_amount,
    round(avg_fraud_amount, 2) AS avg_fraud_amount,
    
    -- Клиенты и мерчанты
    unique_customers,
    fraud_customers,
    round(fraud_customers / unique_customers * 100, 2) AS customer_fraud_rate,
    unique_merchants,
    fraud_merchants,
    round(fraud_merchants / unique_merchants * 100, 2) AS merchant_fraud_rate,
    
    -- Транзакций на клиента
    round(total_transactions / unique_customers, 2) AS avg_transactions_per_customer,
    
    -- Расстояние
    round(avg_distance_km, 2) AS avg_distance_km,
    round(avg_fraud_distance_km, 2) AS avg_fraud_distance_km,
    
    -- Категории
    fraud_categories,
    
    -- Гендерная статистика
    male_transactions,
    female_transactions,
    male_fraud,
    female_fraud,
    round(male_fraud / nullIf(male_transactions, 0) * 100, 4) AS male_fraud_rate,
    round(female_fraud / nullIf(female_transactions, 0) * 100, 4) AS female_fraud_rate,
    
    -- Ранг по fraud_rate
    row_number() OVER (ORDER BY fraud_transactions / total_transactions DESC) AS fraud_rate_rank

FROM state_stats
ORDER BY fraud_rate DESC
