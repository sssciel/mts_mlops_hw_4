{{
    config(
        materialized='table'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('transactions_db', 'transactions') }}
),

with_datetime AS (
    SELECT
        *,
        toDateTime(concat(transaction_time, ':00')) AS transaction_datetime
    FROM source
),

cleaned AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['transaction_time', 'merch', 'amount', 'name_1', 'name_2']) }} AS transaction_id,
        transaction_datetime AS transaction_time,
        toDate(transaction_datetime) AS transaction_date,
        toHour(transaction_datetime) AS transaction_hour,
        toDayOfWeek(transaction_datetime) AS day_of_week,
        toMonth(transaction_datetime) AS transaction_month,
        toYear(transaction_datetime) AS transaction_year,
        merch AS merchant_name,
        replaceRegexpOne(merch, '^fraud_', '') AS merchant_clean_name,
        cat_id AS category,
        toFloat64(amount) AS amount,
        {{ amount_bucket('amount') }} AS amount_bucket,
        CASE WHEN amount >= {{ var('large_transaction_threshold') }} THEN 1 ELSE 0 END AS is_large_transaction,
        concat(name_1, ' ', name_2) AS customer_full_name,
        {{ dbt_utils.generate_surrogate_key(['name_1', 'name_2']) }} AS customer_id,
        name_1 AS first_name,
        name_2 AS last_name,
        gender,
        us_state AS state,
        toFloat64(lat) AS customer_lat,
        toFloat64(lon) AS customer_lon,
        toFloat64(merchant_lat) AS merchant_lat,
        toFloat64(merchant_lon) AS merchant_lon,
        {{ haversine_distance('lat', 'lon', 'merchant_lat', 'merchant_lon') }} AS distance_km,
        toUInt8(target) AS is_fraud

    FROM with_datetime
)

SELECT * FROM cleaned
