#!/bin/bash
set -e

clickhouse-client -n <<-EOSQL
    CREATE DATABASE IF NOT EXISTS transactions_db;
    
    CREATE TABLE IF NOT EXISTS transactions_db.transactions
    (
        transaction_time String,
        merch String,
        cat_id String,
        amount Float64,
        name_1 String,
        name_2 String,
        gender String,
        street String,
        one_city String,
        us_state String,
        post_code String,
        lat Float64,
        lon Float64,
        population_city UInt32,
        jobs String,
        merchant_lat Float64,
        merchant_lon Float64,
        target UInt8
    )
    ENGINE = MergeTree()
    ORDER BY (transaction_time, merch)
    SETTINGS index_granularity = 8192;
EOSQL

# Загрузка данных из CSV с правильными настройками
if [ -f /data/train.csv ]; then
    echo "Loading data from train.csv..."
    clickhouse-client --query="INSERT INTO transactions_db.transactions FORMAT CSVWithNames" \
        --format_csv_delimiter=',' \
        --input_format_csv_skip_first_lines=0 \
        --date_time_input_format=best_effort \
        < /data/train.csv
    echo "Data loaded successfully!"
    clickhouse-client --query="SELECT count(*) as total_rows FROM transactions_db.transactions"
fi
