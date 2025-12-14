# Fraud Analytics dbt Project

Решение 4 ДЗ MLops MTS.

## Структура проекта

```
dbt/
├── dbt_project.yml           # Конфигурация проекта
├── packages.yml              # Зависимости (dbt_utils, dbt_date, dbt_expectations)
├── profiles.yml              # Настройки подключения к ClickHouse
├── Makefile                  # Автоматизация команд
├── .sqlfluff                 # Конфигурация SQLFluff
├── .pre-commit-config.yaml   # Pre-commit hooks
├── models/
│   ├── sources/
│   │   └── sources.yml       # Определение источников
│   ├── staging/
│   │   ├── stg_transactions.sql      # Staging-модель
│   │   ├── stg_transactions.yml      # Тесты и описания
│   │   └── stg_transactions_unit_tests.yml  # Unit-тесты
│   └── marts/
│       ├── mart_daily_state_metrics.sql    # Дневные метрики по штатам
│       ├── mart_fraud_by_category.sql      # Анализ фрода по категориям
│       ├── mart_fraud_by_state.sql         # Географический анализ фрода
│       ├── mart_customer_risk_profile.sql  # Профиль риска клиентов
│       ├── mart_hourly_fraud_pattern.sql   # Временные паттерны фрода
│       ├── mart_merchant_analytics.sql     # Аналитика по мерчантам
│       └── schema.yml                      # Тесты и описания витрин
├── macros/
│   ├── amount_bucket.sql     # Макрос сегментации сумм
│   ├── risk_level.sql        # Макрос определения уровня риска
│   └── haversine_distance.sql # Макрос расчёта расстояния
├── tests/                    # Singular tests
│   ├── assert_no_negative_amounts.sql
│   ├── assert_fraud_rate_bounds.sql
│   ├── assert_fraud_count_consistency.sql
│   ├── assert_risk_level_consistency.sql
│   └── assert_valid_transaction_hours.sql
└── seeds/
    ├── states.csv            # Справочник штатов
    └── seeds.yml             # Описание seeds
```

## Архитектура моделей

```
raw (source) → staging → marts
```

### Staging-слой
- **stg_transactions** — очистка и нормализация сырых данных, приведение типов, добавление вычисляемых полей

### Витрины (marts)
1. **mart_daily_state_metrics** — Дневные метрики по штатам
2. **mart_fraud_by_category** — Анализ фрода по категориям
3. **mart_fraud_by_state** — Географический анализ фрода
4. **mart_customer_risk_profile** — Профиль риска клиентов (HIGH/MEDIUM/LOW)
5. **mart_hourly_fraud_pattern** — Временные паттерны фрода
6. **mart_merchant_analytics** — Аналитика по мерчантам с флагом подозрительности

## Макросы

- `amount_bucket(amount_column)` — сегментация сумм (micro, small, medium, large, xlarge, xxlarge)
- `risk_level(fraud_rate_column)` — определение уровня риска (HIGH/MEDIUM/LOW)
- `haversine_distance(lat1, lon1, lat2, lon2)` — расчёт расстояния между координатами

## Установка и запуск

### Предварительные требования
- Python 3.9+
- dbt-core >= 1.8.0
- dbt-clickhouse
- ClickHouse с `transactions_db.transactions`

### Установка зависимостей

```bash
pip install dbt-core dbt-clickhouse sqlfluff sqlfluff-templater-dbt pre-commit

cd dbt/
dbt deps
```

### Запуск проекта

```bash
# Сделать все
make all

# Или по частям
make deps      # Установка зависимостей dbt
make seed      # Загрузка seeds
make run       # Запуск моделей
make test      # Запуск тестов
make docs-serve  # Генерация и просмотр документации
```

### Линтинг

```bash
make lint      # Проверка SQL
make lint-fix  # Автоисправление
```

### Pre-commit

```bash
make pre-commit-install  # Установка hooks
make pre-commit-run      # Запуск на всех файлах
```

## Тесты

### Generic tests (schema.yml)
- `not_null`, `unique`, `accepted_values`
- `dbt_expectations.expect_column_values_to_be_between`
- `dbt_utils.unique_combination_of_columns`

### Singular tests (tests/)
- `assert_no_negative_amounts` — проверка отсутствия отрицательных сумм
- `assert_fraud_rate_bounds` — проверка границ fraud_rate (0-100%)
- `assert_fraud_count_consistency` — fraud_transactions <= total_transactions
- `assert_risk_level_consistency` — соответствие risk_level и fraud_rate
- `assert_valid_transaction_hours` — часы в диапазоне 0-23

### Unit tests (dbt 1.8+)
- `test_amount_bucket_segmentation` — проверка макроса сегментации сумм
- `test_is_fraud_flag` — проверка преобразования target в is_fraud
- `test_is_large_transaction_flag` — проверка флага крупной транзакции

## Пакеты

- **dbt_utils** — утилиты для генерации surrogate keys и тестов
- **dbt_date** — работа с датами
- **dbt_expectations** — расширенные тесты данных

## Версии

- dbt-core: >= 1.8.0
- dbt-clickhouse: >= 0.6.0
