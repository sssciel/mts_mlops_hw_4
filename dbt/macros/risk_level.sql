{% macro risk_level(fraud_rate_column) %}
    CASE
        WHEN {{ fraud_rate_column }} >= {{ var('high_risk_threshold') }} THEN 'HIGH'
        WHEN {{ fraud_rate_column }} >= {{ var('medium_risk_threshold') }} THEN 'MEDIUM'
        ELSE 'LOW'
    END
{% endmacro %}
