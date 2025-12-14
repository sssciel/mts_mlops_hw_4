{% macro amount_bucket(amount_column) %}
    CASE
        WHEN {{ amount_column }} < 10 THEN 'micro'
        WHEN {{ amount_column }} >= 10 AND {{ amount_column }} < 50 THEN 'small'
        WHEN {{ amount_column }} >= 50 AND {{ amount_column }} < 100 THEN 'medium'
        WHEN {{ amount_column }} >= 100 AND {{ amount_column }} < 500 THEN 'large'
        WHEN {{ amount_column }} >= 500 AND {{ amount_column }} < 1000 THEN 'xlarge'
        ELSE 'xxlarge'
    END
{% endmacro %}
