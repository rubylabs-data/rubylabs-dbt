{% macro email_hash(column_name) %}
    FARM_FINGERPRINT({{ column_name }})
{% endmacro %}