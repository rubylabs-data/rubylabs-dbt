{% macro platform_lookup(column_name) %}
    CASE WHEN {{ column_name }} = 'app_store' THEN 'ios'
         WHEN {{ column_name }} = 'play_store' THEN 'android'
         WHEN {{ column_name }} = 'web' THEN 'web'
         ELSE 'n. a.' END
{% endmacro %}