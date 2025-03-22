{% macro get_max_date(table_name) %}
    {% set query %}
        SELECT MAX(date) as max_date
        FROM {{ ref(table_name) }},
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set max_date = results.columns[0].values()[0] %}
        {{ return(max_date) }}
    {% endif %}
{% endmacro %}