{% macro example1() %}

    {{ log('Hello World', info=True) }}

{% endmacro %}


{% macro example2() %}

    {% set query %}
        select 1 as id
    {% endset %}

    {% set result = run_query(query) %}

    {{ result.print_table() }}
    {{ log(result.columns[0].values(), info=True) }}
    {{ log(result.columns[0][0], info=True) }}

{% endmacro %}


{% macro example3() %}

    {% set query %}
        select 1 as id
    {% endset %}

    {% set result = dbt_utils.get_single_value(query) %}

    {{ log(result, info=True) }}

{% endmacro %}


{% macro example4() %}

    {{ log('target name:' ~ target.name, info=True) }}
    {{ log('target database:' ~ target.database, info=True) }}
    {{ log('target schema:' ~ target.schema, info=True) }}
    {{ log('target warehouse:' ~ target.warehouse, info=True) }}
    {{ log('target user:' ~ target.user, info=True) }}
    {{ log('target role:' ~ target.role, info=True) }}

    {% if target.schema == 'dev' %}
        {{ log('doing some dev stuff', info=True) }}
    {% elif target.schema == 'prod' %}
        {{ log('doing some prod stuff', info=True) }}
    {% else %}
        {{ log('doing nothing', info=True) }}
    {% endif %}

{% endmacro %}


{% macro example5(my_number=1000) %}

    {% if my_number < 0 or my_number > 100 %}
        {{ exceptions.raise_compiler_error("Invalid `number`. Got: " ~ my_number) }}
    {% elif my_number < 25 or my_number > 75 %}
        {{ exceptions.warn("Invalid `number`. Got: " ~ my_number) }}
    {% else  %}
        {{ log("Valid `number`: " ~ my_number, info=True) }}
    {% endif %}

{% endmacro %}

{% macro example6() %}
    {{ return(adapter.dispatch('example6')()) }}
{% endmacro %}

{% macro trino__example6() %}
    {{ log('trino macro', info=True) }}
{% endmacro %}

{% macro snowflake__example6() %}
    {{ log('snowflake macro', info=True) }}
{% endmacro %}

{% macro default__example6() %}
    {{ log('default macro', info=True) }}
{% endmacro %}

