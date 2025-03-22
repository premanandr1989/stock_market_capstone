{% macro calculate_wma(ticker_col, date_col, price_col, period) %}
    (
        {% for i in range(period) %}
            ({{period}} - {{i}}) * LAG({{price_col}}, {{i}}) OVER (PARTITION BY {{ticker_col}} ORDER BY {{date_col}})
            {% if not loop.last %} + {% endif %}
        {% endfor %}
    ) / ({% for i in range(period) %}{{period - i}}{% if not loop.last %} + {% endif %}{% endfor %})
{% endmacro %}