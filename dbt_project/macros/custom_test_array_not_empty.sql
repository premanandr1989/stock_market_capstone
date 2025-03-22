{% test custom_test_array_not_empty(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE ARRAY_SIZE({{ column_name }}) = 0

{% endtest %}