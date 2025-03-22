{% test colA_greater_equal_colB(model, column_A, column_B) %}

with validation as (

    select
        {{ column_A }} as first_col,
        {{ column_B }} as second_col

    from {{ model }}

),

validation_errors as (

    select
        *

    from validation
    -- if this is true, then even_field is actually odd!
    where first_col < second_col

)

select *
from validation_errors

{% endtest %}