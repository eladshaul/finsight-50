{% test is_positive(model, column_name) %}

with validation_errors as (
    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 0
)

select *
from validation_errors

{% endtest %}