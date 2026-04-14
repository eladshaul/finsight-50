{% test max_date_gap(model, column_name, compare_column, max_days=7) %}

with validation_errors as (
    select
        {{ column_name }},
        {{ compare_column }}
    from {{ model }}
    where 
        date_diff({{ column_name }}, {{ compare_column }}, DAY) > {{ max_days }}
        or date_diff({{ column_name }}, {{ compare_column }}, DAY) < 0
)

select *
from validation_errors

{% endtest %}