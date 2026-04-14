{% test calculation_integrity(model, column_name, multiplier_column, factor_column) %}

with validation_errors as (
    select
        {{ column_name }},
        ({{ multiplier_column }} * {{ factor_column }}) as expected_value
    from {{ model }}
    where 
        abs({{ column_name }} - ({{ multiplier_column }} * {{ factor_column }})) > 0.01
)

select *
from validation_errors

{% endtest %}