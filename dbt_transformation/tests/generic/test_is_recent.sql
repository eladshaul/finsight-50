{% test is_recent(model, column_name, max_days=4) %}

with validation_errors as (
    select
        {{ column_name }}
    from {{ model }}
    where 
        -- מחשבים את ההפרש בין היום לבין התאריך בטבלה
        date_diff(current_date(), {{ column_name }}, DAY) > {{ max_days }}
        -- הגנה למקרה שהתאריך בטעות מהעתיד
        or date_diff(current_date(), {{ column_name }}, DAY) < 0
)

select *
from validation_errors

{% endtest %}