{{
    config(
        tags=['caresoft','dimension','view']
    )
}}
select
    custom_field_id,
    custom_field_lable custom_field_label,
    f.type,
    values.id as value_id,
    values.lable as value_label,
    case values.parent_value_id when -1 then null else values.parent_value_id end as value_parent_id
from {{ref('base_caresoft__ticket_custom_fields')}} f, unnest(values)  values