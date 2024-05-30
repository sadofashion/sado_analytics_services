{{ config(
    tags = ['incremental', 'hourly','fact','kiotviet','nhanhvn']
) }}


with union_relation as (
    {{dbt_utils.union_relations(
    relations=[
        ref('int_kiotviet__invoices'),
        ref('int_kiotviet__returns'),
        ref('int_nhanhvn__ordersdetails')
    ]
)}}
)

select *, 
{{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
from  union_relation
