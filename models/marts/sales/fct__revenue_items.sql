{{ config(
    materialized = 'table',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'hourly','fact','kiotviet','nhanhvn']
) }}

{# select *,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'product_code', 'transaction_date', 'transaction_type', 'transaction_source'])}} as revenue_item_id,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'transaction_date', 'transaction_type', ])}} as transaction_id,
FROM
    kiotviet_details
UNION ALL
SELECT
    *,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'product_code', 'transaction_date', 'transaction_type', 'transaction_source'])}} as revenue_item_id,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'transaction_date', 'transaction_type', ])}} as transaction_id,
FROM
    nhanhvn_details #}

with preprocessed as (
    select *,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'product_code', 'transaction_date', 'transaction_type', 'source'])}} as revenue_item_id,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'transaction_date', 'transaction_type', ])}} as transaction_id,
from {{ ref("int_kiotviet__revenue_items") }} 
union all

select *,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'product_code', 'transaction_date', 'transaction_type', 'source'])}} as revenue_item_id,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'transaction_date', 'transaction_type', ])}} as transaction_id,
from {{ ref("int_kiotviet__return_items") }}

union all

select *,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'product_code', 'transaction_date', 'transaction_type', 'source'])}} as revenue_item_id,
{{dbt_utils.generate_surrogate_key(['branch_id', 'customer_id', 'transaction_date', 'transaction_type', ])}} as transaction_id,
from {{ ref("int_nhanhvn__revenue_items") }}
)

select p.*,
c.inventory_value_per_unit*p.quantity as cogs
from preprocessed p
left join {{ ref("stg_gsheet__cogs") }} c 
on p.product_code = c.product_code 
and p.transaction_date >= date(c.dbt_valid_from) 
and (p.transaction_date < date(c.dbt_valid_to) or c.dbt_valid_to is null)
