{{ config(
    materialized = 'incremental',
    unique_key = ['product_id','branch_id'],
    partition_by ={ 'field': '_batched_at',
    'data_type': 'timestamp',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'hourly','fact','kiotviet']
) }}

WITH source AS (

    SELECT
        branchId AS branch_id,
        productCode AS product_code,
        productId AS product_id,
        productName AS product_name,
        onhand AS on_hand,
        reserved,
        actualReserved AS actual_reserved,
        maxQuantity AS max_quantity,
        minQuantity AS min_quantity,
        _batched_at,
    FROM
        {{ source(
            'kiotViet',
            'p_webhook_inventory_update'
        ) }}

WHERE 1=1
{% if is_incremental() %}
    and date(_batched_at) >= current_date
{% endif %}

UNION ALL
SELECT
    branchId AS branch_id,
    productCode AS product_code,
    productId AS product_id,
    productName AS product_name,
    onhand AS on_hand,
    reserved,
    actualReserved AS actual_reserved,
    maxQuantity AS max_quantity,
    minQuantity AS min_quantity,
    _batched_at,
FROM
    {{ source(
        'kiotViet',
        'p_products_inventory'
    ) }}

WHERE 1=1
{% if is_incremental() %}
    and DATE(_batched_at) >= DATE(_dbt_max_partition)
{% endif %}  

)
{{ 
    dbt_utils.deduplicate(
    relation = 'source', 
    partition_by = 'product_id,branch_id, date(_batched_at)', 
    order_by = "_batched_at desc",
    ) 
    }}