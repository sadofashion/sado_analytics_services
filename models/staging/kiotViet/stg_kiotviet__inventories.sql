{{ config(
    materialized = 'incremental',
    unique_key = ['product_id','branch_id'],
    partition_by ={ 'field': '_batched_at',
    'data_type': 'timestamp',
    'granularity': 'day' },
    incremental_strategy = 'merge',
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

{% if is_incremental() %}
WHERE
    _batched_at >= _dbt_max_partition
{% endif %}
{# UNION ALL
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
    ) }} #}

{# {% if is_incremental() %}
WHERE
    DATE(_batched_at) >= DATE(_dbt_max_partition)
{% endif %}  #}
)
{{ 
    dbt_utils.deduplicate(
    relation = 'source', 
    partition_by = 'product_id,branch_id', 
    order_by = "_batched_at desc",
    ) 
    }}