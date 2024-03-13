{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change = 'sync_all_columns',
    partition_by = {
      "field": "createdDate",
      "data_type": "timestamp",
      "granularity": "day"
    },
    incremental_strategy = 'merge',
    tags = ['incremental', 'daily','kiotviet']
    )
}}


WITH source AS (
    SELECT
        *
    EXCEPT(purchaseOrderDetails),
        purchaseOrderDetails
    FROM
        {{ source(
            'kiotViet',
            'p_purchaseorders_list_*'
        ) }}
    {% if is_incremental() %}
      where date(_TABLE_SUFFIX) >= date(_dbt_max_partition)
    {% endif %}
    UNION ALL
    SELECT
        *
    EXCEPT(purchaseOrder_details),
        purchaseOrder_details AS purchaseOrderDetails
    FROM
        {{ source(
            'kiotViet',
            'p_purchaseorders_list2_*'
        ) }}
    {% if is_incremental() %}
      where date(_TABLE_SUFFIX) >= date(_dbt_max_partition)
    {% endif %}
),
raw_ AS (
    {{ dbt_utils.deduplicate(
        relation = 'source',
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    id,
    code,
    purchaseDate,
    branchId,
    purchaseById,
    supplierId,
    supplierName,
    supplierCode,
    partnerType,
    total,
    totalPayment,
    discount,
    discountRatio,
    status,
    createdDate,
    purchaseOrderDetails,
FROM
    raw_
