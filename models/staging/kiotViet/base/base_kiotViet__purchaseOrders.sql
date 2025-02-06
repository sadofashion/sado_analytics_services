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
    tags = ['incremental', 'hourly','kiotviet']
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
            'p_purchaseorders_list'
        ) }}
    {% if is_incremental() %}
      where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date(_dbt_max_partition)
    {% endif %}

    UNION ALL

    SELECT
        *
    EXCEPT(purchaseOrder_details),
        purchaseOrder_details AS purchaseOrderDetails
    FROM
        {{ source(
            'kiotViet',
            'p_purchaseorders_list2'
        ) }}
    {% if is_incremental() %}
      where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date(_dbt_max_partition)
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
    CASE
        WHEN status = 3 THEN "Đã nhập hàng"
        WHEN status = 1 THEN "Phiếu tạm"
        WHEN status = 4 THEN "Đã huỷ"
    END AS transaction_status,
    createdDate,
    purchaseOrderDetails,
FROM
    raw_
