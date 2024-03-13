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
    FROM
        {{ source(
            'kiotViet',
            'p_customers_list_*'
        ) }}
        {% if is_incremental() %}
          where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date(_dbt_max_partition)
        {% endif %}
    UNION ALL
    SELECT
        *
    FROM
        {{ source(
            'kiotViet',
            'p_webhook_customer_update'
        ) }}
),
raw_ AS (
    {{ dbt_utils.deduplicate(relation = 'source', partition_by = 'id', order_by = "_batched_at desc",) }}
)
SELECT
    id,
    code,
    NAME,
    gender,
    birthDate,
    contactNumber,
    branchId,
    TYPE,
    raw_.groups,
    debt,
    totalInvoiced,
    totalPoint,
    totalRevenue,
    rewardPoint,
    createdDate,
    modifiedDate,
FROM
    raw_