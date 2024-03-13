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
        * except(invoiceDelivery,invoiceDetails),
        invoiceDetails
    FROM
        {{ source(
            'kiotViet',
            'p_invoices_list'
        ) }} invoice1
        {% if is_incremental() %}
          where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date(_dbt_max_partition)
        {% endif %}

    union all 

    select * except(invoiceDelivery,invoice_details),
    invoice_details as invoiceDetails
    from {{ source(
            'kiotViet',
            'p_invoices_list2'
        ) }} 
        {% if is_incremental() %}
          where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date(_dbt_max_partition)
        {% endif %}

    UNION ALL

    SELECT
        * except(invoiceDelivery,invoiceDetails),
        invoiceDetails
    FROM
        {{ source(
            'kiotViet',
            'p_webhook_invoice_update'
        ) }}
        {% if is_incremental() %}
          where date(_batched_at) >= date(_dbt_max_partition)
        {% endif %}
),
raw_ AS (
    {{ dbt_utils.deduplicate(relation = 'source', partition_by = 'id', order_by = "_batched_at desc",) }}
)
SELECT
    id,
    uuid,
    code,
    purchaseDate,
    branchId,
    soldById,
    customerId,
    orderCode,
    total,
    totalPayment,
    statusValue,
    COALESCE(first_value(createdDate ignore nulls) over (partition by id order by _batched_at asc), 
    first_value(_batched_at) over (partition by id order by _batched_at asc)
     ) createdDate ,
    modifiedDate,
    discountRatio,
    discount,
    payments,
    invoiceDetails,
    "invoice" as transaction_type,
FROM
    raw_
