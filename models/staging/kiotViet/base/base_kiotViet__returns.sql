{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change = 'sync_all_columns',
    partition_by ={ "field": "createdDate",
    "data_type": "timestamp",
    "granularity": "day" },
    incremental_strategy = 'merge',
    tags = ['incremental', 'daily','kiotviet']
) }}

WITH source AS (

    SELECT
        *
    EXCEPT(returnDetails),
        returnDetails
    FROM
        {{ source(
            'kiotViet',
            'p_returns_list'
        ) }}

    {% if is_incremental() %}
    where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date(_dbt_max_partition)
    {% endif %}

    UNION ALL

    SELECT
        *
    EXCEPT(return_details),
        return_details AS returnDetails
    FROM
        {{ source(
            'kiotViet',
            'p_returns_list2'
        ) }}

    {% if is_incremental() %}
    where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date(_dbt_max_partition)
    {% endif %}
),
raw_ AS (
    {{ dbt_utils.deduplicate(
        relation = 'source',
        partition_by = 'id',
        order_by = "modifiedDate DESC,_batched_at desc",
    ) }}
)
SELECT
    id,
    code,
    invoiceId,
    returnDate,
    branchId,
    receivedById,
    customerId,
    returnTotal,
    returnDiscount,
    totalPayment,
    returnFee,
    returnFeeRatio,
    saleChannelId,
    statusValue,
    createdDate,
    modifiedDate,
    payments,
    returnDetails,
    "return" AS transaction_type
FROM
    raw_
