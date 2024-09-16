{{ config(
    materialized = 'incremental',
    on_schema_change = 'sync_all_columns',
    partition_by ={ "field": "sent_date",
    "data_type": "timestamp",
    "granularity": "day" },
    incremental_strategy = 'insert_overwrite',
    tags = ['incremental', 'hourly','kiotviet']
) }}

{% set status = {
    2: 'Đang chuyển',
    3: 'Đã nhận',
    4: 'Đã hủy',
}
%}

with source as (
    SELECT *
    FROM
        {{ source('kiotViet','p_transfers_list') }}
    WHERE
        1=1
    {% if is_incremental() %}
        and date(sent_date) in (
            select 
                distinct date(sent_date) 
            from {{ source('kiotViet','p_transfers_list') }} 
                where parse_date('%Y%m%d',_TABLE_SUFFIX) >= current_date
            )
    {% endif %}
),

raw_ as (
    {{ dbt_utils.deduplicate(
        relation = 'source',
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)

select
    dispatchedDate as sent_date,
    receivedDate as received_date,
    code as transfer_code,
    description,
    fromBranchId as transfer_branch_id,
    toBranchId as receipt_branch_id,
    isActive as is_active,
    case status 
    {%for k,v in status.items()%}
        when {{k}} then '{{v}}'
    {%endfor%} end as transfer_status,
    id as transfer_id,
    transferDetails.productId as product_id,
    transferDetails.productCode as product_code,
    transferDetails.productName as product_name,
    transferDetails.sendQuantity as send_quantity,
    transferDetails.receiveQuantity as receive_quantity,
    transferDetails.sendPrice as send_price,
    transferDetails.receivePrice as receive_price,
    transferDetails.price as price,
from raw_
left join unnest(transferDetails) transferDetails