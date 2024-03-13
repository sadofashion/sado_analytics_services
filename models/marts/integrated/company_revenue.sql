{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'merge',
    unique_key = ['transaction_source_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'hourly','fact','kiotviet','nhanhvn']
) }}

{%set order_statuses = [
        'Mới',
        'Chờ xác nhận',
        'Đang xác nhận',
        'Đã xác nhận',
        'Đổi kho hàng',
        'Đang đóng gói',
        'Đã đóng gói',
        'Chờ thu gom',
        'Đang chuyển',
        'Thành công'
        ]%}


with kiotviet_rev as (
    SELECT
    invoices.transaction_id,
    invoices.transaction_code,
    date(invoices.transaction_date) transaction_date,
    CAST(NULL AS int64) AS reference_transaction_id,
    invoices.branch_id,
    invoices.customer_id,
    invoices.employee_id,
    invoices.total,
    invoices.total_payment,
    invoices.discount,
    invoices.discount_ratio,
    CAST(NULL AS int64) AS return_fee,
    invoices.transaction_type,
    date(coalesce(invoices.modified_date, invoices.transaction_date)) as modified_date,
    'kiotviet' as source,
FROM
    {{ ref('stg_kiotviet__invoices') }}
    invoices
WHERE
    invoices.transaction_status = 'Hoàn thành'
    {% if is_incremental() %}
      and date(coalesce(invoices.modified_date, invoices.transaction_date)) >= date_add(date(_dbt_max_partition), interval -2 day)
    {% endif %}
UNION ALL
SELECT
    returns.transaction_id,
    returns.transaction_code,
    date(returns.transaction_date) transaction_date,
    returns.reference_transaction_id,
    returns.branch_id,
    returns.customer_id,
    returns.employee_id,
    (- returns.total) AS total,
    returns.total_payment,
    returns.return_discount,
    CAST( NULL AS float64 ) discount_ratio,
    returns.return_fee,
    returns.transaction_type,
    date( coalesce(returns.modified_date, returns.transaction_date)) as modified_date,
    'kiotviet' as source,
FROM
    {{ ref('stg_kiotviet__returns') }}
    returns
WHERE
    returns.transaction_status = 'Đã trả'
    {% if is_incremental() %}
      and date(coalesce(returns.modified_date, returns.transaction_date)) >= date_add(date(_dbt_max_partition), interval -2 day)
    {% endif %}
    ),
nhanhvn_rev as (
    select 
    order_id as transaction_id,
    shop_order_id as transaction_code,
    coalesce(delivery_date,send_carrier_date,date(created_date)) as transaction_date,
    return_from_order_id as reference_transaction_id,
    traffic_source_id as branch_id,
    customer_id as customer_id,
    created_by_id as employee_id,
    receivables as total,
    (receivables+money_transfer-customer_ship_fee) as total_payment,
    order_discount as discount,
    safe_divide(order_discount,order_discount+receivables) as discount_ratio,
    return_fee,
    case order_type when 'Giao hàng tại nhà' then 'invoice' when 'Khách trả lại hàng' then 'return' end as transaction_type,
    greatest(date(created_date), delivery_date, send_carrier_date) as modified_date,
    'nhanhvn' as source,
    from {{ ref("stg_nhanhvn__ordersdetails") }}
    where 1=1
    {% if is_incremental() %}
      and greatest(date(created_date), delivery_date, send_carrier_date) >= date_add(date(_dbt_max_partition), interval -2 day)
    {% endif %}
    and order_status IN (
        {# {%for status in order_statuses%} '{{status}}' {{',' if not loop.last}}{%endfor%} #}
        "Thành công"
    )
)
select 
    kiotviet_rev.*,
    {{dbt_utils.generate_surrogate_key(['transaction_id','source'])}} as transaction_source_id
from kiotviet_rev

union ALL

select 
    nhanhvn_rev.*,
    {{dbt_utils.generate_surrogate_key(['transaction_id','source'])}} as transaction_source_id
from nhanhvn_rev