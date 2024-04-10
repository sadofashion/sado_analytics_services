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

WITH customer_id_converter AS (

    SELECT
        kiotviet_customer_id,
        nhanhvn_customer_id
    FROM
        {{ ref("fct__customers") }}
    WHERE
        kiotviet_customer_id IS NOT NULL
        AND nhanhvn_customer_id IS NOT NULL
),
kiotviet_rev AS (
    SELECT
        invoices.transaction_id,
        invoices.transaction_code,
        DATE(
            invoices.transaction_date
        ) transaction_date,
        CAST(
            NULL AS int64
        ) AS reference_transaction_id,
        invoices.branch_id,
        invoices.customer_id,
        invoices.employee_id,
        invoices.total,
        invoices.total_payment,
        invoices.discount,
        invoices.discount_ratio,
        CAST(
            NULL AS int64
        ) AS return_fee,
        invoices.transaction_type,
        DATE(
            COALESCE(
                invoices.modified_date,
                invoices.transaction_date
            )
        ) AS modified_date,
        'kiotviet' AS source,
    FROM
        {{ ref('stg_kiotviet__invoices') }}
        invoices
    WHERE
        invoices.transaction_status = 'Hoàn thành'

{% if is_incremental() %}
{# AND date(invoices.transaction_date )in (
    select 
    distinct DATE(invoices.transaction_date) 
    from {{ ref('stg_kiotviet__invoices') }} 
    where DATE(invoices.modified_date)  >= date_add(DATE(_dbt_max_partition), INTERVAL -1 DAY)
) #}
and date(coalesce(invoices.modified_date,invoices.transaction_date)) >= date_add(DATE(_dbt_max_partition), INTERVAL -1 DAY)

{% endif %}
UNION ALL
SELECT
    returns.transaction_id,
    returns.transaction_code,
    DATE(
        returns.transaction_date
    ) transaction_date,
    returns.reference_transaction_id,
    returns.branch_id,
    returns.customer_id,
    returns.employee_id,
    (
        - returns.total
    ) AS total,
    returns.total_payment,
    returns.return_discount,
    CAST(
        NULL AS float64
    ) discount_ratio,
    returns.return_fee,
    returns.transaction_type,
    DATE(
        COALESCE(
            returns.modified_date,
            returns.transaction_date
        )
    ) AS modified_date,
    'kiotviet' AS source,
FROM
    {{ ref('stg_kiotviet__returns') }}
    returns
WHERE
    returns.transaction_status = 'Đã trả'

{% if is_incremental() %}
and date(coalesce(returns.modified_date,returns.transaction_date)) >= date_add(DATE(_dbt_max_partition), INTERVAL -1 DAY)
{# AND date(returns.transaction_date )in (
    select 
    distinct DATE(returns.transaction_date) 
    from {{ ref('stg_kiotviet__returns') }} 
    where DATE(returns.modified_date)  >= date_add(DATE(_dbt_max_partition), INTERVAL -1 DAY)
) #}
{% endif %}),
nhanhvn_rev AS (
    SELECT
        order_id AS transaction_id,
        shop_order_id AS transaction_code,
        COALESCE(delivery_date, send_carrier_date, DATE(created_date)) AS transaction_date,
        return_from_order_id AS reference_transaction_id,
        traffic_source_id AS branch_id,
        COALESCE(
            customer_id_converter.kiotviet_customer_id,
            customer_id
        ) AS customer_id,
        created_by_id AS employee_id,
        receivables AS total,
        (
            receivables + money_transfer - customer_ship_fee
        ) AS total_payment,
        order_discount AS discount,
        safe_divide(
            order_discount,
            order_discount + receivables
        ) AS discount_ratio,
        return_fee,
        CASE
            order_type
            WHEN 'Giao hàng tận nhà' THEN 'invoice'
            WHEN 'Khách trả lại hàng' THEN 'return'
        END AS transaction_type,
        COALESCE(
            delivery_date,
            send_carrier_date,
            DATE(created_date)
        ) AS modified_date,
        'nhanhvn' AS source,
    FROM
        {{ ref("stg_nhanhvn__ordersdetails") }} orders
        LEFT JOIN customer_id_converter
        ON orders.customer_id = customer_id_converter.nhanhvn_customer_id
    WHERE
        1 = 1
{% if is_incremental() %}
AND date(last_sync) >= date_add(DATE(_dbt_max_partition), INTERVAL -1 DAY)
{% endif %}
AND order_status IN (
    {# {%for status in order_statuses%} '{{status}}' {{',' if not loop.last}}{%endfor%} #}
    "Thành công"
)
AND order_type IS NOT NULL
)
SELECT
    kiotviet_rev.*,
    {{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
FROM
    kiotviet_rev
UNION ALL
SELECT
    nhanhvn_rev.*,
    {{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
FROM
    nhanhvn_rev
