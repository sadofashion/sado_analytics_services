
{{ config(
    tags = ['incremental', 'hourly','fact','nhanhvn']
) }}

SELECT
    invoices.*,
    {{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
FROM
    {{ ref("int_kiotviet__invoices") }} invoices
UNION ALL
SELECT
    returns.*,
    {{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
FROM
    {{ ref("int_kiotviet__returns") }} returns
UNION ALL
SELECT
    order_details.*,
    {{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
FROM
    {{ ref("int_nhanhvn__ordersdetails") }} order_details
