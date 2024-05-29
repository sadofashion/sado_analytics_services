
{{ config(
    tags = [ 'hourly','fact','kiotviet','nhanhvn']
) }}

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
FROM
    {{ ref("int_kiotviet__invoices") }}

UNION ALL

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
FROM
    {{ ref("int_kiotviet__returns") }}

UNION ALL

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['transaction_id','source']) }} AS transaction_source_id
FROM
    {{ ref("int_nhanhvn__ordersdetails") }}
