{{ config(
    materialized = 'table',
    tags = ['table','fact','marketing','calls']
) }}

{% set rev_calcols ={ "transaction_id" :"count(distinct ",
"total" :"sum(",
"total_payment" :"sum(" } %}
{% set rev_types = ["invoice", "return"] %}

WITH calls AS (

    SELECT
        cs.call_status,
        cs.call_month,
        c.kiotviet_customer_id AS customer_id,
        cs.branch_name,
    FROM
        {{ ref("stg_gsheet__cs_calls") }}
        cs
        INNER JOIN {{ ref('fct__customers') }} C
        ON cs.customer_phone = c.contact_number
        AND kiotviet_customer_id IS NOT NULL
),
transactions AS (
    SELECT
        customer_id,
        transaction_date,
        branch_id, 
        {% for col,cal in rev_calcols.items() %}
            {{ cal +" " +col + ")" }} AS {{ "val_"+col }},
            {% for type in rev_types %}
                {{ cal }} CASE
                    WHEN transaction_type = '{{type}}' THEN {{ col }}
                END {{ ")" }} AS {{ "num_"+type +"_"+col}},
            {% endfor %}
        {% endfor %}
    FROM
        {{ ref("fct__transactions") }}
    WHERE
        source = 'kiotviet'
        AND transaction_date >= '2024-10-01'
    {{dbt_utils.group_by(3)}}
)
SELECT
    c.call_status,
    c.call_month,
    c.customer_id AS customer_id,
    c.branch_name,
    t.* except(customer_id)
FROM
    calls c
    left join transactions t
    ON c.customer_id = t.customer_id
    AND lower(c.call_status) NOT IN (
        "không nghe máy",
        "từ chối",
        "thuê bao"
    )
    AND (
        (t.transaction_date >= c.call_month
            AND t.transaction_date < date_add(c.call_month, INTERVAL 1 MONTH))
        OR t.transaction_date IS NULL
    )
