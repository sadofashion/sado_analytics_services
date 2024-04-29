{{ config(
    materialized = 'incremental',
    on_schema_change = 'sync_all_columns',
    partition_by = {'field': 'transaction_month', 'data_type': 'date', 'granularity': 'month'},
    incremental_strategy = 'insert_overwrite',
    tags = ['incremental','dashboard','fact','daily'],
    unique_key = ['customer_id','transaction_month']
) }}

{% set cohort_age_ranges ={ "0. T0" :"0 and 0",
                            "1. 1-3m" :"1 and 3",
                            "2. 4-6m" :"4 and 6",
                            "3. 7-9m" :"7 and 9",
                            "4. 10-12m" :"10 and 12",
                            "5. 1-1.5y": "13 and 18",
                            "6. 1.5-2y": "19 and 24",
                            "7. 2-3y": "25 and 36",
                            } %}
WITH joined_tables AS (
    SELECT
        t.customer_id,
        DATE_TRUNC(t.transaction_date,MONTH) transaction_month,
        DATE_TRUNC(r.first_purchase,MONTH) first_purchase_month,
        date_diff(t.transaction_date,r.first_purchase,MONTH) AS cohort_age,
        r.segment,
        r.previous_segment
    FROM
        {{ ref("fct__transactions") }} t
        LEFT JOIN {{ ref("rfm_movement") }} r
        ON t.customer_id = r.customer_id
        AND r.start_of_month = DATE_TRUNC(CURRENT_DATE, MONTH)
        where t.transaction_type = 'invoice'
        and t.transaction_code not like 'HDD_%'
        {% if is_incremental() %}
        AND t.transaction_date >= date_trunc(current_date,month)
        {% endif %}
)
{# , #}

{# create_age_group as ( #}
    SELECT
    *,
    CASE
    {% for group,condition in cohort_age_ranges.items() %}
        WHEN cohort_age BETWEEN {{ condition }} THEN '{{group}}'
    {% endfor %}
        ELSE '8. 3y+'
    END AS cohort_age_group
FROM
    joined_tables
    {{dbt_utils.group_by(7)}}
{# )
select 
    first_purchase_month,
    transaction_month,
    segment, 
    previous_segment, 
    cohort_age, 
    cohort_age_group,
    count(distinct customer_id) as num_customers,
    count(customer_id) as num_transactions
from create_age_group
{{dbt_utils.group_by(6)}} #}