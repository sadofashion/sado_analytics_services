{{
  config(
    materialized = 'incremental',
    partition_by ={ 'field': 'last_purchase_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'merge',
    unique_key = ['customer_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'daily','fact','kiotviet','nhanhvn']
    )
}}

with updated_customers as (
    select distinct customer_id
    from {{ ref("fct__transactions") }}
    where customer_id is not null
    {% if is_incremental() %}
       and transaction_date >= date_add(current_date(), interval -7 day)
    {% endif %}
),

agg_metrics as (
    SELECT
    u.customer_id,
    COUNT(
        DISTINCT CASE
            WHEN transaction_type = 'invoice' THEN t.transaction_source_id
        END
    ) AS num_transactions,
    SUM(t.total) AS total_purchased_goods_value,
    SUM(t.total_payment) AS total_monetary_value,
    MAX(t.transaction_date) AS last_purchase_date,
    MIN(t.transaction_date) AS first_purchase_date,
    COUNT(
        CASE
            WHEN t.source = 'nhanhvn' THEN t.transaction_id
        END
    ) > 1 AS has_online_purchase,
FROM
    updated_customers u
    inner join {{ ref("fct__transactions") }} t on u.customer_id = t.customer_id
    {{ dbt_utils.group_by(1) }}
),

agg_dimensions as (
    SELECT
        u.customer_id,
        first_value(t.branch_id) over w1 as registered_branch_id,
        last_value(t.branch_id) over w1 as last_purchase_branch_id,
    FROM
        updated_customers u
    inner join {{ ref("fct__transactions") }} t on u.customer_id = t.customer_id
    qualify row_number() over (partition by u.customer_id order by t.transaction_date asc ) = 1

    window w1 AS (
        partition by u.customer_id order by t.transaction_date asc rows between unbounded preceding and unbounded following
    )
)

select 
    m.*,
    d.* except(customer_id),
from agg_metrics m 
inner join agg_dimensions d on m.customer_id = d.customer_id