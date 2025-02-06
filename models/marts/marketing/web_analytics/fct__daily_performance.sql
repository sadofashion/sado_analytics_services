{{
  config(
    materialized = 'table',
    )
}}

with web_metrics as (

    select 
    session_start_date as date,
    count(distinct session_key) as sessions,
    sum(count_purchase) as purchases,
    sum(count_message) as messages,
    sum(count_call) as calls,
    sum(count_generate_lead) as leads,
    sum(count_add_to_cart) as add_to_cart,
    sum(count_pageviews) as page_views,
    sum(sum_engaged_time_msec)/1000 as engagement_time,
    count(distinct case when count_add_to_cart >0 then session_key end) as sessions_with_add_to_cart,
    count(distinct case when is_session_engaged =1 then session_key end) as engaged_sessions,

    from {{ ref("fct__sessions") }}
    group by 1
),
web_conversion as (
  select 
  date(transaction_date) as date,
  count(distinct code) as orders,
  count(distinct case when status <> "Cancelled" then code end ) as confirmed_orders,
  count(distinct case when status = "Cancelled" then code end ) as cancelled_orders,
  sum(total) as recorded_revenue,
  sum(case when status <> "Cancelled" then total end) as confirmed_revenue,
  from {{ ref("stg_5sfashion__orders") }} 
  group by 1
)

select 
w.*, c.* except(date)
from web_metrics w
left join web_conversion c on w.date = c.date 