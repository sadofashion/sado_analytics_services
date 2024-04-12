{{
  config(
    materialized = 'incremental',
    unique_key = ['id'],
    )
}}

{%set events = ['purchase','view_item','add_to_cart']%}


with event_format as (
select e.* except(event_name),
/*
    begin_checkout event actually another add_to_cart (nothing different in term of operation)
  */
  case 
    when e.event_name = 'begin_checkout' then 'add_to_cart' 
    else e.event_name end as event_name,
from {{ ref("stg_ga4__event_items") }} e
left join {{ ref('dim__excluded_clients') }} ex on e.client_key = ex.client_key
where e.event_name in ( '{{ events|join("','") }}','begin_checkout')
and ex.client_key is null
{% if is_incremental() %}
  and e.event_date_dt >= date_add(current_date, interval -1 day)
{% endif %}
)

select
  item_id,
  client_key,
  session_key,
  event_date_dt,

  item_variant,

  coupon,
  promotion_name,
  payment_type,
  delivery_method,
  {%for event in events%}
  count(distinct case when event_name = '{{event}}' then event_key end) as num_{{event}},
  sum(case when event_name = '{{event}}' then quantity end) as total_{{event}}_quantity,
  avg(case when event_name = '{{event}}' then price end) as avg_{{event}}_price,
  {%endfor%}
  
  count(distinct transaction_id) as num_orders,
  sum(item_revenue) as total_item_revenue,
  sum(shipping) as total_shipping,
  sum(tax) as total_tax,

from event_format

{{dbt_utils.group_by(9)}}
