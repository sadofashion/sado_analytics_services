{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'sent_time',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'fact','dashboard','daily']
) }}


WITH customer_data as (
    select 
        c.contact_number, 
        c.customer_id, 
        rfm.segment, 
        rfm.previous_segment, 
        rfm.start_of_month,
        rfm.first_purchase,
    from {{ ref('stg_kiotviet__customers') }} c
    left join {{ ref('rfm_movement') }} rfm 
    on c.customer_id = rfm.customer_id
    where c.contact_number is not null
    {% if is_incremental() -%}
      and rfm.start_of_month >= date_trunc(current_date, month)
    {% endif -%}
),

sms_sent_data AS (

    SELECT
        {# DATE_TRUNC(DATE(sent_time), MONTH) sent_month, #}
        DATE(sent_time) sent_time,
        sms.campaign,
        coalesce(c.segment,'Cold Data') segment,
        case 
            when (c.previous_segment = 'First-time Purchaser' and date(c.first_purchase) >= DATE(sent_time)) or c.first_purchase is null then 'Cold Data' 
        else c.previous_segment end previous_segment,
        COUNT(DISTINCT sent_id) AS sms_sent,
        COUNT(DISTINCT phone) AS customer_sent,
        SUM(sms_cost) sms_cost,
    FROM
        {{ ref("stg_esms__sent_data") }} sms
    left join customer_data c 
    on sms.phone = c.contact_number 
    and DATE_TRUNC(DATE(sent_time), MONTH) = c.start_of_month
    WHERE
        sms.sent_time IS NOT NULL
            {# AND sms.campaign LIKE 'QC%' #}
        AND sent_status = 'Thành công'
        and (audience not in ('TUYEN DUNG','THONG BAO DON HANG') or audience is null)
        {% if is_incremental() -%}
        and sms.sent_time >= date_trunc(current_date, month)
        {% endif -%}
    {{dbt_utils.group_by(4)}}
),

sms_revenue as (
    select 
        {# date_trunc(date(sent_time),month) sent_month, #}
        date(sent_time) sent_time,
        r.campaign,
        coalesce(c.segment,'Cold Data') segment,
        case when c.previous_segment = 'First-time Purchaser' and date(c.first_purchase) >= DATE(sent_time) then 'Cold Data' else c.previous_segment end previous_segment,
        sum(total) total,
        count(distinct r.customer_id) as num_customer_converted,
    from {{ ref('fct__sms_revenue') }} r 
    left join customer_data c 
    on r.customer_id = c.customer_id 
    and date_trunc(date(sent_time),month) = c.start_of_month
    {% if is_incremental() -%}
        where r.sent_time >= date_trunc(current_date, month)
    {% endif -%}
    {{dbt_utils.group_by(4)}}
)

select 
    ss.*, 
    sr.total, 
    sr.num_customer_converted
from sms_sent_data ss 
left join sms_revenue sr 
on ss.sent_time = sr.sent_time 
and ss.segment = sr.segment 
and ss.previous_segment = sr.previous_segment
and ss.campaign = sr.campaign
