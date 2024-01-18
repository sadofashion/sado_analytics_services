{{ config(
    tags = ['table','fact'],
) }}

WITH customer_data as (
    select 
        c.contact_number, 
        c.customer_id, 
        rfm.segment, 
        rfm.previous_segment, 
        rfm.start_of_month
    from {{ ref('stg_kiotviet__customers') }} c
    left join {{ ref('rfm_movement') }} rfm 
    on c.customer_id = rfm.customer_id
    where c.contact_number is not null
),

sms_sent_data AS (

    SELECT
        DATE_TRUNC(DATE(sent_time), MONTH) sent_month,
        sms.campaign,
        coalesce(c.segment,'Cold Data') segment,
        coalesce(c.previous_segment,'Cold Data') previous_segment,
        COUNT(
            DISTINCT sent_id
        ) AS sms_sent,
        COUNT(
            DISTINCT phone
        ) AS customer_sent,
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
            and (audience not in ('TUYEN DUNG') or audience is null)
    GROUP BY
        1,2,3,4
),

sms_revenue as (
    select 
        date_trunc(date(sent_time),month) sent_month,
        r.campaign,
        coalesce(c.segment,'Cold Data') segment,
        coalesce(c.previous_segment,'Cold Data') previous_segment,
        sum(total) total,
        count(distinct r.customer_id) as num_customer_converted,
    from {{ ref('fct__sms_revenue') }} r 
    left join customer_data c 
    on r.customer_id = c.customer_id 
    and date_trunc(date(sent_time),month) = c.start_of_month
    group by 1,2,3,4
)

select 
    ss.*, 
    sr.total, 
    sr.num_customer_converted
from sms_sent_data ss 
left join sms_revenue sr 
on ss.sent_month = sr.sent_month 
and ss.segment = sr.segment 
and ss.previous_segment = sr.previous_segment
and ss.campaign = sr.campaign
