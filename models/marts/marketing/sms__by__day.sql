{{config(
    tags=['table','fact'],
)}}

select
    date(sent_time) sent_date,
    count(distinct sent_id) as sms_sent,
    count(distinct phone) as customer_sent,
    sum(sms_cost) sms_cost,
from {{ref("stg_esms__sent_data")}}
where sent_status = 'Thành công'
group by 1