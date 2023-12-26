{{ config(
  tags = ['view','esms']
) }}

{%set sms_types = {
  "1": "Tin quảng cáo",
  "2": "Tin CSKH",
  "8": "Tin Cố định giá rẻ",
  "24": "Zalo ưu tiên",
  "25": "Zalo bình thường",
}%}

{%set sms_statuses = {
  "1": "Chờ duyệt",
  "2": "Chờ gửi",
  "3": "Đang gửi",
  "4": "Từ chối",
  "5": "Thành công",
}%}

WITH source AS (
  {{ 
    dbt_utils.deduplicate(
      relation = source('esms', 'sms_sent_data'), 
      partition_by = 'smsid,phone,senttime',
      order_by = "_batched_at desc",
      ) 
     }}
)
SELECT
  phone,
  {{ dbt_utils.generate_surrogate_key(['phone','SmsId','senttime']) }} as sent_id,
  ReferenceId as reference_id,
  SellPrice as sms_cost,
  case sendstatus 
  {%for key,status in sms_statuses.items()%}
  when {{key}} then "{{status}}"
  {%endfor%} end as sent_status,
  SentResult as sent_result,
  parse_datetime('%d/%m/%Y %H:%M:%S',senttime) as sent_time,
  SmsId as sms_id,
  case smstype 
  {%for key,type in sms_types.items()%}
  when {{key}} then '{{type}}'
  {%endfor%} end as sms_type,
from 
source
