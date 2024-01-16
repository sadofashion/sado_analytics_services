{{ config(
  tags = ['view','esms']
) }}

{% set sms_types ={ "1": "Tin quảng cáo",
"2": "Tin CSKH",
"8": "Tin Cố định giá rẻ",
"24": "Zalo ưu tiên",
"25": "Zalo bình thường",} %}

{% set sms_statuses ={ "1": "Chờ duyệt",
"2": "Chờ gửi",
"3": "Đang gửi",
"4": "Từ chối",
"5": "Thành công",} %}


WITH source AS (
  {{ dbt_utils.deduplicate(
    relation = source('esms', 'sms_sent_data'), 
    partition_by = 'smsid,phone', 
    order_by = "_batched_at desc",) 
    }}
)
SELECT
  phone,
  {{ dbt_utils.generate_surrogate_key(['phone','SmsId']) }} AS sent_id,
  coalesce(campaign,'QC||SINH NHAT') as campaign,
  referenceid AS reference_id,
  sellprice AS sms_cost,
  CASE
    sendstatus
    {% for key,status in sms_statuses.items() %}
    WHEN {{ key }} THEN "{{status}}"
    {% endfor %}
  END AS sent_status,
  sentresult AS sent_result,
  parse_datetime(
    '%d/%m/%Y %H:%M:%S',
    coalesce(senttime, min(senttime) over (partition by campaign))
  ) AS sent_time,
  smsid AS sms_id,
  CASE smstype
  {% for key, type in sms_types.items() %}
  WHEN {{ key }} THEN '{{type}}'
{% endfor %} END AS sms_type,
FROM
  source
