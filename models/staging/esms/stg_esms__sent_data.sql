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
"5": "Thành công",
"7": "Thất bại",} %}
WITH source AS (
  {{ dbt_utils.deduplicate(relation = source('esms', 'sms_sent_data'), partition_by = 'smsid,phone', order_by = "_batched_at desc",) }}
)
SELECT
  phone,
  {{ dbt_utils.generate_surrogate_key(['phone','SmsId']) }} AS sent_id,
  case when smstype = 25 and regexp_contains(content,r'^\(.*\)$')
  then 'CSKH||THONG BAO DON HANG' else
  COALESCE(
    campaign,
    'QC||SINH NHAT'
  ) end AS campaign,
  referenceid AS reference_id,
  sellprice AS sms_cost,
  CASE
    sendstatus
    {% for key,status in sms_statuses.items() %}
    WHEN {{ key }} THEN "{{status}}"
    {% endfor %}
  END AS sent_status,
  sentresult AS sent_result,
  COALESCE(
    DATE(
      parse_datetime(
        '%d/%m/%Y %H:%M:%S',
        COALESCE(senttime, MIN(senttime) over (PARTITION BY campaign))
      )
    ),
    DATE(
      SPLIT(regexp_extract(campaign, r'\|\|([0-9\-_]+)\|\|'), '_') [offset(0)]
    )
  ) AS sent_time,
  smsid AS sms_id,
  CASE
    smstype
    {% for key,type in sms_types.items() %}
    WHEN {{ key }} THEN '{{type}}'
    {% endfor %}
  END AS sms_type,
  SPLIT(regexp_extract(campaign, r'\|([0-9\-_]+)\|'), '_') [offset(0)] AS start_date,
  SPLIT(regexp_extract(campaign, r'\|([0-9\-_]+)\|'), '_') [offset(1)] AS end_date,
  CASE
    when regexp_contains(lower(content),r'phong van|chon loc ho so|ung tuyen') then 'TUYEN DUNG'
    when smstype = 25 and regexp_contains(content,r'^\(.*\)$') then 'THONG BAO DON HANG'
    WHEN campaign IS NULL THEN 'SINH NHAT'
    ELSE regexp_extract(
      campaign,
      r'\|-\s?(.*)$'
    )
  END AS audience
FROM
  source
