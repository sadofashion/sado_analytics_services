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
  CASE
    WHEN smstype = 25
    AND regexp_contains(
      content,
      r'^\(.*\)$'
    ) THEN 'CSKH||THONG BAO DON HANG'
    WHEN campaign IN (
      'Chiến dịch 02/02/2024',
      'Chiến dịch 01/02/2024'
    ) THEN 'QC||FLASHSALES'
    ELSE COALESCE(
      campaign,
      'QC||SINH NHAT'
    )
  END AS campaign,
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
      regexp_extract_all(campaign,r'\d{4}-\d{2}-\d{2}')[safe_offset(0)]
    )
  ) AS sent_time,
  smsid AS sms_id,
  CASE
    smstype

    {% for key,type in sms_types.items() %}
    WHEN {{ key }} THEN '{{type}}'
    {% endfor %}
  END AS sms_type,
  CASE
    WHEN campaign IN (
      'Chiến dịch 02/02/2024',
      'Chiến dịch 01/02/2024'
    ) THEN '2024-02-02'
    WHEN campaign IN (
      'KM tháng 3 - 8000 ngày 1',
      'KM T3 dot 2- 8000'
    ) THEN '2024-03-01'
    ELSE regexp_extract_all(campaign,r'\d{4}-\d{2}-\d{2}')[safe_offset(0)]
  END AS start_date,
  CASE
    WHEN campaign IN (
      'Chiến dịch 02/02/2024',
      'Chiến dịch 01/02/2024'
    ) THEN '2024-02-07' 
    WHEN campaign IN (
      'KM tháng 3 - 8000 ngày 1',
      'KM T3 dot 2- 8000'
    ) THEN '2024-03-03'
    ELSE coalesce(regexp_extract_all(campaign,r'\d{4}-\d{2}-\d{2}')[safe_offset(1)],regexp_extract_all(campaign,r'\d{4}-\d{2}-\d{2}')[safe_offset(0)])
  END AS end_date,
  CASE
    WHEN regexp_contains(LOWER(content), r'phong van|chon loc ho so|ung tuyen') THEN 'TUYEN DUNG'
    WHEN smstype = 25
    AND regexp_contains(
      content,
      r'^\(.*\)$'
    ) THEN 'THONG BAO DON HANG'
    WHEN campaign IS NULL THEN 'SINH NHAT'
    ELSE regexp_extract(
      campaign,
      r'\|-\s?(.*)$'
    )
  END AS audience
FROM
  source
