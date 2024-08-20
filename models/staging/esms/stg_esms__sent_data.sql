{{ config(
  tags = ['incremental','esms','daily'],
  materialized ="incremental",
  unique_key = 'sent_id',
  on_schema_change = 'sync_all_columns',
  incremental_strategy = 'insert_overwrite',
  partition_by = {
    "field": "sent_time",
    "data_type": "date",
    "granularity": "day"
  }
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
WITH 
increment as (
  select * except(sentresult) from 
  {{source('esms', 'sms_sent_data')}}
  where (campaign not in ('ZNS||2024-03-15-2024-17-03|| WK 84 LL - 5S mua hàng 6 tháng') or campaign is null)
  {% if is_incremental() %}
   and parse_date('%Y%m%d', _TABLE_SUFFIX) >= date_add(current_date, interval -1 day)
  {% endif %}
  union all 
  select * except(sentresult) from 
  {{source('esms', 'sms_sent_data_history')}}
  where (campaign not in ('ZNS||2024-03-15-2024-17-03|| WK 84 LL - 5S mua hàng 6 tháng') or campaign is null)
  {% if is_incremental() %}
   and parse_date('%Y%m%d', _TABLE_SUFFIX) >= date_add(current_date, interval -1 day)
  {% endif %}
),

source AS (
  {{ dbt_utils.deduplicate(relation = 'increment', partition_by = 'smsid,phone', order_by = "_batched_at desc",) }}
)
SELECT
  phone,
  {{ dbt_utils.generate_surrogate_key(['phone','SmsId']) }} AS sent_id,
  CASE
    WHEN smstype = 25 AND regexp_contains(content,r'^\(.*\)$') THEN 'CSKH||THONG BAO DON HANG'
    WHEN campaign IN ('Chiến dịch 02/02/2024','Chiến dịch 01/02/2024') THEN 'QC||FLASHSALES'
    ELSE COALESCE(campaign,'QC||KHONG PHAN LOAI')
  END AS campaign,
  referenceid AS reference_id,
  sellprice AS sms_cost,
  CASE
    sendstatus
    {% for key,status in sms_statuses.items() -%}
    WHEN {{ key }} THEN "{{status}}"
    {% endfor -%}
  END AS sent_status,
  {# sentresult AS sent_result, #}
  CASE
    when senttime like "/Date%" then date(timestamp_millis(safe_cast(regexp_extract(senttime,r'(\d+)\+') as int64)),"Asia/Saigon")
    WHEN campaign IN ('Chiến dịch 02/02/2024','Chiến dịch 01/02/2024') THEN date('2024-02-02')
    WHEN campaign IN ('KM tháng 3 - 8000 ngày 1','KM T3 dot 2- 8000') THEN date('2024-03-01')
    WHEN campaign IN ("QC||2024-02-26-2024-03-03|| CT DON KHO - KH 3 THANG") THEN date('2024-03-07')
    when senttime is not null then date(parse_datetime('%d/%m/%Y %H:%M:%S',COALESCE(senttime, MIN(senttime) over (PARTITION BY campaign))))
    else date(regexp_extract_all(campaign,r'\d{4}-\d{2}-\d{2}')[safe_offset(0)])
  END as sent_time,
  smsid AS sms_id,
  CASE
    smstype
    {% for key,type in sms_types.items() -%}
    WHEN {{ key }} THEN '{{type}}'
    {% endfor -%}
  END AS sms_type,
  CASE
    WHEN campaign IN ('Chiến dịch 02/02/2024','Chiến dịch 01/02/2024') THEN '2024-02-02'
    WHEN campaign IN ('KM tháng 3 - 8000 ngày 1','KM T3 dot 2- 8000') THEN '2024-03-01'
    WHEN campaign IN ("QC||2024-02-26-2024-03-03|| CT DON KHO - KH 3 THANG") THEN '2024-03-07'
    ELSE regexp_extract_all(campaign,r'\d{4}-\d{2}-\d{2}')[safe_offset(0)]
  END AS start_date,
  CASE
    WHEN campaign IN ('Chiến dịch 02/02/2024','Chiến dịch 01/02/2024') THEN '2024-02-07' 
    WHEN campaign IN ('KM tháng 3 - 8000 ngày 1','KM T3 dot 2- 8000') THEN '2024-03-03'
    WHEN campaign IN ("QC||2024-02-26-2024-03-03|| CT DON KHO - KH 3 THANG") THEN '2024-03-17'
    ELSE coalesce(regexp_extract_all(campaign,r'\d{4}-\d{2}-\d{2}')[safe_offset(1)],regexp_extract_all(campaign,r'\d{4}-\d{2}-\d{2}')[safe_offset(0)])
  END AS end_date,
  CASE
    WHEN regexp_contains(LOWER(content), r'phong van|chon loc ho so|ung tuyen') THEN 'TUYEN DUNG'
    WHEN smstype = 25 AND regexp_contains(content,r'^\(.*\)$') THEN 'THONG BAO DON HANG'
    WHEN campaign IS NULL THEN 'KHONG PHAN LOAI'
    ELSE regexp_extract(campaign,r'\|-\s?(.*)$')
  END AS audience
FROM
  source
