{{
  config(
    tags=['table','dimension','sms']
    )
}}

SELECT
    DISTINCT campaign,
    date(start_date) start_date,
    date(end_date) end_date,
    audience
FROM
    {{ ref('stg_esms__sent_data') }}
{# WHERE
    campaign LIKE 'QC%' #}
