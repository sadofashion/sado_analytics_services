{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'sent_time',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'merge',
    unique_key = ['campaign','phone', 'sent_time', 'transaction_date'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'fact','sms']
) }}

WITH revenue AS (

    SELECT
        r.customer_id,
        C.contact_number,
        DATE(
            r.transaction_date
        ) transaction_date,
        SUM(
            r.total
        ) total,
    FROM
        {{ ref('revenue') }}
        r
        LEFT JOIN {{ ref('stg_kiotviet__customers') }} C
        ON r.customer_id = C.customer_id
    WHERE
        r.transaction_type = 'invoice'
        AND C.contact_number IS NOT NULL

{% if is_incremental() %}
AND DATE(r.transaction_date) >= date_add(DATE(_dbt_max_partition), INTERVAL -7 DAY)
{% endif %}
GROUP BY
    1,
    2,
    3),
    sms_data AS (
        SELECT
            *
        FROM
            {{ ref('stg_esms__sent_data') }}
            sms
        WHERE
            sms.sent_time IS NOT NULL
            {# AND sms.campaign LIKE 'QC%' #}
            AND sent_status = 'Thành công'
            and (audience not in ('TUYEN DUNG') or audience is null)

{% if is_incremental() %}
AND DATE(
    sms.sent_time
) >= date_add(DATE(_dbt_max_partition), INTERVAL -7 DAY)
{% endif %}
)
SELECT
    sms.* except(start_date, end_date,audience),
    revenue.transaction_date,
    revenue.total,
    revenue.customer_id,
FROM
    sms_data sms
    LEFT JOIN revenue
    ON sms.phone = revenue.contact_number
WHERE
    DATE(
        sms.sent_time
    ) < revenue.transaction_date 
    AND (
        revenue.transaction_date  <= DATE(sms.end_date) 
    or date_diff(revenue.transaction_date, DATE(sms.sent_time), DAY) <= 10 
    )

qualify ROW_NUMBER() over( PARTITION BY revenue.customer_id, revenue.transaction_date ORDER BY sms.sent_time DESC) = 1
