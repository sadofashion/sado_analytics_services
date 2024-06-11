{{ config(
    materialized = 'table',
    partition_by ={ 'field': 'sent_time',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    unique_key = ['campaign','phone', 'sent_time', 'transaction_date'],
    on_schema_change = 'sync_all_columns',
    tags = ['table', 'fact','sms','daily']
) }}

WITH revenue AS (
    SELECT
        r.customer_id,
        C.contact_number,
        DATE(r.transaction_date) transaction_date,
        SUM(r.total) total,
    FROM
        {{ ref('fct__transactions') }} r
        LEFT JOIN {{ ref('fct__customers') }} C
        ON r.customer_id = C.universal_customer_id
        left join {{ ref("dim__branches") }} b on r.branch_id = b.branch_id
    WHERE
        r.transaction_type = 'invoice'
        AND C.contact_number IS NOT NULL
        and b.channel = 'Offline'
        {% if is_incremental() %}
        AND DATE(r.transaction_date) >= date_add(current_date, INTERVAL -15 DAY)
        {% endif %}
{{dbt_utils.group_by(3)}}
    ),
    sms_data AS (
        SELECT
            *
        FROM
            {{ ref('stg_esms__sent_data') }}
            sms
        WHERE
            sms.sent_time IS NOT NULL
            AND sent_status = 'Thành công'
            and (audience not in ('TUYEN DUNG','THONG BAO DON HANG') or audience is null)
        {% if is_incremental() %}
        AND DATE(sms.sent_time) >= date_add(current_date, INTERVAL -7 DAY)
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
    DATE(sms.sent_time) < revenue.transaction_date 
    AND (
        revenue.transaction_date  <= DATE(sms.end_date)  or date_diff(revenue.transaction_date, DATE(sms.sent_time), DAY) <= 10 
    )

qualify ROW_NUMBER() over( PARTITION BY revenue.customer_id, revenue.transaction_date ORDER BY sms.sent_time DESC) = 1
