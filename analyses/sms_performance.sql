

WITH rfm_ as (
    select customer_id,start_of_month, segment,
    coalesce(lag(segment) over (partition by customer_id order by start_of_month asc),'First-time Purchaser') prev_segment
    from {{ ref('rfm_movement') }}
    where start_of_month >='2022-12-01'
),

sms AS (

    SELECT
        DISTINCT s.phone,
        C.customer_id,
        DATE(
            s.sent_time
        ) sent_time,
        r.segment,
        r.prev_segment,
        s.sms_cost,
    FROM
        {{ ref('stg_esms__sent_data') }}
        s
        LEFT JOIN {{ ref('stg_kiotviet__customers') }} C
        ON s.phone = C.contact_number
        LEFT JOIN rfm_ r
        ON C.customer_id = r.customer_id
        AND DATE_TRUNC(
            s.sent_time,
            MONTH
        ) = r.start_of_month
),
revenue AS (
    SELECT
        DATE(transaction_date) transaction_date,
        customer_id,
        COUNT(
            DISTINCT transaction_id
        ) AS num_invoice,
        SUM(
            total
        ) AS total_invoice_value,
    FROM
        {{ ref('revenue') }}
    WHERE
        transaction_type = 'invoice'
        and transaction_date>='2023-01-01'
    GROUP BY
        1,
        2
),
performance as (
    SELECT
        sms.*,
        revenue.num_invoice,
        revenue.total_invoice_value,
        concat()
        date_diff(
            revenue.transaction_date,
            sms.sent_time,
            DAY
        ) AS days_lag,
    FROM
        sms
        LEFT JOIN revenue
        ON sms.sent_time < revenue.transaction_date
        AND sms.customer_id = revenue.customer_id
        AND date_diff(
            revenue.transaction_date,
            sms.sent_time,
            DAY
        ) <= 7
    
)
SELECT
    prev_segment,
    avg(days_lag) days_lag,
    SUM(num_invoice) num_invoice,
    SUM(total_invoice_value) total_invoice_value,
    count(phone) as num_sent,
    sum(sms_cost) sms_cost,
FROM
    performance
group by 1