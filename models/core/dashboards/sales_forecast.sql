WITH revenue_metrics AS (
    SELECT
        DATE,
        SUM(num_invoice_transaction_id) AS num_invoices,
        SUM(num_stores) AS num_stores,
        safe_divide(SUM(num_invoice_total_payment), SUM(num_invoice_transaction_id)) AS daily_aov,
    FROM
        {{ ref('sales_dashboard') }} s 
        left join {{ ref('dim__offline_stores') }} b on s.branch_id = b.branch_id
    WHERE
        DATE >= '2024-01-01'
        and b.asm_name is not null
    GROUP BY
        1
),
forecast AS (
    SELECT
        forecast_date,
        date_in_future,
        mean_estimated,
        mean_upper_estimated,
        mean_lower_estimated,
        forecasted_value
    FROM
        {{ ref('stg_forecaster__forecast') }}
    WHERE
        forecast_date >= '2024-01-01'
),
daily_forecasted_values AS (
    SELECT
        r.date,
        r.num_invoices,
        r.num_stores,
        r.daily_aov,
        f.mean_estimated,
        f.mean_lower_estimated,
        f.mean_upper_estimated,
        f.forecasted_value
    FROM
        revenue_metrics r
        LEFT JOIN forecast f
        ON r.date = f.forecast_date
        AND r.date = f.date_in_future
),
fureture_values AS (
    SELECT
        *
    FROM
        daily_forecasted_values
    UNION ALL
    SELECT
        date_in_future,
        forecasted_value,
        CAST(
            NULL AS numeric
        ) AS num_stores,
        CAST(
            NULL AS numeric
        ) AS daily_aov,
        mean_estimated,
        mean_lower_estimated,
        mean_upper_estimated,
        forecasted_value
    FROM
        forecast
    WHERE
        date_in_future > CURRENT_DATE()
        AND forecast_date = CURRENT_DATE()
)

select 
* except(num_stores,daily_aov),
coalesce(
    num_stores,
    first_value(num_stores ignore nulls) over(order by date desc rows between current row and 10 following)
) num_stores,
coalesce(daily_aov,
avg(daily_aov) over (order by unix_date(date) desc range between current row and 30 following )
) daily_aov
from fureture_values
