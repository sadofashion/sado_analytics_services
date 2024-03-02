{{
  config(
    tags=['view', 'forecaster']
  )
}}
WITH source AS (
        {{ dbt_utils.deduplicate(
        relation = source(
            'forecaster',
            'forecast'
        ),
        partition_by = 'forecast_date',
        order_by = "forecast_date",
    ) }}
)

select 
forecast_date,
estimates.date as date_in_future,
estimates.mean_hat as mean_estimated,
estimates.mean_ci_upper_hat as mean_upper_estimated,
estimates.mean_ci_lower_hat as mean_lower_estimated,
exponential_smoothing.forecast as forecasted_value
from source
left join unnest(estimates) estimates
left join unnest(exponential_smoothing) exponential_smoothing
where estimates.date = exponential_Smoothing.date