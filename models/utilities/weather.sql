WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('misc', 'weather'), partition_by = 'lat,long,time', order_by = "_batched_at desc",) }}
),

source_hourly AS (
    {{ dbt_utils.deduplicate(relation = source('misc', 'weather_hourly'), partition_by = 'lat,long,time', order_by = "_batched_at desc",) }}
),

hourly as (
    select date(source_hourly.time) as date,
    extract(hour from timestamp(source_hourly.time)) as hour,
    source_hourly.rain,
    source_hourly.temperature2m,
    source_hourly.lat,
    source_hourly.long,
    from source_hourly
),

hourly_aggregate as (
    select 
    hourly.date,
    hourly.lat,
    hourly.long,
    count( distinct case when hourly.rain>=3 then hourly.hour end) as raining_hour,
    sum(hourly.rain) as rain_sum,
    avg(hourly.temperature2m) as avg_temp,
    from hourly
    where hourly.hour between 9 and 23
    group by 1,2,3
)

SELECT
    DATE(
        source.time
    ) AS date,
    source.temperature2mMax AS temperature_2m_max,
    source.temperature2mMin AS temperature_2m_min,
    source.temperature2mMean AS temperature_2m_mean,
    source.apparentTemperatureMax AS apparent_temperature_max,
    source.apparentTemperatureMin AS apparent_temperature_min,
    source.apparentTemperatureMean AS apparent_temperature_mean,
    source.rainSum AS rain_sum,
    source.windSpeed10mMax AS wind_speed10m_max,
    source.lat AS lat,
    source.long AS long,
    safe_divide(hourly_aggregate.raining_hour,14) as rain_pct,
    hourly_aggregate.rain_sum as selling_hour_rain,
    hourly_aggregate.avg_temp as selling_hour_avg_temp,
FROM
    source
    left join hourly_aggregate 
    on source.lat= hourly_aggregate.lat 
    and source.long = hourly_aggregate.long 
    and DATE(source.time) = hourly_aggregate.date