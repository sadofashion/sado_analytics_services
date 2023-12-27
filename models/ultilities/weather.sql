WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('misc', 'weather'), partition_by = 'lat,long,time', order_by = "_batched_at desc",) }}
)

{%set ranges = [10,15,30]%}

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
    {%for range in ranges%}
    AVG(
        source.apparentTemperatureMean
    ) over w{{range}}d as apparent_temperature_mean_{{range}}d,
    AVG(
        source.temperature2mMean
    ) over w{{range}}d as temperature_2m_mean_{{range}}d,
    {%endfor%}
    
    source.rainSum AS rain_sum,
    source.windSpeed10mMax AS wind_speed10m_max,
    source.lat AS lat,
    source.long AS long,
FROM
    source
    window
    {%for range in ranges%}
    w{{range}}d AS (
        PARTITION BY concat(lat,long)
        ORDER BY
            unix_date(DATE(source.time)) ASC RANGE BETWEEN {{range}} preceding
            AND CURRENT ROW
    ) {{',' if not loop.last}}
    {%endfor%}