{{ config(
    tags = ['view','tiktok']
) }}

WITH source AS (
    SELECT
        json_extract_array(data, '$.info.country_code') AS country_code,
        json_value(data, '$.info.create_time') AS create_time,
        json_value(data, '$.info.currency') AS currency,
        json_value(data, '$.info.identity') AS identity,
        json_value(data, '$.info.material_id') AS material_id,
        json_value(data, '$.info.material_name') AS material_name,
        json_extract_array(data, '$.info.placement') AS placement,
        json_value(data, '$.info.video_material_source') AS video_material_source,
        safe_cast(json_value(data, '$.metrics.clicks') as float64) AS clicks,
        safe_cast(json_value(data, '$.metrics.conversion') as float64) AS conversion,
        safe_cast(json_value(data, '$.metrics.conversion_rate_v2') as float64) AS conversion_rate_v2,
        safe_cast(json_value(data, '$.metrics.cost_per_conversion') as float64) AS cost_per_conversion,
        safe_cast(json_value(data, '$.metrics.cpc') as float64) AS cpc,
        safe_cast(json_value(data, '$.metrics.cpm') as float64) AS cpm,
        safe_cast(json_value(data, '$.metrics.ctr') as float64) AS ctr,
        safe_cast(json_value(data, '$.metrics.impressions') as float64) AS impressions,
        safe_cast(json_value(data, '$.metrics.organic_video_views') as float64) AS organic_video_views,
        safe_cast(json_value(data, '$.metrics.spend') as float64) AS spend,
        _batched_at,
    FROM
        {{ source('tiktok','video_insight') }}
) 

{{ dbt_utils.deduplicate(
    relation = "source",
    partition_by = "material_id",
    order_by = "_batched_at desc"
) }}
