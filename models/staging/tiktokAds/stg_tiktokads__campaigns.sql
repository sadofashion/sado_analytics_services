{{ config(
    tags = ['view','tiktok']
) }}


WITH source AS (

    SELECT
        json_value(DATA,'$.advertiser_id') AS advertiser_id,
        json_value(DATA,'$.campaign_id') AS campaign_id,
        json_value(DATA,'$.campaign_name') AS campaign_name,
        json_value(DATA,'$.campaign_type') AS campaign_type,
        json_value(DATA,'$.objective') AS objective,
        json_value(DATA,'$.objective_type') AS objective_type,
        json_value(DATA,'$.operation_status') AS operation_status,
        json_value(DATA,'$.secondary_status') AS secondary_status,
        json_value(DATA,'$.app_promotion_type') AS app_promotion_type,
        safe_cast(json_value(DATA, '$.budget') AS int64) AS budget,
        json_value(DATA,'$.budget_mode') AS budget_mode,
        json_value(DATA,'$.budget_optimize_on') AS budget_optimize_on,
        json_value(DATA,'$.deep_bid_type') AS deep_bid_type,
        safe_cast(json_value(DATA,'$.is_advanced_dedicated_campaign') AS bool) AS is_advanced_dedicated_campaign,
        safe_cast(json_value(DATA, '$.is_new_structure') AS bool) AS is_new_structure,
        safe_cast(json_value(DATA, '$.is_search_campaign') AS bool) AS is_search_campaign,
        safe_cast(json_value(DATA,'$.is_smart_performance_campaign') AS bool) AS is_smart_performance_campaign,
        safe_cast(json_value(DATA, '$.roas_bid') AS float64) AS roas_bid,
        datetime(json_value(DATA, '$.create_time')) AS create_time,
        datetime(json_value(DATA, '$.modify_time')) AS modify_time,
    FROM
        {{ source(
            'tiktok',
            'campaign'
        ) }}
) 

{{ dbt_utils.deduplicate(
    relation = "source",
    partition_by = "advertiser_id, campaign_id",
    order_by = "modify_time desc"
) }}
