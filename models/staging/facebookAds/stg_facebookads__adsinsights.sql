{{
  config(
    materialized = 'incremental',
    partition_by = {"field": "date_start", "data_type": "date", "granularity": "day"},
    incremental_strategy = 'insert_overwrite',
    )
}}

WITH source AS (

    select * from {{source(
            'facebookAds',
            'p_AdsInsights__*'
        )}}
        {% if is_incremental() %}
          where date_start >= date_add(current_date, INTERVAL -1 DAY)
        {% endif %}
    
)
{{ dbt_utils.deduplicate(
        relation = "source",
        partition_by = 'account_id,
            campaign_id,
            adset_id,
            ad_id,
            date_start',
        order_by = "_batched_at desc",
    ) }}
