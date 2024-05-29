{{
  config(
    materialized = 'incremental',
    partition_by = {"field": "date_start", "data_type": "date"},
    incremental_strategy = 'insert_overwrite',
    )
}}

WITH source AS (

    select * from {{source(
            'facebookAds',
            'p_AdsInsights__*'
        )}}
        {% if is_incremental() %}
          where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date_add(current_date, INTERVAL -1 DAY)
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
