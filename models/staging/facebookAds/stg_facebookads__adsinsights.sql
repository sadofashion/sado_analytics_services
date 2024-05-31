{{
  config(
    materialized = 'incremental',
    partition_by = {"field": "date_start", "data_type": "date", "granularity": "day"},
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'hourly','fact']
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
    
),
deduplicate as ({{ dbt_utils.deduplicate(
        relation = "source",
        partition_by = 'account_id,
            campaign_id,
            adset_id,
            ad_id,
            date_start',
        order_by = "_batched_at desc",
    ) }})
SELECT
        account_id ,
        campaign_id ,
        adset_id ,
        ad_id ,
        date_start ,
        clicks,
        impressions,
        reach,
        spend,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'link_click'
        ) AS link_click,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'post_engagement'
        ) AS post_engagement,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'onsite_conversion.messaging_conversation_started_7d'
        ) AS messaging_conversation_started_7d,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'offline_conversion.purchase'
        ) AS offline_conversion_purchase,
        (
            SELECT
                action_values.value
            FROM
                unnest (action_values) action_values
            WHERE
                action_values.action_type = 'offline_conversion.purchase'
        ) AS offline_conversion_purchase_value,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'offsite_conversion.fb_pixel_purchase'
        ) AS pixel_purchase,
        (
            SELECT
                action_values.value
            FROM
                unnest (action_values) action_values
            WHERE
                action_values.action_type = 'offsite_conversion.fb_pixel_purchase'
        ) AS pixel_purchase_value,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'onsite_conversion.purchase'
        ) AS meta_purchase,
        (
            SELECT
                action_values.value
            FROM
                unnest (action_values) action_values
            WHERE
                action_values.action_type = 'onsite_conversion.purchase'
        ) AS meta_purchase_value
    FROM
        deduplicate