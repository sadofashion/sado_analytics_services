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
          where 1=1
          and date_start <'2024-07-01'
        {% if is_incremental() %}
          and date_start >= date_add(current_date, INTERVAL -30 DAY)
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
        safe_cast(account_id as string) account_id ,
        safe_cast(campaign_id as string) campaign_id,
        safe_cast(adset_id as string) adset_id ,
        safe_cast(ad_id as string) ad_id ,
        {{dbt_utils.generate_surrogate_key(['account_id','campaign_id','adset_id','ad_id'])}} as ad_key,
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
        ) AS meta_purchase_value,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'purchase'
        ) AS purchase,
        (
            SELECT
                action_values.value
            FROM
                unnest (action_values) action_values
            WHERE
                action_values.action_type = 'purchase'
        ) AS purchase_value
    FROM
        deduplicate

union all
select 
    account_id,
    campaign_id,
    adset_id,
    ad_id,
    {{dbt_utils.generate_surrogate_key(['account_id','campaign_id','adset_id','ad_id'])}} as ad_key,
    date_start,
    clicks,
    impressions,
    reach,
    spend,
    no__link_click,
    no__post_engagement,
    no__onsite_conversion__messaging_conversation_started_7d,

    no__offline_conversion__purchase,
    offline_conversion__purchase__value,

    no__offsite_conversion__fb_pixel_purchase,
    offsite_conversion__fb_pixel_purchase__value,

    no__onsite_conversion__purchase,
    onsite_conversion__purchase__value,

    no__purchase,
    purchase__value,

from {{ ref("stg_fb__ad_insights") }}
where 1=1 
{% if is_incremental() %}
  and date_start >= date_add(current_date, INTERVAL -30 DAY)
{% else %}
  and date_start >= '2024-07-01'
{% endif %}