WITH source AS (
    SELECT
        account_id ,
        campaign_id,
        adset_id ,
        ad_id ,
        date_start ,
        region,
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
    FROM
        {{ ref('base_facebookAds__regionInsights') }}
)
SELECT
    *
FROM
    source

union all 
select 
account_id ,
        campaign_id,
        adset_id ,
        ad_id ,
        date_start ,
        region,
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

from {{ ref( "stg_fb__region_demographic") }}