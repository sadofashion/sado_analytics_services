WITH source AS (
    SELECT
        safe_cast(account_id as string) account_id,
        date_start,
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
        ) AS purchase_value,
    FROM
        {{ ref('base_facebookAds__accountInsights') }}
)
SELECT
    *
FROM
    source

union all
select 
    account_id,
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

from {{ ref("stg_fb__account_insights") }}
where 1=1 
{% if is_incremental() %}
  and date_start >= date_add(current_date, INTERVAL -7 DAY)
{% else %}
  and date_start >= '2024-07-01'
{% endif %}