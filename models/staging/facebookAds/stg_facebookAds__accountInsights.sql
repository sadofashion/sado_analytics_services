WITH source AS (
    SELECT
        account_id as accountId,
        date_start as dateStart,
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
        ) AS linkClick,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'post_engagement'
        ) AS postEngagement,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'onsite_conversion.messaging_conversation_started_7d'
        ) AS messagingConversationStarted7d,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'offline_conversion.purchase'
        ) AS offlineConversionPurchase,
        (
            SELECT
                action_values.value
            FROM
                unnest (action_values) action_values
            WHERE
                action_values.action_type = 'offline_conversion.purchase'
        ) AS offlineConversionPurchaseValue,
        (
            SELECT
                actions.value
            FROM
                unnest (actions) actions
            WHERE
                actions.action_type = 'offsite_conversion.fb_pixel_purchase'
        ) AS pixelPurchase,
        (
            SELECT
                action_values.value
            FROM
                unnest (action_values) action_values
            WHERE
                action_values.action_type = 'offsite_conversion.fb_pixel_purchase'
        ) AS pixelPurchaseValue,
    FROM
        {{ ref('base_facebookAds__accountInsights') }}
)
SELECT
    *
FROM
    source
