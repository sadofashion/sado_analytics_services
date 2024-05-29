{% set metrics = [
    "link_click",
    "post_engagement",
    "onsite_conversion.messaging_conversation_started_7d",
    "offline_conversion.purchase",
    "offsite_conversion.fb_pixel_purchase",
    "onsite_conversion.purchase"
    ] %}
WITH source AS (
    SELECT
        account_id,
        campaign_id,
        adset_id,
        ad_id,
        date_start,
        clicks,
        impressions,
        reach,
        spend,
        {% for metric in metrics %}
            (
            SELECT
                actions.value
            FROM
                unnest (actions) actions 
            WHERE actions.action_type = '{{metric}}'
            ) AS num_{{metric | replace('.','_')}},

            (
            SELECT
                action_values.value
            FROM
                unnest (action_values) action_values
            WHERE
                action_values.action_type = '{{metric}}'
            ) as {{metric | replace('.','_') }}_value,
        {% endfor %}
    FROM
        {{ ref('stg_facebookads__adsinsights') }}
)
SELECT
    *
FROM
    source
