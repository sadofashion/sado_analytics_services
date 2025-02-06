SELECT
    DISTINCT campaign_id,
    customer_id AS account_id,
    campaign_advertising_channel_type AS advertising_channel,
    (
        FIRST_VALUE(campaign_bidding_strategy_type) over w1
    ) AS bidding_stategy,
    (
        FIRST_VALUE(campaign_budget_period) over w1
    ) as budget_period,
    (
        FIRST_VALUE(campaign_budget_amount_micros) over w1 / 1e6
    ) AS budget,
    FIRST_VALUE(campaign_name) over w1 AS campaign_name
FROM
    {{ source(
        'googleads',
        'campaign'
    ) }}
    window w1 as (
        PARTITION BY campaign_id
        ORDER BY _DATA_DATE desc, _LATEST_DATE desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING
    )
