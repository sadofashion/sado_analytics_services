{{ config(
    materialized='view',
    tags=['ggads','dimension','view']
)}}

SELECT
    A.account_name,
    A.currency_code,
    C.campaign_id,
    C.campaign_name,
    C.advertising_channel,
    C.bidding_stategy,
    C.budget,
    C.budget_period,
FROM
    {{ ref('base_googleAds__accounts') }} A
    LEFT JOIN {{ ref('base_googleAds__campaigns') }} C
    ON A.account_id = C.account_id