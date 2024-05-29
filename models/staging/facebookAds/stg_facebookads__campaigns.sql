{{ config(
    materialized = 'view',
    tags = ['fb','dimension','view']
) }}

WITH 
old_convention as (
    old_naming
),
current_campaign_name AS (

    SELECT
        DISTINCT account_id,
        FIRST_VALUE (account_name) over (PARTITION BY account_id ORDER BY _batched_at DESC) AS account_name,
        upper(FIRST_VALUE (campaign_name) over ( PARTITION BY campaign_id ORDER BY _batched_at DESC )) AS campaign_name,
        upper(FIRST_VALUE (adset_name) over (PARTITION BY adset_id ORDER BY _batched_at DESC )) AS adset_name,
        upper(FIRST_VALUE (ad_name) over (PARTITION BY ad_id ORDER BY _batched_at DESC)) AS ad_name,
        MIN(date_start) over (PARTITION BY campaign_id) AS campaign_start_date,
        MAX(date_start) over (PARTITION BY campaign_id) AS campaign_stop_date,
        campaign_id
    FROM
        {{ source(
            'facebookAds',
            'p_AdsInsights__*'
        ) }}
        where date_start >= '2024-06-01'
),
convention_version as (
    select * 
    case when campaign_name like '5S%' then 'B2406' else '2406' end as convention_version
    from current_campaign_name
),

new_naming_convention as (
    select 
        * ,
        {{parse_naming_convention(campaign_col = "campaign_name", adset_col = "adset_name", ad_col = "ad_name")}}
    from convention_version
    where convention_version = '2406'
),
old_naming_convention(
    SELECT
        *,
        regexp_extract (campaign_name,r"^(?:.*?_){4}(.*?)_(?:.*?)$") AS big_campaign,
        regexp_extract (campaign_name,r"^(?:.*?_){4}(.*?_.*?)_(?:.*?)$") AS content_group,
        regexp_extract (campaign_name,r"^(?:.*?_){1}(.*?)_(?:.*?)$") AS pic,
        regexp_extract (campaign_name,r"^(?:.*?_){6}(.*?)_(?:.*?)$") AS promoted_productline,
        regexp_extract (campaign_name,r"^(.*?)_") AS page,
        regexp_extract (campaign_name,r"(?:.*?_){7}(.*?)_(?:.*?)") AS media_type,
        regexp_extract (campaign_name,r"^(?:.*?_){2}(.*?)_(?:.*?)$") AS funnel,
        regexp_extract (campaign_name,r"^(?:.*?_){3}(.*?)_(?:.*?)$") AS ad_type,
    from convention_version
    where convention_version = 'B2406'
    )



select * 
from new_naming_convention