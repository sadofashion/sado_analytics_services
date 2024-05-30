{{ config(
    materialized = 'view',
    tags = ['fb','dimension','view']
) }}

WITH 
current_campaign_name AS (

    SELECT
        DISTINCT account_id,
        FIRST_VALUE (account_name) over  ad_window AS account_name,
        FIRST_VALUE (campaign_name) over  ad_window AS campaign_name,
        FIRST_VALUE (adset_name) over  ad_window AS adset_name,
        FIRST_VALUE (ad_name) over  ad_window AS ad_name,
        MIN(date_start) over campaign_window AS campaign_start_date,
        MAX(date_start) over campaign_window AS campaign_stop_date,
        campaign_id
    FROM
        {{ source(
            'facebookAds',
            'p_AdsInsights__*'
        ) }}
        window ad_window as (partition by account_id, campaign_id, adset_id,ad_id order by _batched_at desc),
        {# account_window as (partition by account_id order by _batched_at desc), #}
        campaign_window as (partition by campaign_id)

),
convention_version as (
    select * ,
    case when campaign_name like '5S%' then 'B2406' else '2406' end as convention_version_number
    from current_campaign_name
),

new_naming_convention as (
    select 
        * ,
        {{parse_naming_convention(campaign_col = "campaign_name", adset_col = "adset_name", ad_col = "ad_name")}}
    from convention_version
    where convention_version.convention_version_number = '2406'
),
old_naming_convention as (
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
    where convention_version.convention_version_number = 'B2406'
    )



select 
    * 
from new_naming_convention

union all

select 
    account_id,
    account_name,
    campaign_name,
    adset_name,
    ad_name,
    campaign_start_date,
    campaign_stop_date,
    campaign_id,
    convention_version_number,
    'fb' as channel,
    page as ad_location,
    case when page in ("5SFTHA","5SFTIE","5SFTUN","5SFTRA","5SFT","5SFG","5SF","5SFTUY") then "PIC Region" else "Store" end as ad_location_layer,
    ad_type as campaign_category,
    big_campaign as event_name,
    content_group as content_edge,
    pic as ad_pic,
    null as audience_type,
    null as target_method,
    null as original_audience_name,
    null as audience_demographic,
    null as audience_region,
    funnel as audience_source_name,
    media_type,
    content_group as content_code
from old_naming_convention