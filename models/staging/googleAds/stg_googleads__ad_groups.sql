{{ config(
    materialized='view',
    tags=['ggads','dimension','view']
)}}

SELECT
    ag.account_id,
    ag.campaign_id,
    ag.ad_group_id,
    ag.ad_group_type,
    ag.ad_group_name
FROM
   {{ ref('base_googleAds__adgroups') }}
    ag