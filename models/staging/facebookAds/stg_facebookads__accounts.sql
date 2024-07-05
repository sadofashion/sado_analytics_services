{{ config(
    materialized = 'view',
    tags = ['fb','dimension','view']
    enabled =False
) }}

    SELECT
        DISTINCT account_id,
        FIRST_VALUE (account_name) over  account_window AS account_name,
    FROM
        {{ source(
            'facebookAds',
            'p_AdsInsights__*'
        ) }}
        window account_window as (partition by account_id order by _batched_at desc)
        

    {# select * 
    from {{ ref("stg_fb__ad_accounts") }} #}