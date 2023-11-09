

SELECT

    
to_hex(md5(cast(coalesce(cast(account_id as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(date_start as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(age as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(gender as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) as row_id,
    account_id,
    date_start,
    age,
    gender,
    sum(impressions) impressions,
    sum(spend) spend,
    sum(clicks) clicks,
    sum(reach) reach,
    sum(link_click) link_click,
    sum(post_engagement) post_engagement,
    sum(offline_conversion_purchase) offline_conversion_purchase,
    sum(offline_conversion_purchase_value) offline_conversion_purchase_value,
    sum(pixel_purchase) pixel_purchase,
    sum(pixel_purchase_value) pixel_purchase_value,
    sum(messaging_conversation_started_7d) messaging_conversation_started_7d
FROM
    `agile-scheme-394814`.`dbt_dev`.`stg_facebookads__audienceinsights`
    group by 1,2,3,4,5