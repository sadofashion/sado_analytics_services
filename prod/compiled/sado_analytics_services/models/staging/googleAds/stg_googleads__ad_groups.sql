

SELECT
    ag.account_id,
    ag.campaign_id,
    ag.ad_group_id,
    ag.ad_group_type,
    ag.ad_group_name
FROM
   `agile-scheme-394814`.`dbt_dev`.`base_googleAds__adgroups`
    ag