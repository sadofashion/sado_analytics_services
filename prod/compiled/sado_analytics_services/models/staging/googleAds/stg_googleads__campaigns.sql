

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
    `agile-scheme-394814`.`dbt_dev`.`base_googleAds__accounts` A
    LEFT JOIN `agile-scheme-394814`.`dbt_dev`.`base_googleAds__campaigns` C
    ON A.account_id = C.account_id