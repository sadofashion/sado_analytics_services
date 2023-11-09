SELECT
    COALESCE(
        thumbnail_url,
        image_url
    ) as thumbnailUrl,
    name,
    regexp_extract (
        body,
        r"^(.*)\n"
    ) title,
    body,
    call_to_action_type,
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_facebookAds__creatives`