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
    {{ ref('base_facebookAds__creatives') }}
