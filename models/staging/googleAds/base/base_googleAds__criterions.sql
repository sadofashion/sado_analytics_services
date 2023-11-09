SELECT
    ad_group_criterion_criterion_id AS criterion_id,
    ad_group_id,
    ad_group_criterion_display_name AS criterion_name,
    ad_group_criterion_negative AS is_negative,
    ad_group_criterion_type AS criterion_type,
FROM
    {{ source(
        'googleads',
        'ad_group_criterion'
    ) }}
WHERE
    _LATEST_DATE = _DATA_DATE
