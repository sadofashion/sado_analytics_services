SELECT
    DISTINCT campaign,
    SPLIT(regexp_extract(campaign, r '\|\|([0-9\-_]+)\|\|'), '_') [offset(0)] AS start_date,
    SPLIT(regexp_extract(campaign, r '\|\|([0-9\-_]+)\|\|'), '_') [offset(1)] AS end_date,
    CASE
        WHEN campaign = 'QC||SINH NHAT' THEN 'SINH NHAT'
        ELSE regexp_extract(
            campaign,
            r '\|\|-\s?(.*)$'
        )
    END AS audience
FROM
    {{ ref('stg_esms__sent_data') }}
WHERE
    campaign LIKE 'QC%'
