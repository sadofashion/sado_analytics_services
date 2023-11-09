WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY account_id,
            ad_id,
            id
            ORDER BY
                _batched_at DESC
        ) AS rn_,
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Facebook`.`p_CreativesInsights__*`
                LIMIT
                    1000
            )
        

        
)
SELECT
    account_id,
    ad_id,
    body,
    image_url,
    thumbnail_url,
    call_to_action_type,
    title,
    name,
FROM
    source
WHERE
    rn_ = 1