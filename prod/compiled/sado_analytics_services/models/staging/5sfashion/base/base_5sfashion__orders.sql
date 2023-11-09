WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY _id
            ORDER BY
                _batched_at DESC,
                updated_at DESC
        ) rn_
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`WebsiteDB`.`p_orders_*`
                LIMIT
                    1000
            )
        

        
)
SELECT
    *
EXCEPT(rn_, _batched_at)
FROM
    source
WHERE
    rn_ = 1