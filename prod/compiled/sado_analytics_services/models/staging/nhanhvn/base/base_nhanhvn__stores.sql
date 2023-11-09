WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) rn_
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Nhanhvn`.`p_stores_*`
                LIMIT
                    1000
            )
        

        
)
SELECT
    *
EXCEPT(rn_)
FROM
    source
WHERE
    rn_ = 1