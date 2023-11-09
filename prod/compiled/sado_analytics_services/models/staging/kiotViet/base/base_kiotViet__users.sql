WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) AS rn_
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_users_list_*`
                LIMIT
                    1000
            )
        

        
)
SELECT
    id,
    userName,
    givenName,
    birthDate,
FROM
    source
WHERE
    rn_ = 1