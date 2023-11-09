SELECT
    *
EXCEPT(rn_)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY id
                ORDER BY
                    _batched_at DESC,
                    updated_at DESC
            ) rn_
        FROM
            
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Caresoft`.`p_agents_*`
                LIMIT
                    1000
            )
        

        
    )
WHERE
    rn_ = 1