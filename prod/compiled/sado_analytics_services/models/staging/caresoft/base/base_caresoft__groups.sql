SELECT
    *
EXCEPT(rn_)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY group_id
                ORDER BY
                    _batched_at DESC
            ) rn_
        FROM
            
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Caresoft`.`p_groups_*`
                LIMIT
                    1000
            )
        

        
    )
WHERE
    rn_ = 1