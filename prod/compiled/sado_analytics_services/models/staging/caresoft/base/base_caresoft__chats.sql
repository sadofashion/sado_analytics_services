SELECT
    *
EXCEPT(rn_)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY conversation_id
                ORDER BY
                    _batched_at DESC
            ) rn_
        FROM
            
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Caresoft`.`p_chats_*`
                LIMIT
                    1000
            )
        

        
    )
WHERE
    rn_ = 1