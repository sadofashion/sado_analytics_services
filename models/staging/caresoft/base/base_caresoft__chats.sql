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
            {{ source(
                'caresoft',
                'chats'
            ) }}
    )
WHERE
    rn_ = 1
