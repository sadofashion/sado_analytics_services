SELECT
    *
EXCEPT(rn_)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY ticket_id
                ORDER BY
                    _batched_at DESC,
                    updated_at DESC
            ) rn_
        FROM
            {{ source(
                'caresoft',
                'tickets'
            ) }}
    )
WHERE
    rn_ = 1
