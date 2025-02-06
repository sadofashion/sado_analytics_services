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
                'ticket_details'
            ) }}
    )
WHERE
    rn_ = 1
