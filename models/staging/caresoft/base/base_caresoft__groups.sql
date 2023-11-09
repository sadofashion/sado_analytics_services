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
            {{ source(
                'caresoft',
                'groups'
            ) }}
    )
WHERE
    rn_ = 1
