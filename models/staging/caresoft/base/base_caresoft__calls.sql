SELECT
    *
EXCEPT(rn_)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY call_id
                ORDER BY
                    _batched_at DESC
            ) rn_
        FROM
            {{ source(
                'caresoft',
                'calls'
            ) }}
    )
WHERE
    rn_ = 1
