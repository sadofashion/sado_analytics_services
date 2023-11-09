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
            {{ source(
                'caresoft',
                'contact_details'
            ) }}
    )
WHERE
    rn_ = 1
