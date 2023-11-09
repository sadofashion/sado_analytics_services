WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY _id
            ORDER BY
                _batched_at DESC,
                updated_at DESC
        ) rn_
    FROM
        {{ source(
            '5sfashion',
            'districts'
        ) }}
)
SELECT
    *
EXCEPT(rn_, _batched_at)
FROM
    source
WHERE
    rn_ = 1
