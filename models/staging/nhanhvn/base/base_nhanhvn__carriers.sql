WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) AS rn_
    FROM
        {{ source(
            'nhanhvn',
            'p_carriers_*'
        ) }}
)
SELECT
    *
EXCEPT(rn_)
FROM
    source
WHERE
    rn_ = 1
