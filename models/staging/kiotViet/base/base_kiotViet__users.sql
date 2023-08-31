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
            'kiotViet',
            'p_users_list_*'
        ) }}
)
SELECT
    id,
    userName,
    givenName,
    birthDate,
FROM
    source
WHERE
    rn_ = 1
