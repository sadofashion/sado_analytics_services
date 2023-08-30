WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                modifiedDate DESC,
                _batched_at DESC
        ) AS rn_
    FROM
        {{ source(
            'kiotViet',
            'p_branches_list_*'
        ) }}
)
SELECT
    id,
    branchName,
    address,
    locationName,
    wardName,
    contactNumber,
    email,
    createdDate,
    modifiedDate
from source
WHERE
    rn_ = 1
