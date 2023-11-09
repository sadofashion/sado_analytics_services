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
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_branches_list_*`
                LIMIT
                    1000
            )
        

        
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