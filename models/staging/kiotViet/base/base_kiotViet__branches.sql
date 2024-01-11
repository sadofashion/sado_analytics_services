WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'kiotViet',
            'p_branches_list_*'
        ),
        partition_by = 'id',
        order_by = "modifiedDate DESC,_batched_at desc",
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
FROM
    source