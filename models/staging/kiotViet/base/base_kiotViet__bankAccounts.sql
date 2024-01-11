WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'kiotViet',
            'p_bankAccounts_list_*'
        ),
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    id,
    bankName,
    accountNumber,
    description,
    createdDate,
    modifiedDate
from source
