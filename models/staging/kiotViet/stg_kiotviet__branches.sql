{{
  config(
    tags=['view', 'dimension','kiotviet']
  )
}}
with source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'kiotViet',
            'p_branches_list'
        ),
        partition_by = 'id',
        order_by = "modifiedDate DESC,_batched_at desc",
    ) }}
)
SELECT
    b.id as branch_id,
    b.branchName as branch_name,
    b.address,
    b.wardName ward_name,
    regexp_replace(b.contactNumber,r'\.|\,|\s','') as contact_number,
    b.email,
    b.createdDate as created_Date,
    b.modifiedDate as modified_date,
FROM
    source b