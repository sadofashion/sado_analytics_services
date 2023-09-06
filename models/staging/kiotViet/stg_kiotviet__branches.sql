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
    {{ ref('base_kiotViet__branches') }} 