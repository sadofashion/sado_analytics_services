SELECT
    id as branch_id,
    branchName as branch_name,
    address,
    wardName ward_name,
    regexp_replace(contactNumber,r'\.|\,|\s','') as contact_number,
    email,
    createdDate as created_Date,
    modifiedDate as modified_date,
FROM
    {{ ref('base_kiotViet__branches') }}