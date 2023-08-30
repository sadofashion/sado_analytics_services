SELECT
    id AS branchId,
    branchName,
    address,
    wardName,
    regexp_replace(contactNumber,r'\.|\,|\s','') as contactNumber,
    email,
    createdDate,
    modifiedDate,
FROM
    {{ ref('base_kiotViet__branches') }}
