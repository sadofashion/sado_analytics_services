SELECT
    id as customerId,
    code as customerCode,
    name as customerName,
    gender,
    extract(month from birthDate) as birthMonth,
    birthDate,
    contactNumber,
    branchId,
    type as customerType,
    c.groups as customerGroups,
    debt,
    totalInvoiced,
    totalPoint,
    totalRevenue,
    rewardPoint
FROM
    {{ ref('base_kiotViet__customers') }} c 
