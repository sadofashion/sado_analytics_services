SELECT
    id as customer_id,
    code as customer_code,
    name as customer_name,
    gender,
    extract(month from birthDate) as birth_month,
    birthDate as birth_date,
    contactNumber as contact_number,
    branchId as branch_id,
    type as customer_type,
    c.groups as customer_groups,
    debt,
    totalInvoiced as total_invoiced,
    totalPoint as total_point,
    totalRevenue as total_revenue,
    rewardPoint as rewardpoint,
FROM
    {{ ref('base_kiotViet__customers') }} c 
