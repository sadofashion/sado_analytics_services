SELECT
    id AS transaction_id,
    code AS transaction_code,
    purchaseDate AS transaction_date,
    branchId as branch_id,
    purchaseById AS employee_id,
    supplierId as supplier_id,
    partnerType as partner_type,
    total,
    totalPayment as total_payment,
    discount,
    discountRatio as discount_ratio ,
    CASE
        WHEN status = 3 THEN "Đã nhập hàng"
        WHEN status = 1 THEN "Phiếu tạm"
        WHEN status = 4 THEN "Đã huỷ"
    END AS transaction_status,
    createdDate as created_date,
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__purchaseOrders`