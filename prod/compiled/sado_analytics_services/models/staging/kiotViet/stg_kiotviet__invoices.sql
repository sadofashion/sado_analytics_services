SELECT
    id AS transaction_id,
    code AS transaction_code,
    purchaseDate AS transaction_date,
    branchId as branch_id,
    soldById AS employee_id,
    customerId as customer_id,
    orderCode AS reference_transaction_id,
    total,
    totalPayment as total_payment,
    statusValue AS transaction_status,
    createdDate as created_Date,
    modifiedDate as modified_date,
    discountRatio as discount_ratio,
    discount,
    transaction_type
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__invoices`