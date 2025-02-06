SELECT
    id AS transaction_id,
    code AS transaction_code,
    invoiceId AS reference_transaction_id,
    returnDate AS transaction_date,
    branchId  as branch_id,
    receivedById AS employee_id,
    customerId as customer_id,
    returnTotal AS total,
    returnFeeRatio as return_fee_ratio,
    returnDiscount as return_discount,
    totalPayment as total_payment,
    returnFee as return_fee,
    statusValue AS transaction_status,
    saleChannelId as salechannel_id,
    createdDate as created_date ,
    coalesce(modifiedDate,createdDate) as modified_date,
    transaction_type
FROM
    {{ ref('base_kiotViet__returns') }}
