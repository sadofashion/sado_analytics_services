SELECT
    invoices.id AS transaction_id,
    invoices.code AS transaction_code,
    invoices.orderCode as reference_transaction_id,
    invoices.purchaseDate AS transaction_date,
    invoices.statusValue AS transaction_status,
    invoices.branchId as branch_id,
    invoices.soldById AS employee_id,
    invoices.customerId as customer_id,
    invoiceDetails.productId as product_id,
    invoiceDetails.productCode as product_code,
    invoiceDetails.quantity,
    invoiceDetails.price,
    invoiceDetails.discountRatio as discount_ratio,
    invoiceDetails.discount,
    invoiceDetails.subTotal,
    invoices.transaction_type,
    invoices.modifiedDate as modified_date,
FROM
    {{ ref('base_kiotViet__invoices') }}
    invoices
    left join unnest(invoiceDetails) invoiceDetails
