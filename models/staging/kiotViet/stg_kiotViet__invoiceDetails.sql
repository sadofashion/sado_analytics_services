SELECT
    invoices.id AS transaction_id,
    invoices.code AS transactionCode,
    invoices.orderCode as referencetransaction_id,
    invoices.purchaseDate AS transaction_date,
    invoices.statusValue AS transactionStatus,
    invoices.branchId,
    invoices.soldById AS employeeId,
    invoices.customerId,
    invoiceDetails.productId,
    invoiceDetails.productCode,
    invoiceDetails.quantity,
    invoiceDetails.price,
    invoiceDetails.discountRatio,
    invoiceDetails.discount,
    invoiceDetails.subTotal
FROM
    {{ ref('base_kiotViet__invoices') }}
    invoices,
    unnest(invoiceDetails) invoiceDetails
