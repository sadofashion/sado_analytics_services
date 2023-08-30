SELECT
    invoices.id AS transactionId,
    invoices.code AS transactionCode,
    invoices.orderCode as referenceTransactionId,
    invoices.purchaseDate AS transactionDate,
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
