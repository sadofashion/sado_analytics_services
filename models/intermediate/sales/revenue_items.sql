SELECT
invoices.transactionId,
invoices.transactionCode,
invoices.transactionDate,
invoices.branchId,
invoices.customerId,
invoices.employeeId,
invoices.total,
invoices.totalPayment,
invoices.discount,
invoices.discountValue
FROM
    {{ ref('stg_kiotViet__invoiceDetails') }}
    invoices
    where invoices.transactionStatus = 'Hoàn thành'
UNION ALL
SELECT
returns.transactionId,
returns.transactionCode,
returns.transactionDate,
returns.branchId,
returns.customerId,
returns.employeeId,
returns.total,
returns.totalPayment,
returns.discount,
returns.discountValue
FROM
    {{ ref('stg_kiotViet__returnDetails') }}
    returns
    where returns.transactionStatus = 'Đã trả'