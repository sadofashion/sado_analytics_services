SELECT
    returns.id AS transactionId,
    returns.code AS transactionCode,
    returns.invoiceId AS referenceTransactionId,
    returns.returnDate AS transactionDate,
    returns.statusValue as transactionStatus,
    returns.branchId,
    returns.receivedById AS employeeId,
    returns.customerId,
    returns.saleChannelId,
    returnDetails.productId,
    returnDetails.productCode,
    returnDetails.quantity,
    returnDetails.price,
    returnDetails.subTotal
FROM
    {{ ref('base_kiotViet__returns') }}
    returns,
    unnest(returnDetails) returnDetails
