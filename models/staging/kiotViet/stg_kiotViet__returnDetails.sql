SELECT
    returns.id AS transaction_id,
    returns.code AS transactionCode,
    returns.invoiceId AS referencetransaction_id,
    returns.returnDate AS transaction_date,
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
