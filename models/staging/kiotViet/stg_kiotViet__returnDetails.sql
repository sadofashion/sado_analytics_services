SELECT
    returns.id AS transaction_id,
    returns.code AS transaction_code,
    returns.invoiceId AS reference_transaction_id,
    returns.returnDate AS transaction_date,
    returns.statusValue as transaction_status,
    returns.branchId as branch_id,
    returns.receivedById AS employee_id,
    returns.customerId as customer_id,
    returns.saleChannelId as salechannel_id,
    returnDetails.productId as product_id,
    returnDetails.productCode as product_code,
    returnDetails.quantity,
    returnDetails.price,
    returnDetails.subTotal
FROM
    {{ ref('base_kiotViet__returns') }}
    returns,
    unnest(returnDetails) returnDetails
