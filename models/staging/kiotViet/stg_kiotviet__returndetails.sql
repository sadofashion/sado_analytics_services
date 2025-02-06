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
    returns.returnDiscount*safe_divide(returnDetails.price*returnDetails.quantity,returns.returnTotal+returns.returnDiscount) as order_discount,
    returnDetails.productId as product_id,
    returnDetails.productCode as product_code,
    returnDetails.quantity,
    returnDetails.price,
    returnDetails.subTotal,
    returns.transaction_type,
    returns.modifiedDate as modified_date,
FROM
    {{ ref('base_kiotViet__returns') }}
    returns
    left join unnest(returnDetails) returnDetails
