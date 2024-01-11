WITH source AS (
    SELECT
        * except(invoiceDelivery,invoiceDetails),
        invoiceDetails
    FROM
        {{ source(
            'kiotViet',
            'p_invoices_list_*'
        ) }} invoice1
    union all 
    select * except(invoiceDelivery,invoice_details),
    invoice_details as invoiceDetails
    from {{ source(
            'kiotViet',
            'p_invoices_list2_*'
        ) }} 
    UNION ALL
    SELECT
        * except(invoiceDelivery,invoiceDetails),
        invoiceDetails
    FROM
        {{ source(
            'kiotViet',
            'p_webhook_invoice_update'
        ) }}
),
raw_ AS (
    {{ dbt_utils.deduplicate(relation = 'source', partition_by = 'id', order_by = "_batched_at desc",) }}
)
SELECT
    id,
    uuid,
    code,
    purchaseDate,
    branchId,
    soldById,
    customerId,
    orderCode,
    total,
    totalPayment,
    statusValue,
    COALESCE(first_value(createdDate ignore nulls) over (partition by id order by _batched_at asc), 
    first_value(_batched_at) over (partition by id order by _batched_at asc)
     ) createdDate ,
    modifiedDate,
    discountRatio,
    discount,
    payments,
    invoiceDetails,
    "invoice" as transaction_type,
FROM
    raw_
