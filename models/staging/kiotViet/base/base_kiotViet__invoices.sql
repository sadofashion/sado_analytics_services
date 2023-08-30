WITH source AS (
    SELECT
        * except(invoiceDelivery)
    FROM
        {{ source(
            'kiotViet',
            'p_invoices_list_*'
        ) }}
    UNION ALL
    SELECT
        * except(invoiceDelivery)
    FROM
        {{ source(
            'kiotViet',
            'p_webhook_invoice_update'
        ) }}
),
raw_ AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC,
                modifiedDate DESC
        ) AS rn_
    FROM
        source
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
    createdDate,
    modifiedDate,
    discountRatio,
    discount,
    payments,
    invoiceDetails
FROM
    raw_
WHERE
    rn_ = 1
