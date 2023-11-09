

SELECT
    distinct
    supplierId as supplier_id,
    supplierName as supplier_name,
    supplierCode as supplier_code
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__purchaseOrders`