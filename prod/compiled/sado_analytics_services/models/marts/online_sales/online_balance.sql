
SELECT
    bills.bill_id,
    bills.bill_date,
    bills.created_date,
    bills.bill_type,
    bills.bill_mode,
    bills.order_id,
    depots.depot_name,
    bills.customer_id,
    bills.created_by_id,
    bills.product_id,
    bills.discount,
    bills.quantity,
    bills.price,
    bills.vat,
    ((bills.price - bills.discount) * bills.quantity - coalesce(bills.vat,0)) AS item_net_amount,
FROM
    `agile-scheme-394814`.`dbt_dev`.`stg_nhanhvn__bills` bills
    left join `agile-scheme-394814`.`dbt_dev`.`stg_nhanhvn__depots` depots on bills.depot_id = depots.depot_id