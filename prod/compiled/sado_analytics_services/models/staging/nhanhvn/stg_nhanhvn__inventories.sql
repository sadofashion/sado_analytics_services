SELECT
inv.product_id,
d.depot_name,
inv.updated_at,
inv.available,
inv.remain,
inv.shipping,
inv.damaged,
inv.holding,
inv.warranty,
inv.warranty_holding,
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_nhanhvn__inventories` inv 
    left join `agile-scheme-394814`.`dbt_dev`.`stg_nhanhvn__depots`  d on inv.depot_id = d.depot_id