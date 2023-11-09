SELECT
    safe_cast(id as int64) as product_id,
    datetime_add(_batched_at, interval 7 hour) as updated_at,
    safe_cast(depots.depotId as int64) as depot_id,
    depots.available,
    depots.remain,
    depots.shipping,
    depots.damaged,
    depots.holding,
    depots.warranty,
    depots.warrantyHolding as warranty_holding,
FROM
    
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Nhanhvn`.`p_webhook_inventoryChange`
                LIMIT
                    1000
            )
        

        , unnest(depots) depots
    where depots.remain is not null

union all
SELECT
    safe_cast(idNhanh as int64) as product_id,
    timestamp(datetime_add(_batched_at, interval 7 hour)) as updated_at,
    safe_cast(depots.depotId as int64) as depot_id,
    depots.available,
    depots.remain,
    depots.shipping,
    depots.damage,
    depots.holding,
    depots.warranty,
    depots.warrantyHolding as warranty_holding,
FROM
    
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Nhanhvn`.`p_inventory_*`
                LIMIT
                    1000
            )
        

        , unnest(depots) depots
where depots.remain <> 0