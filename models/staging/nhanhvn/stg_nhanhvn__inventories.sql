with source as (
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
    {{ source(
        'nhanhvn',
        'p_webhook_inventoryChange'
    ) }}, unnest(depots) depots
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
    {{ source(
        'nhanhvn',
        'p_inventory_*'
    ) }}, unnest(depots) depots
where depots.remain <> 0

)

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
    source inv 
    left join {{ref('stg_nhanhvn__depots')}}  d on inv.depot_id = d.depot_id