{{ config(
    materialized = 'incremental',
    unique_key = ['product_id','depot_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'daily','nhanhvn'],
    incremental_strategy = 'insert_overwrite',
    partition_by ={ "field": "updated_at",
    "data_type": "timestamp",
    "granularity": "day" }
) }}

WITH source AS (

    SELECT
        safe_cast(
            id AS int64
        ) AS product_id,
        datetime_add(
            _batched_at,
            INTERVAL 7 HOUR
        ) AS updated_at,
        safe_cast(
            depots.depotId AS int64
        ) AS depot_id,
        depots.available,
        depots.remain,
        depots.shipping,
        depots.damaged,
        depots.holding,
        depots.warranty,
        depots.warrantyHolding AS warranty_holding,
    FROM
        {{ source(
            'nhanhvn',
            'p_webhook_inventoryChange'
        ) }},
        unnest(depots) depots
    WHERE
        depots.remain IS NOT NULL
    {% if is_incremental() %}
    AND date(_batched_at) >= date(_dbt_max_partition)
    {% endif %}
UNION ALL
SELECT
    safe_cast(
        idNhanh AS int64
    ) AS product_id,
    TIMESTAMP(datetime_add(_batched_at, INTERVAL 7 HOUR)) AS updated_at,
    safe_cast(
        depots.depotId AS int64
    ) AS depot_id,
    depots.available,
    depots.remain,
    depots.shipping,
    depots.damage,
    depots.holding,
    depots.warranty,
    depots.warrantyHolding AS warranty_holding,
FROM
    {{ source(
        'nhanhvn',
        'p_inventory'
    ) }},
    unnest(depots) depots
WHERE
    depots.remain <> 0
    {% if is_incremental() %}
      and date(TIMESTAMP(datetime_add(_batched_at, INTERVAL 7 HOUR))) >= date(_dbt_max_partition)
    {% endif %}
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
    LEFT JOIN {{ ref('stg_nhanhvn__depots') }}
    d
    ON inv.depot_id = d.depot_id
