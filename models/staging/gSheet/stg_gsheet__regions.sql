{{ config(
    materialized = 'table',
    tags = ['dimension','table']
) }}

WITH source AS (

    SELECT
        *,
    FROM
        {{ source(
            'gSheet',
            '_ext_region'
        ) }}
        where branch_name is not null
)
SELECT
    *
FROM
    source
