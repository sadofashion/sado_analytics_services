{{ config(
    tags = ['nhanhvn','dimension','view']
) }}

WITH source AS (

    SELECT
        *,
    FROM
        {{ source(
            'gSheet',
            '_ext_nhanhvn_salechannel'
        ) }}
)
SELECT
    safe_cast(
        sale_channel_id AS int64
    ) sale_channel_id,
    sale_channel,
FROM
    source
