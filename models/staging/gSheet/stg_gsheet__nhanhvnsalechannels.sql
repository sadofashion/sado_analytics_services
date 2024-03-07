{{ config(
    tags = ['nhanhvn','dimension','view']
) }}

SELECT
    safe_cast(
        sale_channel_id AS int64
    ) sale_channel_id,
    sale_channel,
FROM
    {{ source(
            'gSheet',
            '_ext_nhanhvn_salechannel'
        ) }}
