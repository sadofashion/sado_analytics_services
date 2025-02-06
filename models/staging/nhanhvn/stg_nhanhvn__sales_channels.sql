{{ config(
    tags = ['view', 'dimension','nhanhvn']
) }}

{% set sales_channel = [
    "Admin","Website","API","Facebook",
    "Instagram","Lazada.vn","Shopee.vn",
    "Sendo.vn","Tiki.vn","Zalo Shop","1Landing.vn",
    "Tiktok Shop","Zalo OA","Shopee Chat","Lazada Chat",
    "Website","Sàn","FB - WEB","Chưa phân loại nguồn"
    ] %}

SELECT
    distinct *
FROM
    unnest([ struct <channel string, channel_id int64>
    {%for channel in sales_channel%} 
    ('{{channel}}', farm_fingerprint('{{channel}}')) {{"," if not loop.last}}
    {%endfor%}
    ])
