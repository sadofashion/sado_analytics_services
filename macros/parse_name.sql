{%- macro parse_naming_convention(campaign_col,adset_col, ad_col)%}
    {{parse_campaign_name(campaign_col)}}
    {{parse_adset_name(adset_col)}}
    {{parse_ad_name(ad_col)}}
{% endmacro -%}


{% macro parse_campaign_name(column_name) %}
    -- fb_vn_engagement_{true summer_}_tung.ng
    {%- set extracted_campaign -%}
    REGEXP_EXTRACT({{column_name}},r"{(.*)}")
    {%- endset -%}
    {%- set extracted_fields %}
    SPLIT({{column_name}},"_") [safe_offset(0)] AS channel,
    SPLIT({{column_name}},"_") [safe_offset(1)] AS brand_name,
    UPPER(SPLIT({{column_name}},"_") [safe_offset(2)]) AS ad_location,
    CASE
        length(SPLIT({{column_name}}, "_") [safe_offset(2)])
        WHEN 2 THEN "Country"
        WHEN 3 THEN "Province"
        WHEN 4 THEN "Economic Region"
        WHEN 5 THEN "Store"
    END AS ad_location_layer,
    SPLIT({{column_name}},"_") [safe_offset(3)] AS campaign_category,
    SPLIT({{extracted_campaign}}, "_") [safe_offset(0)] AS event_name,
    REGEXP_REPLACE(REGEXP_EXTRACT({{extracted_campaign}}, r"_(.*)"),r"_","") AS content_edge,
    REGEXP_EXTRACT({{column_name}}, r"(?:.*)_(.*)$") AS ad_pic,
    {% endset -%}
    {% do return(extracted_fields) %}
{% endmacro %}

{% macro parse_adset_name(column_name) %}
    -- int _1824fm-vn-web visitor
    {%- set extracted_fields %}
    SPLIT({{column_name}} ,"_") [safe_offset(0)] AS audience_type,
    CASE LEFT({{column_name}} , 3)
    WHEN 'lal' THEN 'Lookalike'
    WHEN 'int' THEN 'Interest'
    WHEN 'ret' THEN 'Retargeting'
    WHEN 'mas' THEN 'Mass'
    ELSE 'Unknown'END AS target_method,
    SPLIT({{column_name}} ,"_") [safe_offset(1)] AS original_audience_name,
    SPLIT(SPLIT({{column_name}} , "_") [safe_offset(1)], "-") [safe_offset(0)] AS audience_demographic,
    SPLIT(SPLIT({{column_name}} , "_") [safe_offset(1)], "-") [safe_offset(1)] AS audience_region,
    SPLIT(SPLIT({{column_name}} , "_") [safe_offset(1)], "-") [safe_offset(2)] AS audience_source_name,
    {% endset -%}
    {% do return(extracted_fields) %}
{% endmacro %}

{%- macro parse_ad_name(column_name) %}
    {% set media_types ={ "crs" :"Carousel",
    "vid" :"Video",
    "sgm" :"Single Image",
    "clt" :"Collection",
    "rls" :"Reels",
    "mix" :"Mixed",
    "str" :"Story",
    "pcr" :"Product Catalog + Carousel",
    "pcl" :"Product Catalog + Collection",
    "pcm" :"Product Catalog + Mixed",
    "liv" :"Live Stream",
    "txt": "Text" } %}
    -- int _1824fm-vn-web visitor
    {%-set media_type_code-%}
    SPLIT({{column_name}},"_") [safe_offset(0)]
    {%-endset-%}
    {%- set extracted_fields %}
    case 
    {%-for code, media_type in media_types.items() %}
    when {{media_type_code}} = '{{code}}' then '{{media_type}}'
    {%endfor-%} end AS media_type,
    SPLIT({{column_name}},"_") [safe_offset(1)] AS content_code,
    {% endset -%}
    {% do return(extracted_fields) %}
{% endmacro -%}
