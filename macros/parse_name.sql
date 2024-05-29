{%- macro parse_naming_convention(campaign_col,adset_col, ad_col)%}
{{parse_campaign_name(campaign_col)}}
{{parse_adset_name(adset_col)}}
{{parse_ad_name(ad_col)}}
{% endmacro -%}


{% macro parse_campaign_name(column_name) %}
    -- fb_vn_engagement_{true summer_}_tung.ng
    {%- set extracted_fields %}
    SPLIT({{column_name}},"_") [safe_offset(0)] AS channel,
    SPLIT({{column_name}},"_") [safe_offset(1)] AS localtion,
    CASE
        length(SPLIT({{column_name}}, "_") [safe_offset(1)])
        WHEN 2 THEN "Country"
        WHEN 3 THEN "Province"
        WHEN 4 THEN "Economic Region"
        WHEN 5 THEN "Store"
    END AS localtion_layer,
    SPLIT({{column_name}},"_") [safe_offset(2)] AS campaign_category,
    REGEXP_REPLACE(SPLIT({{column_name}}, "_") [safe_offset(3)], r"{|}|[|]", "") AS event_name,
    REGEXP_REPLACE(SPLIT({{column_name}}, "_") [safe_offset(3)], r"{|}|[|]", "") AS content_edge,
    SPLIT({{column_name}},"_") [safe_offset(5)] AS ad_pic,
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

{% macro parse_ad_name(column_name) %}
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
    "liv" :"Live Stream" } %}
    -- int _1824fm-vn-web visitor
    {%- set extracted_fields %}
    SPLIT({{column_name}},"_") [safe_offset(0)] AS media_type,
    SPLIT({{column_name}},"_") [safe_offset(1)] AS content_code,
    {% endset -%}
    {% do return(extracted_fields) %}
{% endmacro %}
