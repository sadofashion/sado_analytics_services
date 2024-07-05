{{ config(
    materialized = 'view',
    tags = ['fb','dimension','view']
) }}

{% set products_mapping ={ "ABZ" :["áo blazer"],
"ACN" :["áo chống nắng","áo cn"],
"AGB" :["áo gió bộ"],
"AGN" :["áo giữ nhiệt"],
"AKB" :["bomber","akb"],
"AKC" :["áo phao","phao","akc"],
"AKD" :["áo khoác da"],
"AKG" :["áo gió","akg"],
"ALO" :["áo len"],
"ANB" :["áo nỉ bộ"],
"ANO" :["áo nỉ","ani","ano"],
"APB" :["áo bộ polo"],
"APC" :["polo","apc"],
"APD" :["polo dài tay"],
"APO" :["áo thun","thun nỉ"],
"ATS" :["t-shirt","tshirt","shirt","ats"],
"ATT" :["tanktop","áo ba lỗ","abl","att"],
"AVB" :["áo vest","bộ đồ"],
"BNI" :["bộ nỉ","bni"],
"PKN" :["phụ kiện","sịp"],
"QBD" :["jeans","qbd"],
"QDT" :["quần gió ","qdt"],
"QGB" :["quần gió bộ"],
"QKD" :["kaki dài","qkd"],
"QNB" :["quần nỉ bộ"],
"QNI" :["quần nỉ","qni"],
"QSC" :["short casual","qsc"],
"QSG" :["short gió","qsg"],
"QSK" :["short kk","qsk"],
"QST" :["short tt","short thể thao","bộ thể thao","qst","qsa","short âu"],
"QVE" :["quần vest"],
"SMC" :["smc","sơ mi cộc"],
"SMD" :["smd","sơ mi dài"],} %}
{% set compiled_products ={ "thu đông" :["th hàng đông","thu đông","đông sang","đông th"],
"xuân hè" :["th hàng hè","hè th"],
"quanh năm" :["th quanh năm","quanh năm th"],} %}




WITH 
campaign_names as (
    select 
    _batched_at, 
    safe_cast(account_id as string) account_id ,
    safe_cast(campaign_id as string) campaign_id,
    safe_cast(adset_id as string) adset_id ,
    safe_cast(ad_id as string) ad_id ,
    campaign_name, adset_name, ad_name
    from {{ source("facebookAds","p_AdsInsights__*") }}
    where date_start < '2024-07-01'
    union all
    select 
    cast(null as timestamp) as _batched_at, account_id, campaign_id, adset_id, ad_id, campaign_name, adset_name, ad_name
    from {{ ref("dim_fb__campaigns") }}
),
current_campaign_name AS (

    SELECT
        DISTINCT account_id,
        {# FIRST_VALUE (account_name) over  ad_window AS account_name, #}
        FIRST_VALUE (campaign_name) over campaign_window AS campaign_name,
        FIRST_VALUE (adset_name) over adset_window AS adset_name,
        FIRST_VALUE (ad_name) over ad_window AS ad_name,
        {# MIN(date_start) over campaign_window AS campaign_start_date, #}
        {# MAX(date_start) over campaign_window AS campaign_stop_date, #}
        campaign_id,
        adset_id,
        ad_id,
        {{ dbt_utils.generate_surrogate_key(['account_id','campaign_id','adset_id','ad_id']) }} AS ad_key,
    FROM
        campaign_names
        window campaign_window AS (
            PARTITION BY campaign_id
            ORDER BY
                _batched_at DESC rows BETWEEN unbounded preceding
                AND unbounded following
        ),
        adset_window AS (
            PARTITION BY campaign_id,
            adset_id
            ORDER BY
                _batched_at DESC rows BETWEEN unbounded preceding
                AND unbounded following
        ),
        ad_window AS (
            PARTITION BY campaign_id,
            adset_id,
            ad_id
            ORDER BY
                _batched_at DESC rows BETWEEN unbounded preceding
                AND unbounded following
        ) 
        {# account_window as (partition by account_id order by _batched_at desc), #}
        {# campaign_window as (partition by campaign_id) #}
),
convention_version AS (
    SELECT
        *,
        CASE
            WHEN campaign_name LIKE '5S%' THEN 'B2406'
            ELSE '2406'
        END AS convention_version_number
    FROM
        current_campaign_name
),
new_naming_convention AS (
    SELECT
        *,
        {{ parse_naming_convention(
            campaign_col = "campaign_name",
            adset_col = "adset_name",
            ad_col = "ad_name"
        ) }}
    FROM
        convention_version
    WHERE
        convention_version.convention_version_number = '2406'

),
old_naming_convention AS (
    SELECT
        *,
        regexp_extract (LOWER(campaign_name), r"^(?:.*?_){4}(.*?)_(?:.*?)$") AS big_campaign,
        regexp_extract (LOWER(campaign_name), r"^(?:.*?_){4}(.*?_.*?)_(?:.*?)$") AS content_group,
        regexp_extract (LOWER(campaign_name), r"^(?:.*?_){1}(.*?)_(?:.*?)$") AS pic,
        regexp_extract (LOWER(campaign_name), r"^(?:.*?_){6}(.*?)_(?:.*?)$") AS promoted_productline,
        regexp_extract (
            campaign_name,
            r"^(.*?)_"
        ) AS page,
        regexp_extract (LOWER(campaign_name), r"(?:.*?_){7}(.*?)_(?:.*?)") AS media_type,
        regexp_extract (LOWER(campaign_name), r"^(?:.*?_){2}(.*?)_(?:.*?)$") AS funnel,
        regexp_extract (LOWER(campaign_name), r"^(?:.*?_){3}(.*?)_(?:.*?)$") AS ad_type,
    FROM
        convention_version
    WHERE
        convention_version.convention_version_number = 'B2406'
        ),
    renaming_old_convention AS (
            SELECT
                o.account_id,
                {# account_name, #}
                o.campaign_name,
                o.adset_name,
                o.ad_name,
                {# campaign_start_date, #}
                {# campaign_stop_date, #}
                o.campaign_id,
                o.adset_id,
                o.ad_id,
                o.ad_key,
                o.convention_version_number,
                'fb' AS channel,
                '5s' AS brand_name,
                o.page AS ad_location,
                CASE
                    WHEN o.page IN (
                        "5SFTHA",
                        "5SFTIE",
                        "5SFTUN",
                        "5SFTRA",
                        "5SFT",
                        "5SFG",
                        "5SF",
                        "5SFTUY"
                    ) THEN "PIC Region"
                    ELSE "Store"
                END AS ad_location_layer,
                o.ad_type AS campaign_category,
                o.big_campaign AS event_name,
                CASE
                {% for k, v in products_mapping.items() -%}
                    WHEN regexp_contains(CONCAT(o.promoted_productline, o.content_group), r"{{v|join('|')}}") THEN 'sp {{k|lower()}}'
                {% endfor -%}
                {% for k, v in compiled_products.items() -%}
                    WHEN regexp_contains(CONCAT(o.promoted_productline, o.content_group), r"{{v|join('|')}}") THEN 'th {{k|lower()}}'
                {% endfor -%}END AS content_edge,
                o.pic AS ad_pic,
                CAST(NULL AS STRING) AS audience_type,
                CAST(NULL AS STRING) AS target_method,
                CAST(NULL AS STRING) AS original_audience_name,
                CAST(NULL AS STRING) AS audience_demographic,
                CAST(NULL AS STRING) AS audience_region,
                o.funnel AS audience_source_name,
                o.media_type,
                o.content_group AS content_code
            FROM
                old_naming_convention o
        )

        SELECT
            *
        FROM
            new_naming_convention
        UNION ALL
        SELECT
            *
        FROM
            renaming_old_convention