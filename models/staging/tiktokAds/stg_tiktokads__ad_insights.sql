{{ config(
    tags = ['view','tiktok']
) }}

{% set dimensions = ["ad_id","stat_time_day"] %}
{% set metrics ={ "string" :["adgroup_id", "adgroup_name", "advertiser_id", "advertiser_name","campaign_id", "campaign_name","placement_type","objective_type",],
"float64" :["add_to_wishlist", "add_to_wishlist_rate", "app_event_add_to_cart", "app_event_add_to_cart_rate", "average_video_play", "average_video_play_per_user", "checkout", "checkout_rate", "clicks", "comments", "complete_payment", "complete_payment_rate", "conversion", "conversion_rate", "cost_per_1000_reached", "cost_per_add_to_wishlist", "cost_per_app_event_add_to_cart", "cost_per_checkout", "cost_per_complete_payment", "cost_per_conversion", "cost_per_cta_purchase", "cost_per_cta_registration", "cost_per_download_start", "cost_per_initiate_checkout", "cost_per_on_web_add_to_wishlist", "cost_per_page_event_search", "cost_per_product_details_page_browse", "cost_per_purchase", "cost_per_registration", "cost_per_result", "cost_per_secondary_goal_result", "cost_per_total_add_to_wishlist", "cost_per_total_app_event_add_to_cart", "cost_per_total_checkout", "cost_per_total_purchase", "cost_per_total_registration", "cost_per_total_view_content", "cost_per_user_registration", "cost_per_view_content", "cost_per_vta_conversion", "cost_per_vta_purchase", "cost_per_vta_registration", "cost_per_web_event_add_to_cart", "cpc", "cpm", "cta_app_install", "cta_conversion", "cta_purchase", "cta_registration", "ctr", "download_start", "download_start_rate", "dpa_target_audience_type", "engaged_view", "engaged_view_15s", "follows", "impressions", "initiate_checkout", "initiate_checkout_rate", "likes", "on_web_add_to_wishlist", "on_web_add_to_wishlist_per_click", "page_event_search", "page_event_search_rate", "product_details_page_browse", "product_details_page_browse_rate", "profile_visits", "purchase", "purchase_rate", "reach", "real_time_conversion", "real_time_conversion_rate", "real_time_cost_per_conversion", "real_time_cost_per_result", "real_time_result", "real_time_result_rate", "registration", "registration_rate", "result", "result_rate", "sales_lead", "secondary_goal_result", "secondary_goal_result_rate", "shares", "skan_sales_lead", "skan_total_sales_lead", "skan_total_sales_lead_value", "spend", "total_active_pay_roas", "total_add_to_wishlist", "total_add_to_wishlist_value", "total_app_event_add_to_cart", "total_app_event_add_to_cart_value", "total_checkout", "total_checkout_value", "total_complete_payment_rate", "total_download_start_value", "total_initiate_checkout_value", "total_on_web_add_to_wishlist_value", "total_page_event_search_value", "total_product_details_page_browse_value", "total_purchase", "total_purchase_value", "total_registration", "total_sales_lead", "total_sales_lead_value", "total_user_registration_value", "total_view_content", "total_view_content_value", "total_web_event_add_to_cart_value", "user_registration", "user_registration_rate", "value_per_checkout", "value_per_complete_payment", "value_per_download_start", "value_per_initiate_checkout", "value_per_on_web_add_to_wishlist", "value_per_page_event_search", "value_per_product_details_page_browse", "value_per_total_add_to_wishlist", "value_per_total_app_event_add_to_cart", "value_per_total_purchase", "value_per_total_view_content", "value_per_user_registration", "value_per_web_event_add_to_cart", "video_play_actions", "video_views_p100", "video_views_p25", "video_views_p50", "video_views_p75", "video_watched_2s", "video_watched_6s", "view_content", "view_content_rate", "vta_app_install", "vta_conversion", "vta_purchase", "vta_registration", "web_event_add_to_cart", "web_event_add_to_cart_rate"] } %}
WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'tiktok',
            'ad_report_daily'
        ),
        partition_by = "JSON_VALUE(data, '$.dimensions.ad_id'),JSON_VALUE(data, '$.dimensions.stat_time_day')",
        order_by = '_batched_at desc'
    ) }}
),
unnested_keys AS (
    SELECT
        {% for dimension in dimensions -%}
            json_value(
                DATA,
                '$.dimensions.{{dimension}}'
            ) AS {{ dimension }},
        {%- endfor %}
        {%- for type,metric_group in metrics.items() -%}
            {%- for metric in metric_group -%}
            safe_cast(json_value(DATA, '$.metrics.{{metric}}') AS {{ type }}) AS {{ metric }},
            {% endfor %}
        {%- endfor %}
    FROM
        source
)
SELECT
    *
EXCEPT(stat_time_day),
    DATE(stat_time_day) AS date
FROM
    unnested_keys
