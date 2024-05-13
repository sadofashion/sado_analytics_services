{{ config(
    tags = ['view','tiktok']
) }}

WITH source AS (
    SELECT
        json_value(data,'$.ad_format') AS ad_format,
        json_value(data,'$.ad_id') AS ad_id,
        json_value(data,'$.ad_name') AS ad_name,
        json_value(data,'$.ad_text') AS ad_text,
        json_value(data,'$.ad_texts') AS ad_texts,
        json_value(data,'$.adgroup_id') AS adgroup_id,
        json_value(data,'$.adgroup_name') AS adgroup_name,
        json_value(data,'$.advertiser_id') AS advertiser_id,
        json_value(data,'$.app_name') AS app_name,
        json_value(data,'$.avatar_icon_web_uri') AS avatar_icon_web_uri,
        json_value(data,'$.brand_safety_postbid_partner') AS brand_safety_postbid_partner,
        json_value(data,'$.brand_safety_vast_url') AS brand_safety_vast_url,
        json_value(data,'$.call_to_action') AS call_to_action,
        json_value(data,'$.call_to_action_id') AS call_to_action_id,
        json_value(data,'$.campaign_id') AS campaign_id,
        json_value(data,'$.campaign_name') AS campaign_name,
        json_value(data,'$.card_id') AS card_id,
        json_value(data,'$.carousel_image_labels') AS carousel_image_labels,
        json_value(data,'$.click_tracking_url') AS click_tracking_url,
        datetime(json_value(data,'$.create_time')) AS create_time,
        safe_cast(json_value(data,'$.creative_authorized') as bool) AS creative_authorized,
        json_value(data,'$.creative_type') AS creative_type,
        json_value(data,'$.deeplink') AS deeplink,
        json_value(data,'$.deeplink_type') AS deeplink_type,
        json_value(data,'$.display_name') AS display_name,
        json_value(data,'$.identity_id') AS identity_id,
        json_value(data,'$.identity_type') AS identity_type,
        json_extract_array(data,'$.image_ids') AS image_ids,
        json_value(data,'$.impression_tracking_url') AS impression_tracking_url,
        safe_cast(json_value(data,'$.is_aco') as bool) AS is_aco,
        safe_cast(json_value(data,'$.is_new_structure') as bool) AS is_new_structure,
        json_value(data,'$.landing_page_url') AS landing_page_url,
        json_value(data,'$.landing_page_urls') AS landing_page_urls,
        datetime(json_value(data,'$.modify_time')) AS modify_time,
        json_value(data,'$.music_id') AS music_id,
        json_value(data,'$.operation_status') AS operation_status,
        json_value(data,'$.optimization_event') AS optimization_event,
        json_value(data,'$.page_id') AS page_id,
        json_value(data,'$.playable_url') AS playable_url,
        json_value(data,'$.profile_image_url') AS profile_image_url,
        json_value(data,'$.secondary_status') AS secondary_status,
        json_value(data,'$.tracking_pixel_id') AS tracking_pixel_id,
        safe_cast(json_value(data,'$.vast_moat_enabled') as bool) AS vast_moat_enabled,
        json_value(data,'$.video_id') AS video_id,
        json_value(data,'$.viewability_postbid_partner') AS viewability_postbid_partner,
        json_value(data,'$.viewability_vast_url') AS viewability_vast_url,
    FROM
        {{ source('tiktok','ad') }}
) 

{{ dbt_utils.deduplicate(
    relation = "source",
    partition_by = "advertiser_id, campaign_id, adgroup_id,ad_id",
    order_by = "modify_time desc"
) }}
