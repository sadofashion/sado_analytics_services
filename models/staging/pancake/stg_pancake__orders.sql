{% set pancake_order_statuses = {
    "0":"Mới",
    "17":"Chờ xác nhận",
    "11":"Chờ hàng",
    "12":"Chờ in",
    "13":"Đã in",
    "20":"Đã đặt hàng",
    "1":"Đã xác nhận",
    "8":"Đang đóng hàng",
    "9":"Chờ chuyển hàng",
    "2":"Đã gửi hàng",
    "3":"Đã nhận",
    "16":"Đã thu tiền",
    "4":"Đang trả hàng",
    "15":"Hoàn 1 phần",
    "5":"Đã hoàn",
    "6":"Đã hủy",
    "7":"Đã xóa",
    "10":"Đơn Webcake",
    "21":"Đơn Storecake",
    }
%}


WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'pos_orders'), partition_by = 'id,shop_id', order_by = "_batched_at desc",) }}
)

select 
source.page_id,
source.id as order_id,
source.shop_id,
case source.status
{%for key, value in pancake_order_statuses.items() %}
when {{key}} then '{{value}}'
{%endfor%}
end as status,
source.shipping_address.*,
source.bill_phone_number as phone_number,
source.assigning_seller_id as seller_id,
datetime_add(datetime(source.inserted_at), interval 7 hour) inserted_at,
datetime_add(datetime(source.updated_at), interval 7 hour) updated_at,
source.customer.customer_id as customer_id,
source.conversation_id,
source.total_price_after_sub_discount,
source.total_price,
source.time_assign_seller as seller_assigned_at,
source.p_utm_campaign,
source.p_utm_medium,
source.p_utm_source,
source.p_utm_term,
source.p_utm_content,
from source