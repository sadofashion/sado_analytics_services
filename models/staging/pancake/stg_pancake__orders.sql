{%set pancake_order_statuses = {
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


SELECT
    customer_id,
    recent_orders.assigning_seller_id as seller_id,
    recent_orders.display_id,
    recent_orders.id as order_id,
    recent_orders.full_name,
    recent_orders.inserted_at as order_created_at,
    recent_orders.is_locked,
    recent_orders.last_update_status_at as order_modified_at,
    recent_orders.payment,
    recent_orders.phone_number as customer_contact_number,
    recent_orders.recipient_location,
    case recent_orders.status
    {%for key, value in pancake_order_statuses.items()%}
        when {{key}} then "{{value}}"
    {%endfor%}
    end as status,
FROM
    {{ ref("base_pancake__customers") }},unnest(recent_orders) recent_orders
