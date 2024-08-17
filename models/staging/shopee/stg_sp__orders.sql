{%set statuses = {
    "SHIPPED": ['READY_TO_SHIP','PROCESSED','RETRY_SHIP','SHIPPED'],
    "CANCELLED": ['CANCELLED','IN_CANCEL'],
    "TO_RETURN":["TO_RETURN"],
    "COMPLETED":["COMPLETED"],
    "UNPAID":["UNPAID"],
    "TO_CONFIRM_RECEIVE":["TO_CONFIRM_RECEIVE"],
}%}

{%set reasons= {
    "Need to Change Delivery Address":'Thay đổi địa chỉ giao hàng',
    "Don't Want to Buy Anymore":'Không còn nhu cầu',
    "Modify existing order (colour, size, address, voucher, etc.)":'Thay đổi thông tin đơn hàng đã có',
    "Need to Modify Order":"Cần thay đổi thông tin đơn hàng",
    "Need to input / Change Voucher Code":"Thêm/đổi mã khuyến mại",
    "Others":"Khác",
    "Others / change of mind":"Khác/ Đổi ý",
    "Unpaid Order":"Chưa thanh toán",
    "Failed Delivery":"Giao thất bại",
    "Payment Procedure too Troublesome":"Lỗi thanh toán",
    "Need to change delivery address":"Thay đổi địa chỉ giao hàng",
    "Found Cheaper Elsewhere":"Tìm thấy sản phẩm giá thấp hơn",
    "Other":"Khác",
    "Seller is not responsive to my inquiries":"Người bán không phản hồi",
    "Unsuccessful / Rejected Payment":"Thanh toán không thành công/bị từ chối",
    "Seller did not Ship":"Người bán không giao hàng"
}%}


with source as (
    {{
        dbt_utils.deduplicate(
            relation = source('shopee', 'order_list'), 
            partition_by = 'json_value(data,"$.order_sn")', 
            order_by = '_batched_at desc'
            )
            }}
)

select 
case 
    when nullif(json_value(o.data,'$.cancel_by'),"") ="buyer" then "Người dùng" 
    when nullif(json_value(o.data,'$.cancel_by'),"") ="system" then "Hệ thống" 
    end as cancel_by,
case {% for k,v in reasons.items()-%}
    when nullif(json_value(o.data,'$.cancel_reason'),"") = "{{k}}" then "{{v}}"
{% endfor -%} end as cancel_reason,
date_add(timestamp_seconds(safe_cast(json_value(o.data,'$.create_time') as int64)),interval 7 hour) as create_time,
date_add(timestamp_seconds(safe_cast(json_value(o.data,'$.update_time') as int64)), interval 7 hour) as update_time,
safe_cast(json_value(o.data,'$.total_amount') as float64) as total_amount,
json_value(o.data,'$.order_sn') as order_sn,
case  {% for k,v in statuses.items()-%}
    when json_value(o.data,'$.order_status') in ("{{v|join('","')}}") then "{{k}}"
{% endfor -%} end as order_status,
json_value(o.data,'$.payment_method') as payment_method,
json_extract_array(o.data,'$.item_list') item_list,
json_extract_array(o.data,'$.package_list') package_list,
from source o