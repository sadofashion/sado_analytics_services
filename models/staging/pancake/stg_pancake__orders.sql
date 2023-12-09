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
    recent_orders.status,
FROM
    {{ ref("base_pancake__customers") }},unnest(recent_orders) recent_orders
