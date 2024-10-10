with calls as (
SELECT
    cs.call_status,
    cs.call_month,
    c.kiotviet_customer_id as customer_id,
FROM
    {{ ref("stg_gsheet__cs_calls") }} cs
INNER JOIN {{ ref('fct__customers') }} c
    ON cs.customer_phone = c.contact_number
    and kiotviet_customer_id is not null
),

transactions as (
    select 
        * 
    from {{ ref("fct__transactions") }}
    where source = 'kiotviet'
        and transaction_date >= '2024-10-01'
)

select 
    c.call_status,
    c.call_month,
    c.customer_id as customer_id,
    t.transaction_date,
from calls c
left join {{ ref("fct__transactions") }} t 
    on c.customer_id = t.customer_id 
    and c.call_status not in ("Không nghe máy","Từ chối","Thuê bao") 
    and ((t.transaction_date >= c.call_month
    and t.transaction_date < date_add(c.call_month, interval 1 month)) or t.transaction_date is null)
