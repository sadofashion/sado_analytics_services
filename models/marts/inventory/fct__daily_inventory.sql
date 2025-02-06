with inventory as (
    select 
        * except(last_inventory_date,next_inventory_date,inventory_date),
        coalesce(lag(ending_inventory) over (partition by product_id,branch_id order by date),0) as beginning_inventory,
    from {{ ref("int_kiotviet__inventory_change") }} inv
),
quantity_sold as (
    select 
        branch_id,
        product_id,
        transaction_date,
        sum(quantity) as quantity_sold
    from {{ ref("int_kiotviet__revenue_items") }}
    group by 1,2,3
),
quantity_return as (
    select 
        branch_id,
        product_id,
        transaction_date,
        sum(quantity) as quantity_return
    from {{ ref("int_kiotviet__return_items") }}
    group by 1,2,3
),

transfers_in as (
    select 
        receipt_branch_id,
        product_id,
        date(received_date) transaction_date,
        sum(receive_quantity) as quantity_transfer_in,
    from {{ ref("stg_kiotviet__transfers_details") }}
    group by 1,2,3
),

transfers_out as (
    select 
        transfer_branch_id,
        product_id,
        date(sent_date) transaction_date,
        sum(send_quantity) as quantity_transfer_out,
    from {{ ref("stg_kiotviet__transfers_details") }}
    group by 1,2,3
)

select 
    inv.*,
    (inv.ending_inventory - inv.beginning_inventory) as quantity_change,
    (inv.ending_inventory + inv.beginning_inventory)/2 as average_inventory,
    coalesce(sold.quantity_sold,0) quantity_sold,
    coalesce(ret.quantity_return,0) quantity_return,
    coalesce(tin.quantity_transfer_in,0) quantity_transfer_in,
    coalesce(tout.quantity_transfer_out,0) quantity_transfer_out,
from inventory inv
left join quantity_sold sold on inv.branch_id = sold.branch_id and inv.product_id = sold.product_id and inv.date = sold.transaction_date
left join quantity_return ret on inv.branch_id = ret.branch_id and inv.product_id = ret.product_id and inv.date = ret.transaction_date
left join transfers_in tin on inv.branch_id = tin.receipt_branch_id and inv.product_id = tin.product_id and inv.date = tin.transaction_date
left join transfers_out tout on inv.branch_id = tout.transfer_branch_id and inv.product_id = tout.product_id and inv.date = tout.transaction_date