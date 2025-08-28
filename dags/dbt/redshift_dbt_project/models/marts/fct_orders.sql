with customers as (
    select * from {{ ref("stg_my_store__customers") }}
),

items as (
    select * from {{ ref("stg_my_store__items") }}
),

dates as (
    select * from {{ ref("stg_my_store__datetable") }}
),

orders as (
    select * from {{ ref("stg_my_store__orders") }}
),

final as (
    select
        orders.order_id,
        dates.date,
        customers.customer_id,
        items.item_name,
        orders.quantity,
        orders.amount

    from orders
    left join customers using (customer_id)
    left join items using (item_id)
    left join dates using (date_id)
)

select * from final
