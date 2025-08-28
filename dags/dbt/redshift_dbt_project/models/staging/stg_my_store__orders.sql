select
    order_id,
    date_id,
    customer_id,
    item_id,
    quantity,
    amount

from {{ source('my_store', 'orders') }}
