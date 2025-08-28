select
    item_id,
    item_name,
    unit_price

from {{ source('my_store', 'items') }}