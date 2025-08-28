select
    customer_id,
    first_name,
    last_name,
    address,
    email

from {{ source('my_store', 'customers') }}