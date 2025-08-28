select
    date_id,
    date,
    year,
    month,
    day,
    time

from {{ source('my_store', 'datetable') }}