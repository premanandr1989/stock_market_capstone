
with source as (

    select * from {{ source('PREM', 'stock_exchange_details') }}
    where asset_class = 'stocks'
)

select *
from source