with source as (

    select * from {{ source('PREM', 'stock_ticker_splits') }}
    where split_from is not null and split_to is not null
)

select *
from source
where split_to > 0


