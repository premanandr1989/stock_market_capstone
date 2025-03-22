
with source as (

    select * from {{ source('PREM', 'sic_codes') }}
)

select *
from source