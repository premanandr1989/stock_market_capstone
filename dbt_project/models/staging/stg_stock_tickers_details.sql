with source as (

    select *,
        row_number() over(partition by ticker order by ticker) as rn
    from {{ source('PREM', 'stock_tickers_details_enriched') }}
    where sic_code is not null and type is not null
)

select *
from source
where rn=1