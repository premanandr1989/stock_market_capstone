with tickers as (

    select
        t.ticker,
        t.name,
        t.active,
        td.sic_code,
        td.sic_description,
        sc.office,
        sc.industry,
        td.market_cap,
        td.branding_icon_url,
        td.branding_logo_url,
        td.SHARE_CLASS_SHARES_OUTSTANDING,
        td.WEIGHTED_SHARES_OUTSTANDING,
        td.PRIMARY_EXCHANGE,
        td.TOTAL_EMPLOYEES

    from {{ ref('stg_stock_tickers') }} as t
    join {{ ref('stg_stock_tickers_details') }} as td on t.ticker = td.ticker
    join {{ ref('stg_sic_codes') }} as sc on sc.code = td.sic_code

)

select *
from tickers