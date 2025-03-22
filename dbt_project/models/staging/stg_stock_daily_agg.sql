{{
  config(
          materialized = 'incremental',
    	  unique_key = ['ticker','date'],
          cluster_by = ['ticker','date'],
          incremental_strategy = 'append'
  )
}}

with source as (

    select
        *
    from {{ source('PREM', 'stock_daily_agg') }}
    where volume > 0 and transactions > 0 and volume is not null and transactions is not null
)

select s.*
from source s
join {{ ref('stg_stock_tickers') }} st on st.ticker = s.ticker
{% if is_incremental() %}
    where date > (select coalesce(max(date),'1900-01-01') from {{ this }})
{% endif %}




