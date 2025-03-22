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
    from {{ source('PREM', 'earnings_surprises') }}
    where Actual is not null and Estimate is not null and surprise is not null and surprisePercent is not null
)

select s.*
from source s
{% if is_incremental() %}
    where date > (select coalesce(max(date),'1900-01-01') from {{ this }})
{% endif %}