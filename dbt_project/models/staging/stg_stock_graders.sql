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
    from {{ source('PREM', 'stock_graders') }}
)

select distinct s.*
from source s
{% if is_incremental() %}
    where date > (select coalesce(max(date),'1900-01-01') from {{ this }})
{% endif %}