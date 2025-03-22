{{
  config(
          materialized = 'incremental',
    	  unique_key = ['ticker','timestamp'],
          cluster_by = ['ticker','timestamp'],
          incremental_strategy = 'append'
  )
}}

with source as (

    select * from {{ source('PREM', 'mart_sma_errors') }}
)

select *
from source
{% if is_incremental() %}
    where timestamp > (select coalesce(max(timestamp),'1900-01-01') from {{ this }})
{% endif %}