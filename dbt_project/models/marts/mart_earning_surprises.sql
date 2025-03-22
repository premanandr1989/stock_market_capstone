{{
    config(
        materialized='table'
    )
}}

Select
    ticker,
    year(date) as year,
    date,
    quarter,
    epsActual,
    epsEstimate,
    epsSurprise,
    epsSurprisePercent,
    revenueSurprise,
    revenueSurprisePercent
from {{ ref('int_earnings_surprises') }}