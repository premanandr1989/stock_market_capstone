{{
    config(
        materialized='incremental',
        unique_key=['ticker', 'date'],
        cluster_by = ['ticker','date'],
        incremental_strategy = 'append'
    )
}}
WITH cte_earnings_calendar as (
    Select
        *,
        row_number() over(partition by ticker order by date,quarter desc) as row_num
    from
        {{ ref('stg_earnings_calendar') }}
    where epsEstimate <> 0
    )
{% if var('apply_split_adjustment') %}
    , cte_earnings_surprise as (
        Select
            *,
            row_number() over(partition by ticker order by date desc) as row_num
        from
            {{ ref('stg_earnings_surprises') }}
    )
    Select
        ticker,
        year,
        date,
        quarter,
        epsActual,
        epsEstimate,
        (epsActual-epsEstimate) as epsSurprise,
        (epsActual-epsEstimate)/ abs(epsEstimate) * 100 as epsSurprisePercent,
        (revenueActual-revenueEstimate) as revenueSurprise,
        (revenueActual-revenueEstimate)/ abs(CASE WHEN revenueEstimate = 0 THEN 1 ELSE revenueEstimate END) * 100 as revenueSurprisePercent,
        row_num
    from cte_earnings_calendar
    union
    Select
        ticker,
        year,
        date,
        quarter,
        actual,
        estimate,
        surprise,
        surprisePercent,
        null as revenueSurprise,
        null as revenueSurprisePercent,
        row_num
    from cte_earnings_surprise
{% else %}

Select
    ticker,
        year,
        date,
        quarter,
        epsActual,
        epsEstimate,
        (epsActual-epsEstimate) as epsSurprise,
        (epsActual-epsEstimate)/ abs(epsEstimate) * 100 as epsSurprisePercent,
        (revenueActual-revenueEstimate) as revenueSurprise,
        (revenueActual-revenueEstimate)/ abs(CASE WHEN revenueEstimate = 0 THEN 1 ELSE revenueEstimate END) * 100 as revenueSurprisePercent,
        row_num
from
    cte_earnings_calendar
{% endif %}
{% if is_incremental() %}
    WHERE date > (SELECT max(date) FROM {{ this }})
{% endif %}

