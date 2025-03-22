-- models/marts/mart_wma_flexible.sql
{{
    config(
        materialized='table'
    )
}}

WITH daily_data AS (
    SELECT
        ticker,
        is_current_month,
        is_current_year,
        value:date::date as date,
        value:close::float as close
    FROM {{ ref('mart_candlestick_flexible') }},
    LATERAL FLATTEN(input => composite_col)
),
wma_calculations AS (
    SELECT
        ticker,
        date,
        is_current_month,
        is_current_year,
        close,
        {{ calculate_wma('ticker', 'date', 'close', 10) }} as wma_10,
        {{ calculate_wma('ticker', 'date', 'close', 20) }} as wma_20,
        {{ calculate_wma('ticker', 'date', 'close', 50) }} as wma_50,
        {{ calculate_wma('ticker', 'date', 'close', 100) }} as wma_100,
        {{ calculate_wma('ticker', 'date', 'close', 200) }} as wma_200,
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month_num
    FROM daily_data
)

SELECT
    ticker,
    date,
    close,
    wma_10,
    wma_20,
    wma_50,
    wma_100,
    wma_200,
    year,
    month_num,
   is_current_month,
   is_current_year

FROM wma_calculations
where wma_10 is not null and wma_20 is not null and wma_50 is not null and wma_100 is not null and wma_200 is not null