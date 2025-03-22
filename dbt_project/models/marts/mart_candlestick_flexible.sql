-- models/marts/mart_candlestick_flexible.sql
{{
    config(
        materialized='table'
    )
}}

-- Keep the aggregated array structure, just add time period flags
SELECT
    ticker,
    year,
    month_num,
    composite_col,
    -- Add time period flags for easy API filtering
    CASE
        WHEN year = (Select max(year) from {{ ref('int_cumulated_daily_aggregation') }})
                AND month_num = (Select max(month_num) from {{ ref('int_cumulated_daily_aggregation') }})  THEN TRUE
        ELSE FALSE
    END as is_current_month,

    CASE
        WHEN year = (Select max(year) from {{ ref('int_cumulated_daily_aggregation') }}) THEN TRUE
        ELSE FALSE
    END as is_current_year
FROM {{ ref('int_cumulated_daily_aggregation') }}