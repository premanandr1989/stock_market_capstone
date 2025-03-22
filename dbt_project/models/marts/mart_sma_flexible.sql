{{
    config(
        materialized='table',
        cluster_by = ['ticker']
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
sma_calculations AS (
    SELECT
        ticker,
        date,
        close,
        is_current_month,
        is_current_year,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as sma_10,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as sma_20,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) as sma_50,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        ) as sma_100,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) as sma_200,
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month_num
    FROM daily_data
)

SELECT
    ticker,
    date,
    close,
    sma_10,
    sma_20,
    sma_50,
    sma_100,
    sma_200,
    year,
    month_num,
    -- Time period flags
    is_current_month,
    is_current_year

FROM sma_calculations