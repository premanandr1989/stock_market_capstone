-- models/marts/mart_ema_flexible.sql
{{
    config(
        materialized='table'
    )
}}

WITH daily_data AS (
    SELECT
        ticker,
        is_current_year,
        is_current_month,
        value:date::date as date,
        value:close::float as close
    FROM {{ ref('mart_candlestick_flexible') }},
    LATERAL FLATTEN(input => composite_col)
),
base_data AS (
    SELECT
        ticker,
        date,
        close,
        is_current_year,
        is_current_month,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
    FROM daily_data
),
sma_data AS (
    SELECT
        b.ticker,
        b.date,
        b.close,
        b.is_current_year,
        b.is_current_month,
        b.rn,
        sma_10,
        sma_20,
        sma_50,
        sma_100,
        sma_200
    FROM base_data b
    LEFT JOIN {{ ref('mart_sma_flexible') }} s
        ON b.ticker = s.ticker AND b.date = s.date
),
ema_10 AS (
    -- Anchor: First row uses SMA
    SELECT
        ticker,
        date,
        close,
        is_current_year,
        is_current_month,
        rn,
        sma_10 as ema_10
    FROM sma_data
    WHERE rn = 1

    UNION ALL

    -- Recursive part
    SELECT
        s.ticker,
        s.date,
        s.close,
        s.is_current_year,
        s.is_current_month,
        s.rn,
        ((s.close - e.ema_10) * (2.0/11.0)) + e.ema_10 as ema_10
    FROM sma_data s
    JOIN ema_10 e ON s.ticker = e.ticker AND s.rn = e.rn + 1
),
ema_20 AS (
    SELECT
        ticker,
        date,
        close,
        is_current_year,
        is_current_month,
        rn,
        sma_20 as ema_20
    FROM sma_data
    WHERE rn = 1

    UNION ALL

    SELECT
        s.ticker,
        s.date,
        s.close,
        s.is_current_year,
        s.is_current_month,
        s.rn,
        ((s.close - e.ema_20) * (2.0/21.0)) + e.ema_20 as ema_20
    FROM sma_data s
    JOIN ema_20 e ON s.ticker = e.ticker AND s.rn = e.rn + 1
),
ema_50 AS (
    SELECT
        ticker,
        date,
        close,
        is_current_year,
        is_current_month,
        rn,
        sma_50 as ema_50
    FROM sma_data
    WHERE rn = 1

    UNION ALL

    SELECT
        s.ticker,
        s.date,
        s.close,
        s.is_current_year,
        s.is_current_month,
        s.rn,
        ((s.close - e.ema_50) * (2.0/51.0)) + e.ema_50 as ema_50
    FROM sma_data s
    JOIN ema_50 e ON s.ticker = e.ticker AND s.rn = e.rn + 1
),
ema_100 AS (
    SELECT
        ticker,
        date,
        close,
        is_current_year,
        is_current_month,
        rn,
        sma_100 as ema_100
    FROM sma_data
    WHERE rn = 1

    UNION ALL

    SELECT
        s.ticker,
        s.date,
        s.close,
        s.is_current_year,
        s.is_current_month,
        s.rn,
        ((s.close - e.ema_100) * (2.0/101.0)) + e.ema_100 as ema_100
    FROM sma_data s
    JOIN ema_100 e ON s.ticker = e.ticker AND s.rn = e.rn + 1
),
ema_200 AS (
    SELECT
        ticker,
        date,
        close,
        is_current_year,
        is_current_month,
        rn,
        sma_200 as ema_200
    FROM sma_data
    WHERE rn = 1

    UNION ALL

    SELECT
        s.ticker,
        s.date,
        s.close,
        s.is_current_year,
        s.is_current_month,
        s.rn,
        ((s.close - e.ema_200) * (2.0/201.0)) + e.ema_200 as ema_200
    FROM sma_data s
    JOIN ema_200 e ON s.ticker = e.ticker AND s.rn = e.rn + 1
)

SELECT
    e10.ticker,
    e10.date,
    e10.close,
    e10.ema_10,
    e20.ema_20,
    e50.ema_50,
    e100.ema_100,
    e200.ema_200,
    EXTRACT(YEAR FROM e10.date) as year,
    EXTRACT(MONTH FROM e10.date) as month_num,
    e10.is_current_month,
    e10.is_current_year
FROM ema_10 e10
JOIN ema_20 e20 ON e10.ticker = e20.ticker AND e10.date = e20.date
JOIN ema_50 e50 ON e10.ticker = e50.ticker AND e10.date = e50.date
JOIN ema_100 e100 ON e10.ticker = e100.ticker AND e10.date = e100.date
JOIN ema_200 e200 ON e10.ticker = e200.ticker AND e10.date = e200.date
ORDER BY e10.date