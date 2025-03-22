-- models/mart/mart_macd_flexible.sql

WITH base_data AS (
    SELECT
        ticker,
        date,
        close,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) as rn
    FROM {{ ref('int_daily_metrics') }}
),

-- EMA 12 calculation
ema_12 AS (
    -- Initial value using SMA
    SELECT
        ticker,
        date,
        close,
        rn,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as ema_12
    FROM base_data
    WHERE rn = 12

    UNION ALL

    SELECT
        b.ticker,
        b.date,
        b.close,
        b.rn,
        ((b.close - e.ema_12) * (2.0/13.0)) + e.ema_12 as ema_12
    FROM base_data b
    JOIN ema_12 e ON b.ticker = e.ticker AND b.rn = e.rn + 1
),

-- EMA 26 calculation
ema_26 AS (
    -- Initial value using SMA
    SELECT
        ticker,
        date,
        close,
        rn,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
        ) as ema_26
    FROM base_data
    WHERE rn = 26

    UNION ALL

    SELECT
        b.ticker,
        b.date,
        b.close,
        b.rn,
        ((b.close - e.ema_26) * (2.0/27.0)) + e.ema_26 as ema_26
    FROM base_data b
    JOIN ema_26 e ON b.ticker = e.ticker AND b.rn = e.rn + 1
),

-- Combine EMAs and calculate MACD line
macd_line_calc AS (
    SELECT
        b.ticker,
        b.date,
        b.close,
        b.rn,
        e12.ema_12,
        e26.ema_26,
        (e12.ema_12 - e26.ema_26) as macd_line
    FROM base_data b
    LEFT JOIN ema_12 e12 ON b.ticker = e12.ticker AND b.date = e12.date
    LEFT JOIN ema_26 e26 ON b.ticker = e26.ticker AND b.date = e26.date
    WHERE e12.ema_12 IS NOT NULL
    AND e26.ema_26 IS NOT NULL
),

-- Calculate Signal Line (9-day EMA of MACD)
-- Timeline explanation:
-- Day 1-26: Building up to first MACD value (need 26-day EMA)
-- Day 26-34: Building up 9 MACD values to calculate first signal line
-- Day 34 onwards: Full MACD with signal line available
signal_line AS (
    -- Initial value using 9-day SMA of MACD
    SELECT
        ticker,
        date,
        close,
        rn,
        macd_line,
        AVG(macd_line) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) as signal_line
    FROM macd_line_calc
    WHERE rn = 34  -- First signal line value (26 days for MACD + 8 additional days for 9-day SMA)

    UNION ALL

    SELECT
        m.ticker,
        m.date,
        m.close,
        m.rn,
        m.macd_line,
        ((m.macd_line - s.signal_line) * (2.0/10.0)) + s.signal_line as signal_line
    FROM macd_line_calc m
    JOIN signal_line s ON m.ticker = s.ticker AND m.rn = s.rn + 1
)

-- Final selection with all MACD components
SELECT
    ticker,
    date,
    close,
    macd_line,
    signal_line,
    (macd_line - signal_line) as macd_histogram,
    LAG(macd_line) OVER (PARTITION BY ticker ORDER BY date) as prev_macd_line,
    LAG(signal_line) OVER (PARTITION BY ticker ORDER BY date) as prev_signal_line
FROM signal_line
ORDER BY ticker, date