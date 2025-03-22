{{
    config(
        materialized='incremental',
        unique_key=['ticker', 'date'],
        cluster_by = ['ticker','date'],
        incremental_strategy = 'append'
    )
}}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_stock_daily_agg') }}
)

{% if var('apply_split_adjustment') %}
    -- For historical data that needs adjustment
    ,cum_factors AS (
              SELECT
                d.ticker,
                d.date,
                COALESCE(
                  PRODUCT_AGG(ARRAY_AGG(s.split_to/s.split_from)),
                  1
                ) AS cum_factor
              FROM {{ ref('stg_stock_daily_agg') }} d
              LEFT JOIN {{ ref('stg_stock_ticker_splits') }} s
                ON d.ticker = s.ticker
               AND s.date > d.date
              GROUP BY d.ticker, d.date
            )

            SELECT
              d.ticker,
              d.date,
              d.volume * cf.cum_factor   AS volume,
              d.open   / cf.cum_factor   AS open,
              d.high   / cf.cum_factor   AS high,
              d.low    / cf.cum_factor   AS low,
              d.close  / cf.cum_factor   AS close,
              d.transactions
            FROM {{ ref('stg_stock_daily_agg') }} d
            JOIN cum_factors cf
              ON d.ticker = cf.ticker
             AND d.date   = cf.date


{% else %}
    -- For API data that's already adjusted
    SELECT
        *
    FROM base_data
{% endif %}

{% if is_incremental() %}
    WHERE date > (SELECT max(date) FROM {{ this }})
{% endif %}