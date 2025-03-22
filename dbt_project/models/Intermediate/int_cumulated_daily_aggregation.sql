{{
    config(
        materialized='table',
        unique_key=['ticker', 'year', 'month_num'],
        cluster_by = ['ticker','year', 'month_num']
    )
}}

with existing_data AS (
    SELECT
        ticker,
        date,
        DATE_PART('year', date) AS year,
        DATE_PART('month', date) AS month_num,
        OBJECT_CONSTRUCT(
            'open', open,
            'close', close,
            'high', high,
            'low', low,
            'volume', volume,
            'transactions', transactions,
            'date', TO_VARCHAR(date, 'YYYY-MM-DD')
        ) AS daily_data
    FROM {{ ref('int_daily_metrics') }}
),
final_data AS (
    SELECT
        ticker,
        year,
        month_num,
        ARRAY_DISTINCT(ARRAY_AGG(daily_data) WITHIN GROUP (ORDER BY daily_data:date)) AS composite_col,
        COUNT(*) as record_count,
        MIN(daily_data:date::date) as first_date,
        MAX(daily_data:date::date) as last_date,
        CURRENT_TIMESTAMP() as processed_at
    FROM existing_data
    GROUP BY ticker, year, month_num
)
Select * from final_data