WITH volume_metrics AS (
    SELECT
        ticker,
        f.value:date::date as trade_date,
        f.value:volume::float as volume,
        f.value:close::float as close,
        f.value:volume::float * f.value:close::float as daily_traded_value
    FROM {{ ref('int_cumulated_daily_aggregation') }},
    LATERAL FLATTEN(input => composite_col) f
    WHERE f.value:date::date >= DATEADD(month, -5, '{{ get_max_date('int_daily_metrics') }}') -- Last 5 months for recent volume
),

liquidity_metrics AS (
    SELECT
        ticker,
        AVG(volume) as avg_daily_volume,
        AVG(daily_traded_value) as avg_daily_traded_value,
        MIN(daily_traded_value) as min_daily_traded_value,
        MAX(daily_traded_value) as max_daily_traded_value
    FROM volume_metrics
    GROUP BY ticker
),

market_cap AS (
    SELECT
        *
    from
    {{ ref('dim_ticker_enrichment') }}
),

combined_metrics AS (
    SELECT
        l.ticker,
        l.avg_daily_volume,
        l.avg_daily_traded_value,
        market_cap,
        name,
        office,
        industry,
        BRANDING_ICON_URL,
        BRANDING_LOGO_URL,
        PRIMARY_EXCHANGE,

        -- Categorize market cap
        CASE
            WHEN market_cap >= 200000000000 THEN 'Mega Cap'
            WHEN market_cap >= 10000000000 THEN 'Large Cap'
            WHEN market_cap >= 2000000000 THEN 'Mid Cap'
            WHEN market_cap >= 300000000 THEN 'Small Cap'
            ELSE 'Micro Cap'
        END as market_cap_category,
        -- Liquidity score (based on trading value)
        CASE
            WHEN l.avg_daily_traded_value >= 20000000 THEN 5  -- Highly liquid
            WHEN l.avg_daily_traded_value >= 5000000 THEN 4
            WHEN l.avg_daily_traded_value >= 1000000 THEN 3
            WHEN l.avg_daily_traded_value >= 100000 THEN 2
            ELSE 1  -- Low liquidity
        END as liquidity_score
    FROM liquidity_metrics l
    LEFT JOIN market_cap m ON l.ticker = m.ticker
),

final_rankings AS (
    SELECT
        c.*,
        -- Rank within market cap category
        RANK() OVER (
            PARTITION BY market_cap_category
            ORDER BY avg_daily_traded_value DESC
        ) as liquidity_rank_in_category,

        -- Overall ranking considering both market cap and liquidity
        RANK() OVER (
            ORDER BY
                market_cap * 0.6 +  -- 60% weight to market cap
                avg_daily_traded_value * 0.4  -- 40% weight to liquidity
            DESC
        ) as overall_rank
    FROM combined_metrics c
)

SELECT
    ticker,
    name,
    overall_rank,
    market_cap_category,
    ROUND(market_cap/1000000000, 2) as market_cap_billions,
    ROUND(avg_daily_volume, 0) as avg_daily_volume,
    ROUND(avg_daily_traded_value/1000000, 2) as avg_daily_traded_value_millions,
    liquidity_score,
    liquidity_rank_in_category,
    office,
    industry,
    BRANDING_ICON_URL,
    BRANDING_LOGO_URL,
    PRIMARY_EXCHANGE

FROM final_rankings
WHERE
    -- Filter criteria for reliable stocks
    liquidity_score >= 3  -- Minimum liquidity
    AND market_cap >= 300000000  -- Minimum market cap (300M)
    AND avg_daily_volume > 100000  -- Minimum daily volume
ORDER BY overall_rank