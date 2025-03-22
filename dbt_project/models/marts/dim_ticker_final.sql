WITH polygon_news as (
    Select
          ticker,
          sentiment,
          sentiment_reasoning,
          date,
          row_number() over(partition by ticker order by date desc) as rn
    from {{ ref ('stg_polygon_news') }} pn
),
ticker_enriched as (
    Select
        te.*,
        pn.sentiment,
        rn
    from {{ ref ('dim_ticker_enrichment') }} te
    left join polygon_news pn on te.ticker = pn.ticker
    where pn.rn in (1,2)
),
pivoted_data as (
    SELECT ticker,
            name,
            active,
            sic_code,
            sic_description,
            office,
            industry,
            market_cap,
            branding_icon_url,
            branding_logo_url,
            SHARE_CLASS_SHARES_OUTSTANDING,
            WEIGHTED_SHARES_OUTSTANDING,
            PRIMARY_EXCHANGE,
            TOTAL_EMPLOYEES,
            "1" as current_sentiment,
            "2" as previous_sentiment
    FROM (
        SELECT
            *
        FROM ticker_enriched
    )
    PIVOT (
        MAX(SENTIMENT)      -- aggregation function (can be COUNT, AVG, etc.)
        FOR rn IN (1,2)  -- list your categories
    )
)
Select
    p.*,
    pn.date,
    pn.sentiment_reasoning as current_reasoning
from pivoted_data p
left join polygon_news pn on p.ticker = pn.ticker
where rn=1