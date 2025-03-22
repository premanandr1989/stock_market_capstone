{{
    config(
        materialized='table',
        cluster_by = ['ticker','date'],
        unique_key=['ticker', 'date'],
        incremental_strategy = 'append'
    )
}}

SELECT
    ticker,
    date,
    gradingcompany,
    previousgrade,
    newgrade,
    action,
    CASE
        WHEN LOWER(newgrade) LIKE '%buy%' OR
             LOWER(newgrade) LIKE '%strong%' OR
             LOWER(newgrade) LIKE '%outperform%' OR
             LOWER(newgrade) LIKE '%overweight%' OR
             LOWER(newgrade) LIKE '%positive%' OR
             LOWER(newgrade) LIKE '%long term buy%' OR
             LOWER(newgrade) LIKE '%accumulate%' OR
             LOWER(newgrade) LIKE '%top pick%' OR
             LOWER(newgrade) LIKE '%conviction buy%' OR
             LOWER(action) LIKE '%upgrade%'
        THEN 'BUY'
        WHEN LOWER(newgrade) LIKE '%hold%' OR
             LOWER(newgrade) LIKE '%neutral%' OR
             LOWER(newgrade) LIKE '%equal weight%' OR
             LOWER(newgrade) LIKE '%market perform%' OR
             LOWER(newgrade) LIKE '%perform%' OR
             LOWER(newgrade) LIKE '%in line%' OR
             LOWER(newgrade) LIKE '%peer perform%' OR
             LOWER(newgrade) LIKE '%sector weight%' OR
             LOWER(newgrade) LIKE '%sector perform%' OR
             LOWER(newgrade) LIKE '%mixed%' OR
             LOWER(newgrade) LIKE '%average%'
        THEN 'HOLD'
        WHEN LOWER(newgrade) LIKE '%sell%' OR
             LOWER(newgrade) LIKE '%underperform%' OR
             LOWER(newgrade) LIKE '%underweight%' OR
             LOWER(newgrade) LIKE '%reduce%' OR
             LOWER(newgrade) LIKE '%sector underperform%' OR
             LOWER(newgrade) LIKE '%negative%' OR
             LOWER(newgrade) LIKE '%below average%' OR
             LOWER(action) LIKE '%downgrade%'
        THEN 'SELL'
    END as sentiment
FROM {{ ref('stg_stock_graders') }}
{% if is_incremental() %}
    WHERE date > (SELECT max(date) FROM {{ this }})
{% endif %}