{% test check_severity_errors(model) %}

with validation as (
    Select
      ticker,
      timestamp,
      sma_10_is_critical,
      sma_20_is_critical,
      sma_50_is_critical,
      sma_100_is_critical,
      sma_200_is_critical
    from {{ model }}
    where timestamp = dateadd(day, -1, (select max(timestamp) from {{ model }}))
),
validation_errors as (
    select
        *
    from validation
    where sma_10_is_critical = True
      or sma_20_is_critical = True
      or sma_50_is_critical = True
      or sma_100_is_critical = True
      or sma_200_is_critical = True
)
select * from validation_errors

{% endtest %}