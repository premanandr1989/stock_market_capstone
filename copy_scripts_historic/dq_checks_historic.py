from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time, os
from configparser import ConfigParser

# Get the path to AWS credentials file
aws_credentials_path = os.path.expanduser('~/.aws/credentials')

# Read the credentials
config = ConfigParser()
config.read(aws_credentials_path)

# Access polygon profile credentials
polygon_access_key = config['polygon']['aws_access_key_id']
polygon_secret_key = config['polygon']['aws_secret_access_key']

def check_ticker_data_quality(df):
    """
    Perform domain-specific DQ checks for daily stock ticker aggregations

    Parameters:
    df (pyspark.sql.DataFrame): Input Spark dataframe with ticker data

    Returns:
    dict: Dictionary containing DQ metrics and issues found
    """
    start_time = time.time()
    results = {}

    # Cache the dataframe as we'll be doing multiple operations
    df.cache()

    # 1. Volume and transaction checks
    volume_stats = df.select(
        F.count('*').alias('total_records'),
        F.sum(F.when(F.col('volume') == 0, 1).otherwise(0)).alias('invalid_volume'),
        F.sum(F.when(F.col('transactions') == 0, 1).otherwise(0)).alias('invalid_transactions'),
        F.sum(F.when(F.col('volume') < F.col('transactions'), 1).otherwise(0)).alias('volume_less_than_transactions')
    ).collect()[0]

    results['volume_checks'] = {
        'total_records': volume_stats['total_records'],
        'records_with_invalid_volume': volume_stats['invalid_volume'],
        'records_with_invalid_transactions': volume_stats['invalid_transactions'],
        'volume_less_than_transactions': volume_stats['volume_less_than_transactions']
    }

    # 2. Price consistency checks
    price_checks = df.select(
        F.sum(F.when(F.col('high') < F.col('low'), 1).otherwise(0)).alias('high_less_than_low'),
        F.sum(F.when(F.col('open') > F.col('high'), 1).otherwise(0)).alias('open_above_high'),
        F.sum(F.when(F.col('open') < F.col('low'), 1).otherwise(0)).alias('open_below_low'),
        F.sum(F.when(F.col('close') > F.col('high'), 1).otherwise(0)).alias('close_above_high'),
        F.sum(F.when(F.col('close') < F.col('low'), 1).otherwise(0)).alias('close_below_low'),
        F.sum(F.when((F.col('open') <= 0) | (F.col('close') <= 0) |
                     (F.col('high') <= 0) | (F.col('low') <= 0), 1).otherwise(0)).alias('negative_prices')
    ).collect()[0]

    results['price_consistency'] = {k: v for k, v in price_checks.asDict().items()}

    # 3. Timestamp checks
    window_spec = Window.partitionBy('ticker').orderBy('date')

    timestamp_checks = df.withColumn('prev_timestamp',
                                     F.lag('date').over(window_spec)) \
        .select(
        F.sum(F.when(F.col('date').isNull(), 1).otherwise(0)).alias('null_timestamps'),
        F.sum(F.when(F.col('date') < F.col('prev_timestamp'), 1).otherwise(0)).alias('out_of_order_timestamps')
    ).collect()[0]

    results['timestamp_checks'] = {k: v for k, v in timestamp_checks.asDict().items()}

    # 4. Ticker symbol checks
    ticker_stats = df.groupBy('ticker').count() \
        .select(
        F.count('*').alias('unique_tickers'),
        F.sum(F.when(F.col('count') < 0.9 * volume_stats['total_records'], 1).otherwise(0)).alias('incomplete_tickers')
    ).collect()[0]

    results['ticker_stats'] = {k: v for k, v in ticker_stats.asDict().items()}

    # 5. Statistical outliers (using 3-sigma rule)
    for col in ['volume', 'open', 'close', 'high', 'low']:
        stats = df.select(
            F.mean(col).alias('mean'),
            F.stddev(col).alias('stddev')
        ).collect()[0]

        outliers = df.select(
            F.sum(F.when(
                (F.col(col) > stats['mean'] + 3 * stats['stddev']) |
                (F.col(col) < stats['mean'] - 3 * stats['stddev']),
                1).otherwise(0)
                  ).alias(f'{col}_outliers')
        ).collect()[0]

        if 'outliers' not in results:
            results['outliers'] = {}
        results['outliers'][col] = outliers[f'{col}_outliers']

    # 6. Missing data pattern
    null_patterns = df.select([
        (F.count(F.when(F.col(c).isNull(), c)) / F.count('*') * 100).alias(f"{c}_null_percentage")
        for c in df.columns
    ]).collect()[0]

    results['null_percentages'] = {k: float(v) for k, v in null_patterns.asDict().items()}

    # 7. Performance metrics
    results['execution_time'] = time.time() - start_time

    # Uncache the dataframe
    df.unpersist()

    return results


def generate_dq_report(results):
    """
    Generate a readable report from DQ check results
    """
    print("\nStock Ticker Data Quality Report")
    print("=" * 50)

    print("\n1. Volume and Transaction Checks:")
    print(f"Total Records: {results['volume_checks']['total_records']:,}")
    print(f"Invalid Volume Records: {results['volume_checks']['records_with_invalid_volume']:,}")
    print(f"Invalid Transaction Records: {results['volume_checks']['records_with_invalid_transactions']:,}")
    print(f"Volume < Transactions: {results['volume_checks']['volume_less_than_transactions']:,}")

    print("\n2. Price Consistency Issues:")
    for check, count in results['price_consistency'].items():
        print(f"{check}: {count:,}")

    print("\n3. Timestamp Issues:")
    print(f"Null Timestamps: {results['timestamp_checks']['null_timestamps']:,}")
    print(f"Out of Order Timestamps: {results['timestamp_checks']['out_of_order_timestamps']:,}")

    print("\n4. Ticker Statistics:")
    print(f"Unique Tickers: {results['ticker_stats']['unique_tickers']:,}")
    print(f"Incomplete Ticker Series: {results['ticker_stats']['incomplete_tickers']:,}")

    print("\n5. Null percentages:")
    print(f"{results['null_percentages']}")

    print("\n6. Statistical Outliers:")
    for field, count in results['outliers'].items():
        print(f"{field}: {count:,} outliers")

    print(f"\nExecution Time: {results['execution_time']:.2f} seconds")

# if __name__ == '__main__':
#     df = create_polygon_dataframe()
#     results = check_ticker_data_quality(df)
#     generate_dq_report(results)
