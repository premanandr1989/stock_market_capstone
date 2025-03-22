import os
import requests
import snowflake.connector
import time
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from snowflake_config_handler import SnowflakeTableHandler
import yaml
from pathlib import Path

# AWS credentials path
aws_credentials_path = os.path.expanduser('~/.aws/credentials')

# Read the credentials
config = ConfigParser()
config.read(aws_credentials_path)

# Access polygon profile credentials
polygon_access_key = config['polygon']['aws_access_key_id']
polygon_secret_key = config['polygon']['aws_secret_access_key']

# Base URL for API
BASE_URL = 'https://api.polygon.io/v1/indicators/ema'


def fetch_ema(ticker, window, limit=5000):
    """
    Fetch ema data from the Polygon API endpoint.

    Args:
        ticker (str): Stock ticker symbol
        window (int): ema window size
        limit (int): Maximum number of results to return

    Returns:
        list: List of ema data points
    """
    all_ema_data = []
    count = 0

    params = {
        "limit": limit,
        "apiKey": polygon_secret_key,
        "timestamp.gte": '2025-02-03',
        "timespan": 'day',
        "window": window,
        "series_type": 'close',
        "adjusted": 'true'
    }

    url = f"{BASE_URL}/{ticker}"

    try:
        response = requests.get(url, params=params)
        data = response.json()

        print(f"Fetching ema{window} for {ticker}...")

        # Handle the nested structure in the API response
        if "results" in data and isinstance(data["results"], dict) and "values" in data["results"]:
            all_ema_data = data["results"]["values"]
            print(f"Found {len(all_ema_data)} results for {ticker} with window {window}")
        else:
            print(f"No results found for {ticker} with window {window}")
            print(f"Response: {data}")
            return []

    except Exception as e:
        print(f"Error in API request for {ticker} with window {window}: {str(e)}")
        return []

    print(f"Completed fetching ema{window} for {ticker}, returning {len(all_ema_data)} data points")
    return all_ema_data


def create_ema_table_if_not_exists(cursor):
    """
    Create ema table if it doesn't exist with specific columns for each ema window
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS mart_ema_polygon (
        ticker VARCHAR(20),
        timestamp DATE,
        ema_10 FLOAT,
        ema_20 FLOAT,
        ema_50 FLOAT,
        ema_100 FLOAT,
        ema_200 FLOAT,
        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (ticker, timestamp)
    )
    """
    cursor.execute(create_table_sql)
    print("Table mart_ema_polygon created or already exists")


def create_ema_errors_table_if_not_exists(cursor):
    """
    Create ema errors table if it doesn't exist with specific columns for each ema window
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS mart_ema_errors (
        ticker VARCHAR(20),
        timestamp DATE,
        ema_10_polygon FLOAT,
        ema_10_calculated FLOAT,
        ema_10_error_percent FLOAT,
        ema_10_is_critical BOOLEAN,
        ema_20_polygon FLOAT,
        ema_20_calculated FLOAT,
        ema_20_error_percent FLOAT,
        ema_20_is_critical BOOLEAN,
        ema_50_polygon FLOAT,
        ema_50_calculated FLOAT,
        ema_50_error_percent FLOAT,
        ema_50_is_critical BOOLEAN,
        ema_100_polygon FLOAT,
        ema_100_calculated FLOAT,
        ema_100_error_percent FLOAT,
        ema_100_is_critical BOOLEAN,
        ema_200_polygon FLOAT,
        ema_200_calculated FLOAT,
        ema_200_error_percent FLOAT,
        ema_200_is_critical BOOLEAN,
        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (ticker, timestamp)
    )
    """
    cursor.execute(create_table_sql)
    print("Table mart_ema_errors created or already exists")


def load_ema_to_snowflake(cursor, ema_data_by_window, ticker):
    """
    Load ema data to Snowflake table with each window as a separate column

    Args:
        cursor: Snowflake cursor
        ema_data_by_window: Dictionary with window sizes as keys and ema data as values
        ticker: Stock ticker symbol
    """
    # First, organize data by timestamp to combine different windows
    data_by_timestamp = {}

    for window, ema_data in ema_data_by_window.items():
        if not ema_data:
            print(f"No ema data to load for {ticker} with window {window}")
            continue

        for item in ema_data:
            # Convert timestamp from milliseconds to date only (no time component)
            if 'timestamp' in item and item['timestamp']:
                # Convert from milliseconds to seconds
                ts_seconds = item['timestamp'] / 1000
                # Convert to datetime and extract only the date part
                dt_object = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
                timestamp = dt_object.strftime('%Y-%m-%d')
            else:
                continue

            ema_value = item.get('value')

            if timestamp and ema_value is not None:
                if timestamp not in data_by_timestamp:
                    data_by_timestamp[timestamp] = {
                        'ticker': ticker,
                        'timestamp': timestamp,
                        'ema_10': None,
                        'ema_20': None,
                        'ema_50': None,
                        'ema_100': None,
                        'ema_200': None
                    }

                data_by_timestamp[timestamp][f'ema_{window}'] = ema_value

    # Convert to list of records
    records = list(data_by_timestamp.values())

    if not records:
        print(f"No consolidated ema records to load for {ticker}")
        return 0

    # Insert records one by one, deleting existing ones first
    total_loaded = 0
    for record in records:
        try:
            # Delete existing record for this ticker and timestamp
            cursor.execute(
                "DELETE FROM mart_ema_polygon WHERE ticker = %s AND timestamp = %s",
                (record['ticker'], record['timestamp'])
            )

            # Insert the new record
            insert_sql = """
            INSERT INTO mart_ema_polygon (
                ticker, timestamp, ema_10, ema_20, ema_50, ema_100, ema_200, ingestion_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()
            )
            """

            cursor.execute(insert_sql, (
                record['ticker'],
                record['timestamp'],
                record['ema_10'],
                record['ema_20'],
                record['ema_50'],
                record['ema_100'],
                record['ema_200']
            ))

            total_loaded += 1

            if total_loaded % 100 == 0:
                print(f"Processed {total_loaded} records for {ticker}...")
        except Exception as e:
            print(f"Error processing record: {e}")
            continue

    print(f"Loaded {total_loaded} consolidated ema records for {ticker} to Snowflake")
    return total_loaded


def analyze_ema_errors(cursor, ticker):
    """
    Compare all ema values from Polygon with existing mart_ema_calculated table
    and identify critical errors for each window
    """
    # Get all timestamps for this ticker in the polygon data
    cursor.execute("SELECT DISTINCT timestamp FROM mart_ema_polygon WHERE ticker = %s", (ticker,))
    timestamps = [row[0] for row in cursor.fetchall()]

    total_processed = 0

    # Process each timestamp individually
    for timestamp in timestamps:
        try:
            # Delete existing error data for this specific ticker and timestamp
            cursor.execute("DELETE FROM mart_ema_errors WHERE ticker = %s AND timestamp = %s", (ticker, timestamp))

            # Insert error analysis for this specific timestamp
            query = """
            INSERT INTO mart_ema_errors (
                ticker, timestamp, 
                ema_10_polygon, ema_10_calculated, ema_10_error_percent, ema_10_is_critical,
                ema_20_polygon, ema_20_calculated, ema_20_error_percent, ema_20_is_critical,
                ema_50_polygon, ema_50_calculated, ema_50_error_percent, ema_50_is_critical,
                ema_100_polygon, ema_100_calculated, ema_100_error_percent, ema_100_is_critical,
                ema_200_polygon, ema_200_calculated, ema_200_error_percent, ema_200_is_critical,
                ingestion_timestamp
            )
            SELECT 
                p.ticker,
                p.timestamp,
                p.ema_10 AS ema_10_polygon,
                f10.ema_10 AS ema_10_calculated,
                ABS((p.ema_10 - f10.ema_10) / NULLIF(f10.ema_10, 0)) * 100 AS ema_10_error_percent,
                CASE WHEN ABS((p.ema_10 - f10.ema_10) / NULLIF(f10.ema_10, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS ema_10_is_critical,

                p.ema_20 AS ema_20_polygon,
                f20.ema_20 AS ema_20_calculated,
                ABS((p.ema_20 - f20.ema_20) / NULLIF(f20.ema_20, 0)) * 100 AS ema_20_error_percent,
                CASE WHEN ABS((p.ema_20 - f20.ema_20) / NULLIF(f20.ema_20, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS ema_20_is_critical,

                p.ema_50 AS ema_50_polygon,
                f50.ema_50 AS ema_50_calculated,
                ABS((p.ema_50 - f50.ema_50) / NULLIF(f50.ema_50, 0)) * 100 AS ema_50_error_percent,
                CASE WHEN ABS((p.ema_50 - f50.ema_50) / NULLIF(f50.ema_50, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS ema_50_is_critical,

                p.ema_100 AS ema_100_polygon,
                f100.ema_100 AS ema_100_calculated,
                ABS((p.ema_100 - f100.ema_100) / NULLIF(f100.ema_100, 0)) * 100 AS ema_100_error_percent,
                CASE WHEN ABS((p.ema_100 - f100.ema_100) / NULLIF(f100.ema_100, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS ema_100_is_critical,

                p.ema_200 AS ema_200_polygon,
                f200.ema_200 AS ema_200_calculated,
                ABS((p.ema_200 - f200.ema_200) / NULLIF(f200.ema_200, 0)) * 100 AS ema_200_error_percent,
                CASE WHEN ABS((p.ema_200 - f200.ema_200) / NULLIF(f200.ema_200, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS ema_200_is_critical,

                CURRENT_TIMESTAMP()
            FROM 
                mart_ema_polygon p
            LEFT JOIN 
                mart_ema_flexible f10
            ON 
                p.ticker = f10.ticker
                AND p.timestamp = f10.date
            LEFT JOIN 
                mart_ema_flexible f20
            ON 
                p.ticker = f20.ticker
                AND p.timestamp = f20.date
            LEFT JOIN 
                mart_ema_flexible f50
            ON 
                p.ticker = f50.ticker
                AND p.timestamp = f50.date
            LEFT JOIN 
                mart_ema_flexible f100
            ON 
                p.ticker = f100.ticker
                AND p.timestamp = f100.date
            LEFT JOIN 
                mart_ema_flexible f200
            ON 
                p.ticker = f200.ticker
                AND p.timestamp = f200.date
            WHERE 
                p.ticker = %s
                AND p.timestamp = %s
            """

            cursor.execute(query, (ticker, timestamp))
            total_processed += 1

            if total_processed % 100 == 0:
                print(f"Analyzed errors for {total_processed} timestamps for {ticker}")
        except Exception as e:
            print(f"Error analyzing timestamp {timestamp} for {ticker}: {str(e)}")
            continue

    print(f"Completed error analysis for {total_processed} timestamps for {ticker}")

    # Get summary of critical errors for each window
    for window in [10, 20, 50, 100, 200]:
        cursor.execute(f"""
        SELECT COUNT(*) FROM mart_ema_errors 
        WHERE ticker = %s AND ema_{window}_is_critical = TRUE
        """, (ticker,))

        critical_count = cursor.fetchone()[0]
        print(f"Found {critical_count} critical errors for {ticker} with ema {window}")


def main():
    window_list = [10, 20, 50, 100, 200]

    current_dir = Path(__file__).parent
    dbt_project_path = current_dir.parent / "dbt_capstone"

    load_handler = SnowflakeTableHandler(
        project_dir=dbt_project_path,
        profile_name="jaffle_shop"
    )

    if not load_handler.conn:
        load_handler.connect()
    cursor = load_handler.conn.cursor()

    # Create tables if they don't exist
    create_ema_table_if_not_exists(cursor)
    create_ema_errors_table_if_not_exists(cursor)

    # Get ticker list
    cursor.execute(
        "SELECT ticker FROM dim_ticker_classifier WHERE MARKET_CAP_CATEGORY = 'Mega Cap' AND overall_rank <= 7645")
    ticker_list = [row[0] for row in cursor.fetchall()]

    print(f"Processing {len(ticker_list)} tickers")

    # Process each ticker
    for ticker in ticker_list:
        try:
            # Dictionary to hold ema data for each window
            ema_data_by_window = {}

            # Fetch ema data for each window
            for window in window_list:
                try:
                    ema_data = fetch_ema(ticker=ticker, window=window)
                    ema_data_by_window[window] = ema_data

                    if not ema_data:
                        print(f"No ema data fetched for {ticker} with window {window}")

                except Exception as e:
                    print(f"Error fetching ema {window} for {ticker}: {str(e)}")
                    ema_data_by_window[window] = []

            # Load consolidated data to Snowflake
            records_loaded = load_ema_to_snowflake(cursor, ema_data_by_window, ticker)

            # Only analyze errors if we loaded data
            if records_loaded > 0:
                # Analyze errors (now handles all windows at once)
                analyze_ema_errors(cursor, ticker)

            # Commit after each ticker to avoid losing all data if an error occurs
            load_handler.conn.commit()

        except Exception as e:
            print(f"Error processing ticker {ticker}: {str(e)}")
            # Continue with next ticker
            continue

    # Close connection
    cursor.close()

    print("ema data processing completed")


if __name__ == '__main__':
    main()