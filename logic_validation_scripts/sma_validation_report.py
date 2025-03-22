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
BASE_URL = 'https://api.polygon.io/v1/indicators/sma'


def fetch_sma(ticker, window, limit=5000):
    """
    Fetch SMA data from the Polygon API endpoint.

    Args:
        ticker (str): Stock ticker symbol
        window (int): SMA window size
        limit (int): Maximum number of results to return

    Returns:
        list: List of SMA data points
    """
    all_sma_data = []
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

        print(f"Fetching SMA{window} for {ticker}...")

        # Handle the nested structure in the API response
        if "results" in data and isinstance(data["results"], dict) and "values" in data["results"]:
            all_sma_data = data["results"]["values"]
            print(f"Found {len(all_sma_data)} results for {ticker} with window {window}")
        else:
            print(f"No results found for {ticker} with window {window}")
            print(f"Response: {data}")
            return []

    except Exception as e:
        print(f"Error in API request for {ticker} with window {window}: {str(e)}")
        return []

    print(f"Completed fetching SMA{window} for {ticker}, returning {len(all_sma_data)} data points")
    return all_sma_data


def create_sma_table_if_not_exists(cursor):
    """
    Create SMA table if it doesn't exist with specific columns for each SMA window
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS mart_sma_polygon (
        ticker VARCHAR(20),
        timestamp DATE,
        sma_10 FLOAT,
        sma_20 FLOAT,
        sma_50 FLOAT,
        sma_100 FLOAT,
        sma_200 FLOAT,
        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (ticker, timestamp)
    )
    """
    cursor.execute(create_table_sql)
    print("Table mart_sma_polygon created or already exists")


def create_sma_errors_table_if_not_exists(cursor):
    """
    Create SMA errors table if it doesn't exist with specific columns for each SMA window
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS mart_sma_errors (
        ticker VARCHAR(20),
        timestamp DATE,
        sma_10_polygon FLOAT,
        sma_10_calculated FLOAT,
        sma_10_error_percent FLOAT,
        sma_10_is_critical BOOLEAN,
        sma_20_polygon FLOAT,
        sma_20_calculated FLOAT,
        sma_20_error_percent FLOAT,
        sma_20_is_critical BOOLEAN,
        sma_50_polygon FLOAT,
        sma_50_calculated FLOAT,
        sma_50_error_percent FLOAT,
        sma_50_is_critical BOOLEAN,
        sma_100_polygon FLOAT,
        sma_100_calculated FLOAT,
        sma_100_error_percent FLOAT,
        sma_100_is_critical BOOLEAN,
        sma_200_polygon FLOAT,
        sma_200_calculated FLOAT,
        sma_200_error_percent FLOAT,
        sma_200_is_critical BOOLEAN,
        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (ticker, timestamp)
    )
    """
    cursor.execute(create_table_sql)
    print("Table mart_sma_errors created or already exists")


def load_sma_to_snowflake(cursor, sma_data_by_window, ticker):
    """
    Load SMA data to Snowflake table with each window as a separate column

    Args:
        cursor: Snowflake cursor
        sma_data_by_window: Dictionary with window sizes as keys and SMA data as values
        ticker: Stock ticker symbol
    """
    # First, organize data by timestamp to combine different windows
    data_by_timestamp = {}

    for window, sma_data in sma_data_by_window.items():
        if not sma_data:
            print(f"No SMA data to load for {ticker} with window {window}")
            continue

        for item in sma_data:
            # Convert timestamp from milliseconds to date only (no time component)
            if 'timestamp' in item and item['timestamp']:
                # Convert from milliseconds to seconds
                ts_seconds = item['timestamp'] / 1000
                # Convert to datetime and extract only the date part
                dt_object = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
                timestamp = dt_object.strftime('%Y-%m-%d')
            else:
                continue

            sma_value = item.get('value')

            if timestamp and sma_value is not None:
                if timestamp not in data_by_timestamp:
                    data_by_timestamp[timestamp] = {
                        'ticker': ticker,
                        'timestamp': timestamp,
                        'sma_10': None,
                        'sma_20': None,
                        'sma_50': None,
                        'sma_100': None,
                        'sma_200': None
                    }

                data_by_timestamp[timestamp][f'sma_{window}'] = sma_value

    # Convert to list of records
    records = list(data_by_timestamp.values())

    if not records:
        print(f"No consolidated SMA records to load for {ticker}")
        return 0

    # Insert records one by one, deleting existing ones first
    total_loaded = 0
    for record in records:
        try:
            # Delete existing record for this ticker and timestamp
            cursor.execute(
                "DELETE FROM mart_sma_polygon WHERE ticker = %s AND timestamp = %s",
                (record['ticker'], record['timestamp'])
            )

            # Insert the new record
            insert_sql = """
            INSERT INTO mart_sma_polygon (
                ticker, timestamp, sma_10, sma_20, sma_50, sma_100, sma_200, ingestion_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()
            )
            """

            cursor.execute(insert_sql, (
                record['ticker'],
                record['timestamp'],
                record['sma_10'],
                record['sma_20'],
                record['sma_50'],
                record['sma_100'],
                record['sma_200']
            ))

            total_loaded += 1

            if total_loaded % 100 == 0:
                print(f"Processed {total_loaded} records for {ticker}...")
        except Exception as e:
            print(f"Error processing record: {e}")
            continue

    print(f"Loaded {total_loaded} consolidated SMA records for {ticker} to Snowflake")
    return total_loaded


def analyze_sma_errors(cursor, ticker):
    """
    Compare all SMA values from Polygon with existing mart_sma_calculated table
    and identify critical errors for each window
    """
    # Get all timestamps for this ticker in the polygon data
    cursor.execute("SELECT DISTINCT timestamp FROM mart_sma_polygon WHERE ticker = %s", (ticker,))
    timestamps = [row[0] for row in cursor.fetchall()]

    total_processed = 0

    # Process each timestamp individually
    for timestamp in timestamps:
        try:
            # Delete existing error data for this specific ticker and timestamp
            cursor.execute("DELETE FROM mart_sma_errors WHERE ticker = %s AND timestamp = %s", (ticker, timestamp))

            # Insert error analysis for this specific timestamp
            query = """
            INSERT INTO mart_sma_errors (
                ticker, timestamp, 
                sma_10_polygon, sma_10_calculated, sma_10_error_percent, sma_10_is_critical,
                sma_20_polygon, sma_20_calculated, sma_20_error_percent, sma_20_is_critical,
                sma_50_polygon, sma_50_calculated, sma_50_error_percent, sma_50_is_critical,
                sma_100_polygon, sma_100_calculated, sma_100_error_percent, sma_100_is_critical,
                sma_200_polygon, sma_200_calculated, sma_200_error_percent, sma_200_is_critical,
                ingestion_timestamp
            )
            SELECT 
                p.ticker,
                p.timestamp,
                p.sma_10 AS sma_10_polygon,
                f10.sma_10 AS sma_10_calculated,
                ABS((p.sma_10 - f10.sma_10) / NULLIF(f10.sma_10, 0)) * 100 AS sma_10_error_percent,
                CASE WHEN ABS((p.sma_10 - f10.sma_10) / NULLIF(f10.sma_10, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS sma_10_is_critical,

                p.sma_20 AS sma_20_polygon,
                f20.sma_20 AS sma_20_calculated,
                ABS((p.sma_20 - f20.sma_20) / NULLIF(f20.sma_20, 0)) * 100 AS sma_20_error_percent,
                CASE WHEN ABS((p.sma_20 - f20.sma_20) / NULLIF(f20.sma_20, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS sma_20_is_critical,

                p.sma_50 AS sma_50_polygon,
                f50.sma_50 AS sma_50_calculated,
                ABS((p.sma_50 - f50.sma_50) / NULLIF(f50.sma_50, 0)) * 100 AS sma_50_error_percent,
                CASE WHEN ABS((p.sma_50 - f50.sma_50) / NULLIF(f50.sma_50, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS sma_50_is_critical,

                p.sma_100 AS sma_100_polygon,
                f100.sma_100 AS sma_100_calculated,
                ABS((p.sma_100 - f100.sma_100) / NULLIF(f100.sma_100, 0)) * 100 AS sma_100_error_percent,
                CASE WHEN ABS((p.sma_100 - f100.sma_100) / NULLIF(f100.sma_100, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS sma_100_is_critical,

                p.sma_200 AS sma_200_polygon,
                f200.sma_200 AS sma_200_calculated,
                ABS((p.sma_200 - f200.sma_200) / NULLIF(f200.sma_200, 0)) * 100 AS sma_200_error_percent,
                CASE WHEN ABS((p.sma_200 - f200.sma_200) / NULLIF(f200.sma_200, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS sma_200_is_critical,

                CURRENT_TIMESTAMP()
            FROM 
                mart_sma_polygon p
            LEFT JOIN 
                mart_sma_flexible f10
            ON 
                p.ticker = f10.ticker
                AND p.timestamp = f10.date
            LEFT JOIN 
                mart_sma_flexible f20
            ON 
                p.ticker = f20.ticker
                AND p.timestamp = f20.date
            LEFT JOIN 
                mart_sma_flexible f50
            ON 
                p.ticker = f50.ticker
                AND p.timestamp = f50.date
            LEFT JOIN 
                mart_sma_flexible f100
            ON 
                p.ticker = f100.ticker
                AND p.timestamp = f100.date
            LEFT JOIN 
                mart_sma_flexible f200
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
        SELECT COUNT(*) FROM mart_sma_errors 
        WHERE ticker = %s AND sma_{window}_is_critical = TRUE
        """, (ticker,))

        critical_count = cursor.fetchone()[0]
        print(f"Found {critical_count} critical errors for {ticker} with SMA {window}")


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
    create_sma_table_if_not_exists(cursor)
    create_sma_errors_table_if_not_exists(cursor)

    # Get ticker list
    cursor.execute(
        "SELECT ticker FROM dim_ticker_classifier WHERE MARKET_CAP_CATEGORY = 'Mega Cap' AND overall_rank <= 7645")
    ticker_list = [row[0] for row in cursor.fetchall()]

    print(f"Processing {len(ticker_list)} tickers")

    # Process each ticker
    for ticker in ticker_list:
        try:
            # Dictionary to hold SMA data for each window
            sma_data_by_window = {}

            # Fetch SMA data for each window
            for window in window_list:
                try:
                    sma_data = fetch_sma(ticker=ticker, window=window)
                    sma_data_by_window[window] = sma_data

                    if not sma_data:
                        print(f"No SMA data fetched for {ticker} with window {window}")

                except Exception as e:
                    print(f"Error fetching SMA {window} for {ticker}: {str(e)}")
                    sma_data_by_window[window] = []

            # Load consolidated data to Snowflake
            records_loaded = load_sma_to_snowflake(cursor, sma_data_by_window, ticker)

            # Only analyze errors if we loaded data
            if records_loaded > 0:
                # Analyze errors (now handles all windows at once)
                analyze_sma_errors(cursor, ticker)

            # Commit after each ticker to avoid losing all data if an error occurs
            load_handler.conn.commit()

        except Exception as e:
            print(f"Error processing ticker {ticker}: {str(e)}")
            # Continue with next ticker
            continue

    # Close connection
    cursor.close()

    print("SMA data processing completed")


if __name__ == '__main__':
    main()