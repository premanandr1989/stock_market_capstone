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


def process_ema_data(ds, **kwargs):
    """
    Complete ema data processing including fetching from Polygon API and error analysis

    Args:
        ds: Execution date in YYYY-MM-DD format (from Airflow context)
    """
    print(f"Processing ema data for date: {ds}")

    # # AWS credentials path
    # aws_credentials_path = os.path.expanduser('~/.aws/credentials')
    #
    # # Read the credentials
    # config = ConfigParser()
    # config.read(aws_credentials_path)

    # Access polygon profile credentials
    # polygon_secret_key = config['polygon']['aws_secret_access_key']

    # Base URL for API
    BASE_URL = 'https://api.polygon.io/v1/indicators/ema'

    # List of ema windows
    window_list = [10, 20, 50, 100, 200]

    # Get Snowflake connection
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
    cursor.execute("""
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
    """)
    print("Table mart_ema_polygon created or already exists")

    cursor.execute("""
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
    """)
    print("Table mart_ema_errors created or already exists")

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
                    # Fetch ema data
                    params = {
                        "limit": 5000,
                        "apiKey": polygon_secret_key,
                        "timestamp": ds,  # Use execution date
                        "timespan": 'day',
                        "window": window,
                        "series_type": 'close',
                        "adjusted": 'true'
                    }

                    url = f"{BASE_URL}/{ticker}"

                    print(f"Fetching ema{window} for {ticker}...")

                    response = requests.get(url, params=params)
                    data = response.json()

                    # Handle the nested structure in the API response
                    if "results" in data and isinstance(data["results"], dict) and "values" in data["results"]:
                        all_ema_data = data["results"]["values"]
                        print(f"Found {len(all_ema_data)} results for {ticker} with window {window}")

                        # Filter for the execution date
                        filtered_data = []
                        for item in all_ema_data:
                            if item.get('timestamp'):
                                ts_seconds = item.get('timestamp') / 1000
                                date_str = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime('%Y-%m-%d')
                                if date_str == ds:
                                    filtered_data.append(item)
                                    break  # We only need one record per day

                        ema_data_by_window[window] = filtered_data

                        if not filtered_data:
                            print(f"No ema data found for {ticker} with window {window} on {ds}")
                    else:
                        print(f"No results found for {ticker} with window {window}")
                        print(f"Response: {data}")
                        ema_data_by_window[window] = []

                except Exception as e:
                    print(f"Error fetching ema {window} for {ticker}: {str(e)}")
                    ema_data_by_window[window] = []

            # Organize data by timestamp
            data_by_timestamp = {}

            for window, ema_data in ema_data_by_window.items():
                if not ema_data:
                    continue

                for item in ema_data:
                    # Convert timestamp from milliseconds to date only
                    if item.get('timestamp'):
                        ts_seconds = item.get('timestamp') / 1000
                        date_str = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime('%Y-%m-%d')

                        if date_str not in data_by_timestamp:
                            data_by_timestamp[date_str] = {
                                'ticker': ticker,
                                'timestamp': date_str,
                                'ema_10': None,
                                'ema_20': None,
                                'ema_50': None,
                                'ema_100': None,
                                'ema_200': None
                            }

                        data_by_timestamp[date_str][f'ema_{window}'] = item.get('value')

            # Load data to Snowflake
            records = list(data_by_timestamp.values())

            if not records:
                print(f"No ema data to load for {ticker}")
                continue

            # Load data
            total_loaded = 0
            for record in records:
                try:
                    # Delete existing record
                    cursor.execute(
                        "DELETE FROM mart_ema_polygon WHERE ticker = %s AND timestamp = %s",
                        (record['ticker'], record['timestamp'])
                    )

                    # Insert new record
                    cursor.execute("""
                    INSERT INTO mart_ema_polygon (
                        ticker, timestamp, ema_10, ema_20, ema_50, ema_100, ema_200, ingestion_timestamp
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()
                    )
                    """, (
                        record['ticker'],
                        record['timestamp'],
                        record['ema_10'],
                        record['ema_20'],
                        record['ema_50'],
                        record['ema_100'],
                        record['ema_200']
                    ))

                    total_loaded += 1

                except Exception as e:
                    print(f"Error loading data for {ticker}: {e}")
                    continue

            print(f"Loaded {total_loaded} records for {ticker}")

            # Analyze errors if data was loaded
            if total_loaded > 0:
                try:
                    # Delete existing error data
                    cursor.execute("DELETE FROM mart_ema_errors WHERE ticker = %s AND timestamp = %s", (ticker, ds))

                    # Insert error analysis
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

                    cursor.execute(query, (ticker, ds))

                    # Get summary of critical errors for each window
                    for window in window_list:
                        cursor.execute(f"""
                        SELECT COUNT(*) FROM mart_ema_errors 
                        WHERE ticker = %s AND timestamp = %s AND ema_{window}_is_critical = TRUE
                        """, (ticker, ds))

                        critical_count = cursor.fetchone()[0]
                        if critical_count > 0:
                            print(f"Found {critical_count} critical errors for {ticker} with ema {window} on {ds}")

                except Exception as e:
                    print(f"Error analyzing errors for {ticker} on {ds}: {e}")

            # Commit after each ticker
            load_handler.conn.commit()

        except Exception as e:
            print(f"Error processing ticker {ticker}: {str(e)}")
            continue

    # Close connection
    cursor.close()
    # load_handler.disconnect()

    print(f"ema data processing completed for {ds}")
    return "ema data processing completed successfully"

if __name__ == '__main__':
    process_ema_data(ds = '2025-02-03')