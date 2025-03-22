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


def process_sma_data(ds, **kwargs):
    """
    Complete SMA data processing including fetching from Polygon API and error analysis

    Args:
        ds: Execution date in YYYY-MM-DD format (from Airflow context)
    """
    print(f"Processing SMA data for date: {ds}")

    # # AWS credentials path
    # aws_credentials_path = os.path.expanduser('~/.aws/credentials')
    #
    # # Read the credentials
    # config = ConfigParser()
    # config.read(aws_credentials_path)

    # Access polygon profile credentials
    # polygon_secret_key = config['polygon']['aws_secret_access_key']

    # Base URL for API
    BASE_URL = 'https://api.polygon.io/v1/indicators/sma'

    # List of SMA windows
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
    """)
    print("Table mart_sma_polygon created or already exists")

    cursor.execute("""
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
    """)
    print("Table mart_sma_errors created or already exists")

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
                    # Fetch SMA data
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

                    print(f"Fetching SMA{window} for {ticker}...")

                    response = requests.get(url, params=params)
                    data = response.json()

                    # Handle the nested structure in the API response
                    if "results" in data and isinstance(data["results"], dict) and "values" in data["results"]:
                        all_sma_data = data["results"]["values"]
                        print(f"Found {len(all_sma_data)} results for {ticker} with window {window}")

                        # Filter for the execution date
                        filtered_data = []
                        for item in all_sma_data:
                            if item.get('timestamp'):
                                ts_seconds = item.get('timestamp') / 1000
                                date_str = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime('%Y-%m-%d')
                                if date_str == ds:
                                    filtered_data.append(item)
                                    break  # We only need one record per day

                        sma_data_by_window[window] = filtered_data

                        if not filtered_data:
                            print(f"No SMA data found for {ticker} with window {window} on {ds}")
                    else:
                        print(f"No results found for {ticker} with window {window}")
                        print(f"Response: {data}")
                        sma_data_by_window[window] = []

                except Exception as e:
                    print(f"Error fetching SMA {window} for {ticker}: {str(e)}")
                    sma_data_by_window[window] = []

            # Organize data by timestamp
            data_by_timestamp = {}

            for window, sma_data in sma_data_by_window.items():
                if not sma_data:
                    continue

                for item in sma_data:
                    # Convert timestamp from milliseconds to date only
                    if item.get('timestamp'):
                        ts_seconds = item.get('timestamp') / 1000
                        date_str = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime('%Y-%m-%d')

                        if date_str not in data_by_timestamp:
                            data_by_timestamp[date_str] = {
                                'ticker': ticker,
                                'timestamp': date_str,
                                'sma_10': None,
                                'sma_20': None,
                                'sma_50': None,
                                'sma_100': None,
                                'sma_200': None
                            }

                        data_by_timestamp[date_str][f'sma_{window}'] = item.get('value')

            # Load data to Snowflake
            records = list(data_by_timestamp.values())

            if not records:
                print(f"No SMA data to load for {ticker}")
                continue

            # Load data
            total_loaded = 0
            for record in records:
                try:
                    # Delete existing record
                    cursor.execute(
                        "DELETE FROM mart_sma_polygon WHERE ticker = %s AND timestamp = %s",
                        (record['ticker'], record['timestamp'])
                    )

                    # Insert new record
                    cursor.execute("""
                    INSERT INTO mart_sma_polygon (
                        ticker, timestamp, sma_10, sma_20, sma_50, sma_100, sma_200, ingestion_timestamp
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()
                    )
                    """, (
                        record['ticker'],
                        record['timestamp'],
                        record['sma_10'],
                        record['sma_20'],
                        record['sma_50'],
                        record['sma_100'],
                        record['sma_200']
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
                    cursor.execute("DELETE FROM mart_sma_errors WHERE ticker = %s AND timestamp = %s", (ticker, ds))

                    # Insert error analysis
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

                    cursor.execute(query, (ticker, ds))

                    # Get summary of critical errors for each window
                    for window in window_list:
                        cursor.execute(f"""
                        SELECT COUNT(*) FROM mart_sma_errors 
                        WHERE ticker = %s AND timestamp = %s AND sma_{window}_is_critical = TRUE
                        """, (ticker, ds))

                        critical_count = cursor.fetchone()[0]
                        if critical_count > 0:
                            print(f"Found {critical_count} critical errors for {ticker} with SMA {window} on {ds}")

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

    print(f"SMA data processing completed for {ds}")
    return "SMA data processing completed successfully"

if __name__ == '__main__':
    process_sma_data(ds = '2025-02-28')