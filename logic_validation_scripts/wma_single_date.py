import os
import requests
import snowflake.connector
import time
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from snowflake_config_handler import SnowflakeTableHandler
import yaml
from pathlib import Path
from dotenv import load_dotenv

# AWS credentials path
aws_credentials_path = os.path.expanduser('~/.aws/credentials')

# Read the credentials
config = ConfigParser()
config.read(aws_credentials_path)

# Access polygon profile credentials
polygon_access_key = config['polygon']['aws_access_key_id']
polygon_secret_key = config['polygon']['aws_secret_access_key']

# Base URL for API
BASE_URL = 'https://api.polygon.io/v1/indicators/wma'


# Get credentials
# credentials_dict = Variable.get('POLYGON_CREDENTIALS', deserialize_json=True)
# polygon_api_key = credentials_dict['AWS_SECRET_ACCESS_KEY']

# Initialize paths
dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dbt_capstone', 'dbt.env')
load_dotenv(dbt_env_path)

print(dbt_env_path)

#Check what is causing the error
# Retrieve environment variables
airflow_home = os.getenv('AIRFLOW_HOME')
print(airflow_home)


def process_wma_data(ds, **kwargs):
    """
    Complete wma data processing including fetching from Polygon API and error analysis

    Args:
        ds: Execution date in YYYY-MM-DD format (from Airflow context)
    """
    print(f"Processing wma data for date: {ds}")

    # # AWS credentials path
    # aws_credentials_path = os.path.expanduser('~/.aws/credentials')
    #
    # # Read the credentials
    # config = ConfigParser()
    # config.read(aws_credentials_path)

    # Access polygon profile credentials
    # polygon_secret_key = config['polygon']['aws_secret_access_key']

    # Base URL for API
    BASE_URL = 'https://api.polygon.io/v1/indicators/wma'

    # List of wma windows
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
    CREATE TABLE IF NOT EXISTS mart_wma_polygon (
        ticker VARCHAR(20),
        timestamp DATE,
        wma_10 FLOAT,
        wma_20 FLOAT,
        wma_50 FLOAT,
        wma_100 FLOAT,
        wma_200 FLOAT,
        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (ticker, timestamp)
    )
    """)
    print("Table mart_wma_polygon created or already exists")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mart_wma_errors (
        ticker VARCHAR(20),
        timestamp DATE,
        wma_10_polygon FLOAT,
        wma_10_calculated FLOAT,
        wma_10_error_percent FLOAT,
        wma_10_is_critical BOOLEAN,
        wma_20_polygon FLOAT,
        wma_20_calculated FLOAT,
        wma_20_error_percent FLOAT,
        wma_20_is_critical BOOLEAN,
        wma_50_polygon FLOAT,
        wma_50_calculated FLOAT,
        wma_50_error_percent FLOAT,
        wma_50_is_critical BOOLEAN,
        wma_100_polygon FLOAT,
        wma_100_calculated FLOAT,
        wma_100_error_percent FLOAT,
        wma_100_is_critical BOOLEAN,
        wma_200_polygon FLOAT,
        wma_200_calculated FLOAT,
        wma_200_error_percent FLOAT,
        wma_200_is_critical BOOLEAN,
        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (ticker, timestamp)
    )
    """)
    print("Table mart_wma_errors created or already exists")

    # Get ticker list
    cursor.execute(
        "SELECT ticker FROM dim_ticker_classifier WHERE MARKET_CAP_CATEGORY = 'Mega Cap' AND overall_rank <= 7645")
    ticker_list = [row[0] for row in cursor.fetchall()]

    print(f"Processing {len(ticker_list)} tickers")

    # Process each ticker
    for ticker in ticker_list:
        try:
            # Dictionary to hold wma data for each window
            wma_data_by_window = {}

            # Fetch wma data for each window
            for window in window_list:
                try:
                    # Fetch wma data
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

                    print(f"Fetching wma{window} for {ticker}...")

                    response = requests.get(url, params=params)
                    data = response.json()

                    # Handle the nested structure in the API response
                    if "results" in data and isinstance(data["results"], dict) and "values" in data["results"]:
                        all_wma_data = data["results"]["values"]
                        print(f"Found {len(all_wma_data)} results for {ticker} with window {window}")

                        # Filter for the execution date
                        filtered_data = []
                        for item in all_wma_data:
                            if item.get('timestamp'):
                                ts_seconds = item.get('timestamp') / 1000
                                date_str = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime('%Y-%m-%d')
                                if date_str == ds:
                                    filtered_data.append(item)
                                    break  # We only need one record per day

                        wma_data_by_window[window] = filtered_data

                        if not filtered_data:
                            print(f"No wma data found for {ticker} with window {window} on {ds}")
                    else:
                        print(f"No results found for {ticker} with window {window}")
                        print(f"Response: {data}")
                        wma_data_by_window[window] = []

                except Exception as e:
                    print(f"Error fetching wma {window} for {ticker}: {str(e)}")
                    wma_data_by_window[window] = []

            # Organize data by timestamp
            data_by_timestamp = {}

            for window, wma_data in wma_data_by_window.items():
                if not wma_data:
                    continue

                for item in wma_data:
                    # Convert timestamp from milliseconds to date only
                    if item.get('timestamp'):
                        ts_seconds = item.get('timestamp') / 1000
                        date_str = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime('%Y-%m-%d')

                        if date_str not in data_by_timestamp:
                            data_by_timestamp[date_str] = {
                                'ticker': ticker,
                                'timestamp': date_str,
                                'wma_10': None,
                                'wma_20': None,
                                'wma_50': None,
                                'wma_100': None,
                                'wma_200': None
                            }

                        data_by_timestamp[date_str][f'wma_{window}'] = item.get('value')

            # Load data to Snowflake
            records = list(data_by_timestamp.values())

            if not records:
                print(f"No wma data to load for {ticker}")
                continue

            # Load data
            total_loaded = 0
            for record in records:
                try:
                    # Delete existing record
                    cursor.execute(
                        "DELETE FROM mart_wma_polygon WHERE ticker = %s AND timestamp = %s",
                        (record['ticker'], record['timestamp'])
                    )

                    # Insert new record
                    cursor.execute("""
                    INSERT INTO mart_wma_polygon (
                        ticker, timestamp, wma_10, wma_20, wma_50, wma_100, wma_200, ingestion_timestamp
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()
                    )
                    """, (
                        record['ticker'],
                        record['timestamp'],
                        record['wma_10'],
                        record['wma_20'],
                        record['wma_50'],
                        record['wma_100'],
                        record['wma_200']
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
                    cursor.execute("DELETE FROM mart_wma_errors WHERE ticker = %s AND timestamp = %s", (ticker, ds))

                    # Insert error analysis
                    query = """
                    INSERT INTO mart_wma_errors (
                        ticker, timestamp, 
                        wma_10_polygon, wma_10_calculated, wma_10_error_percent, wma_10_is_critical,
                        wma_20_polygon, wma_20_calculated, wma_20_error_percent, wma_20_is_critical,
                        wma_50_polygon, wma_50_calculated, wma_50_error_percent, wma_50_is_critical,
                        wma_100_polygon, wma_100_calculated, wma_100_error_percent, wma_100_is_critical,
                        wma_200_polygon, wma_200_calculated, wma_200_error_percent, wma_200_is_critical,
                        ingestion_timestamp
                    )
                    SELECT 
                        p.ticker,
                        p.timestamp,
                        p.wma_10 AS wma_10_polygon,
                        f10.wma_10 AS wma_10_calculated,
                        ABS((p.wma_10 - f10.wma_10) / NULLIF(f10.wma_10, 0)) * 100 AS wma_10_error_percent,
                        CASE WHEN ABS((p.wma_10 - f10.wma_10) / NULLIF(f10.wma_10, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS wma_10_is_critical,

                        p.wma_20 AS wma_20_polygon,
                        f20.wma_20 AS wma_20_calculated,
                        ABS((p.wma_20 - f20.wma_20) / NULLIF(f20.wma_20, 0)) * 100 AS wma_20_error_percent,
                        CASE WHEN ABS((p.wma_20 - f20.wma_20) / NULLIF(f20.wma_20, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS wma_20_is_critical,

                        p.wma_50 AS wma_50_polygon,
                        f50.wma_50 AS wma_50_calculated,
                        ABS((p.wma_50 - f50.wma_50) / NULLIF(f50.wma_50, 0)) * 100 AS wma_50_error_percent,
                        CASE WHEN ABS((p.wma_50 - f50.wma_50) / NULLIF(f50.wma_50, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS wma_50_is_critical,

                        p.wma_100 AS wma_100_polygon,
                        f100.wma_100 AS wma_100_calculated,
                        ABS((p.wma_100 - f100.wma_100) / NULLIF(f100.wma_100, 0)) * 100 AS wma_100_error_percent,
                        CASE WHEN ABS((p.wma_100 - f100.wma_100) / NULLIF(f100.wma_100, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS wma_100_is_critical,

                        p.wma_200 AS wma_200_polygon,
                        f200.wma_200 AS wma_200_calculated,
                        ABS((p.wma_200 - f200.wma_200) / NULLIF(f200.wma_200, 0)) * 100 AS wma_200_error_percent,
                        CASE WHEN ABS((p.wma_200 - f200.wma_200) / NULLIF(f200.wma_200, 0)) * 100 > 0.1 THEN TRUE ELSE FALSE END AS wma_200_is_critical,

                        CURRENT_TIMESTAMP()
                    FROM 
                        mart_wma_polygon p
                    LEFT JOIN 
                        mart_wma_flexible f10
                    ON 
                        p.ticker = f10.ticker
                        AND p.timestamp = f10.date
                    LEFT JOIN 
                        mart_wma_flexible f20
                    ON 
                        p.ticker = f20.ticker
                        AND p.timestamp = f20.date
                    LEFT JOIN 
                        mart_wma_flexible f50
                    ON 
                        p.ticker = f50.ticker
                        AND p.timestamp = f50.date
                    LEFT JOIN 
                        mart_wma_flexible f100
                    ON 
                        p.ticker = f100.ticker
                        AND p.timestamp = f100.date
                    LEFT JOIN 
                        mart_wma_flexible f200
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
                        SELECT COUNT(*) FROM mart_wma_errors 
                        WHERE ticker = %s AND timestamp = %s AND wma_{window}_is_critical = TRUE
                        """, (ticker, ds))

                        critical_count = cursor.fetchone()[0]
                        if critical_count > 0:
                            print(f"Found {critical_count} critical errors for {ticker} with wma {window} on {ds}")

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

    print(f"wma data processing completed for {ds}")
    return "wma data processing completed successfully"

if __name__ == '__main__':
    # process_wma_data(ds = '2025-02-03')
    pass