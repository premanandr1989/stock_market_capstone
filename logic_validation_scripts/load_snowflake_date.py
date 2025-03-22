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



def fetch_and_load_stock_data_today(ds, **kwargs):
    try:
        BASE_URL = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{ds}"

        params = {
            "adjusted": "true",
            "apiKey": polygon_secret_key
        }
        headers = {
            "Accept": "csv"
        }

        response = requests.get(BASE_URL, params=params, headers=headers)
        data = response.json()

        all_tickers = []
        filter_columns = ['T', 'v', 'o', 'c', 'h', 'l', 't', 'n']

        if "results" in data:
            # Filter results to include only the specified columns
            filtered_results = [
                {col: item[col] for col in filter_columns if col in item}
                for item in data["results"]
            ]
            all_tickers.extend(filtered_results)

        # Map API data to Snowflake table columns
        mapped_tickers = []
        for rec in all_tickers:
            # Convert timestamp to date
            timestamp_ms = rec.get("t")
            date_str = None
            if timestamp_ms:
                # Convert timestamp to UTC date
                from datetime import timezone
                seconds = timestamp_ms / 1000
                date_obj = datetime.fromtimestamp(seconds, tz=timezone.utc)
                date_str = date_obj.strftime('%Y-%m-%d')

            mapped_tickers.append({
                "ticker": rec.get("T"),
                "volume": rec.get("v", None),  # Default to None if key doesn't exist
                "open": rec.get("o", None),
                "close": rec.get("c", None),
                "high": rec.get("h", None),
                "low": rec.get("l", None),
                "date": date_str,
                "transactions": rec.get("n", None)
            })

        # Connect to Snowflake
        current_dir = Path(__file__).parent
        dbt_project_path = current_dir.parent / "dbt_capstone"

        load_handler = SnowflakeTableHandler(
            project_dir=dbt_project_path,
            profile_name="jaffle_shop"
        )
        if not load_handler.conn:
            load_handler.connect()
        # cur_exc = snowflake_handler.conn.cursor()

        cursor = load_handler.conn.cursor()

        # Check if records already exist for this date
        check_query = f"SELECT COUNT(*) FROM stock_daily_agg WHERE date = '{ds}'"
        cursor.execute(check_query)
        existing_count = cursor.fetchone()[0]

        if existing_count > 0:
            print(
                f"Found {existing_count} existing records for {ds}. These will be deleted before loading new data.")

            # Optional: Sample a few records to verify the data
            sample_query = f"SELECT * FROM stock_daily_agg WHERE date = '{ds}' LIMIT 5"
            cursor.execute(sample_query)
            sample_records = cursor.fetchall()

            if sample_records:
                print(f"Sample of existing records for {ds}:")
                for record in sample_records:
                    print(record)

        # Delete existing records for this date
        delete_query = f"DELETE FROM stock_daily_agg WHERE date = '{ds}'"
        cursor.execute(delete_query)
        print(f"Table cleaned for {ds} and ready to be loaded....")

        # Log missing value statistics
        missing_values = {
            "ticker": 0,
            "volume": 0,
            "open": 0,
            "close": 0,
            "high": 0,
            "low": 0,
            "date": 0,
            "transactions": 0
        }

        # Prepare data for bulk insert
        transformed_data = []
        for record in mapped_tickers:
            # Count missing values
            for key in record:
                if record[key] is None:
                    missing_values[key] += 1

            row = (
                record["ticker"],
                record["volume"],
                record["open"],
                record["close"],
                record["high"],
                record["low"],
                record["date"],
                record["transactions"]
            )
            transformed_data.append(row)

        print("Missing value statistics:")
        for field, count in missing_values.items():
            percentage = (count / len(mapped_tickers)) * 100 if mapped_tickers else 0
            print(f"  {field}: {count} records ({percentage:.2f}%)")

        # Use executemany for bulk insert
        insert_sql = """
        INSERT INTO stock_daily_agg 
        (ticker, volume, open, close, high, low, date, transactions)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_sql, transformed_data)

        # Sample and verify the newly loaded data
        verify_query = f"""
        SELECT ticker, volume, open, close, high, low, 
               TO_CHAR(date, 'YYYY-MM-DD') as formatted_date, 
               transactions 
        FROM stock_daily_agg 
        WHERE date = '{ds}' 
        LIMIT 10
        """
        cursor.execute(verify_query)
        new_samples = cursor.fetchall()

        if new_samples:
            print(f"\nVerification of newly loaded data for {ds}:")
            print("TICKER | VOLUME | OPEN | CLOSE | HIGH | LOW | DATE | TRANSACTIONS")
            print("-" * 80)
            for row in new_samples:
                print(" | ".join(str(val) for val in row))

        # Commit and close
        load_handler.conn.commit()
        cursor.close()
        load_handler.conn.close()

        print(f"Successfully loaded {len(transformed_data)} records for {ds}")

    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch data: {str(e)}")
    except Exception as e:
        raise Exception(f"Error in pipeline: {str(e)}")

if __name__ == '__main__':
    fetch_and_load_stock_data_today(ds = '2022-02-22')