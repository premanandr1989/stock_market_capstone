import asyncio, requests
import aiohttp
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
from typing import List, Dict, Optional
import os
from configparser import ConfigParser
from snowflake_config_handler import SnowflakeTableHandler
from pathlib import Path


aws_credentials_path = os.path.expanduser('~/.aws/credentials')

config = ConfigParser()
config.read(aws_credentials_path)

# Access polygon profile credentials
polygon_access_key = config['polygon']['aws_access_key_id']
polygon_secret_key = config['polygon']['aws_secret_access_key']
all_tickers_file = "all_tickers.json"
ticker_details_file = "ticker_details.json"


def fetch_all_tickers(table_name, market: Optional[str] = None) -> List[str]:
    """Fetch all tickers using requests library."""

    """
    Connects to Snowflake, creates the target table if it doesn't exist,
    and loads the news items into the table.
    """
    # Establish a connection to Snowflake.
    try:
        current_dir = Path(__file__).parent
        dbt_project_path = current_dir.parent / "dbt_project"

        load_handler = SnowflakeTableHandler(
            project_dir=dbt_project_path,
            profile_name="jaffle_shop"
        )

        spark = load_handler.spark

        # Build query with filters
        query = f"SELECT ticker FROM {load_handler.sf_options['sfSchema']}.{table_name}"
        filters = []

        if market:
            filters.append(f"market = '{market}'")

        if filters:
            query += " WHERE " + " AND ".join(filters)

        # Configure JDBC URL and properties
        jdbc_url = f"jdbc:snowflake://{load_handler.sf_options['sfAccount']}.snowflakecomputing.com"

        connection_properties = {
            "user": load_handler.sf_options['sfUser'],
            "password": load_handler.sf_options['sfPassword'],
            "warehouse": load_handler.sf_options['sfWarehouse'],
            "db": load_handler.sf_options['sfDatabase'],
            "schema": load_handler.sf_options['sfSchema'],
            "role": load_handler.sf_options['sfRole'],
            "driver": "net.snowflake.client.jdbc.SnowflakeDriver"
        }

        # Read data using query
        df = spark.read \
            .jdbc(url=jdbc_url,
                  table=f"({query})",
                  properties=connection_properties)

        results = df.collect()
        return results

    except Exception as e:
        print(f"Error reading partitioned table: {str(e)}")
        raise

    # while next_url:
    #     try:
    #         response = requests.get(next_url, verify=False)
    #         if response.status_code == 200:
    #             data = response.json()
    #             tickers.extend([result['ticker'] for result in data['results']])
    #
    #             # Get next page URL
    #             next_url = data.get('next_url')
    #             if next_url:
    #                 next_url = f"{next_url}&apiKey={api_key}"
    #
    #             print(f"Fetched {len(tickers)} tickers so far...")
    #             time.sleep(0.2)  # Rate limiting
    #         else:
    #             print(f"Error: {response.status_code}")
    #             print(f"Response: {response.text}")
    #             break
    #
    #     except Exception as e:
    #         print(f"Error fetching tickers: {str(e)}")
    #         break
    #
    # return tickers


def fetch_ticker_details(api_key: str, ticker: str) -> Dict:
    """Fetch details for a single ticker."""
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {"apiKey": api_key}

    try:
        response = requests.get(url, params=params, verify=False)
        if response.status_code == 200:
            return {ticker: response.json()}
        else:
            print(f"Error fetching {ticker}: {response.status_code}")
            return {ticker: None}
    except Exception as e:
        print(f"Exception fetching {ticker}: {str(e)}")
        return {ticker: None}


def main():
    API_KEY = polygon_secret_key  # Replace with your API key

    # 1. Fetch all tickers
    print("Fetching all tickers...")
    tickers = fetch_all_tickers(table_name='stock_tickers',market='stocks')
    print(f"Found {len(tickers)} tickers")

    # 2. Fetch details for each ticker
    if tickers:
        print("Fetching ticker details...")
        all_results = {}

        for i, ticker in enumerate(tickers):
            print(f"Processing ticker {i + 1}/{len(tickers)}: {ticker.TICKER}")
            result = fetch_ticker_details(API_KEY, ticker.TICKER)
            all_results.update(result)

            # Save progress every 100 tickers
            if (i + 1) % 100 == 0:
                with open(all_tickers_file, 'w') as f:
                    json.dump(all_results, f)
                print(f"Saved progress: {i + 1} tickers processed")

            time.sleep(0.2)  # Rate limiting

        # Save final results
        with open(all_tickers_file, 'w') as f:
            json.dump(all_results, f)
        print("Completed fetching ticker details")
    else:
        print("No tickers found. Please check your API key and connection.")

class SparkProcessor:
    def __init__(self):
        current_dir = Path(__file__).parent
        dbt_project_path = current_dir.parent / "dbt_project"

        self.manager = SnowflakeTableHandler(
            project_dir=dbt_project_path,
            profile_name="jaffle_shop"
        )

        self.spark = self.manager.spark

        # Define the schema for the "results" field
        self.results_schema = StructType([
            StructField("ticker", StringType(), True),
            StructField("name", StringType(), True),
            StructField("market", StringType(), True),
            StructField("locale", StringType(), True),
            StructField("primary_exchange", StringType(), True),
            StructField("type", StringType(), True),
            StructField("active", BooleanType(), True),
            StructField("currency_name", StringType(), True),
            StructField("cik", StringType(), True),
            StructField("composite_figi", StringType(), True),
            StructField("share_class_figi", StringType(), True),
            StructField("market_cap", DoubleType(), True),
            StructField("phone_number", StringType(), True),
            StructField("address", StructType([
                StructField("address1", StringType(), True),
                StructField("address2", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("postal_code", StringType(), True)
            ]), True),
            StructField("description", StringType(), True),
            StructField("sic_code", StringType(), True),
            StructField("sic_description", StringType(), True),
            StructField("ticker_root", StringType(), True),
            StructField("homepage_url", StringType(), True),
            StructField("total_employees", IntegerType(), True),
            # If you want to parse the date, you can adjust the format accordingly.
            StructField("list_date", StringType(), True),
            StructField("branding", StructType([
                StructField("logo_url", StringType(), True),
                StructField("icon_url", StringType(), True)
            ]), True),
            StructField("share_class_shares_outstanding", LongType(), True),
            StructField("weighted_shares_outstanding", LongType(), True),
            StructField("round_lot", IntegerType(), True)
        ])

        # Define the schema for each ticker entry
        self.ticker_entry_schema = StructType([
            StructField("request_id", StringType(), True),
            StructField("results", self.results_schema, True),
            StructField("status", StringType(), True)
        ])

        # The top-level JSON is a map from ticker string to ticker_entry_schema
        self.top_level_schema = MapType(StringType(), self.ticker_entry_schema)

    def process_data(self, input_path: str):

        # Read the JSON file with multiline enabled
        # Read the file as text (each file is one JSON string)
        df_raw = self.spark.read.text(input_path)

        # Parse the JSON string into a column "data" using the top_level_schema
        df_parsed = df_raw.select(from_json(col("value"), self.top_level_schema).alias("data"))

        # The parsed DataFrame has a single column "data" (a map); explode it to get one row per ticker.
        df_exploded = df_parsed.select(explode(col("data")).alias("ticker", "details"))

        # Now df_exploded has two columns:
        #   - ticker: the key from the JSON (like "XELB", "ZZZ", etc.)
        #   - details: a struct with request_id, results, and status.
        df_exploded.printSchema()
        df_exploded.show(truncate=False)

        df_filtered = df_exploded.select(
            col("ticker"),
            col("details.results.type").alias("type"),
            col("details.results.market_cap").alias("market_cap"),
            col("details.results.sic_code").alias("sic_code"),
            col("details.results.sic_description").alias("sic_description"),
            col("details.results.branding.icon_url").alias("branding_icon_url"),
            col("details.results.branding.logo_url").alias("branding_logo_url"),
            col("details.results.total_employees").alias("total_employees"),
            col("details.results.name").alias("name"),
            col("details.results.share_class_shares_outstanding").alias("share_class_shares_outstanding"),
            col("details.results.weighted_shares_outstanding").alias("weighted_shares_outstanding"),
            col("details.results.primary_exchange").alias("primary_exchange")


        )


        return df_filtered


if __name__ == "__main__":
    # data = fetch_all_tickers(table_name='stock_tickers',market='stocks')
    # print(len(data))
    # count = 0
    # for item in data['results']:
    #     print(item)
    # print(data['queryCount'])

    # main()

    # 3. Process with Spark and load to Snowflake
    print("Processing data with Spark...")
    processor = SparkProcessor()

    try:
        # Write to Snowflake
        print("Writing to Snowflake...")
        df = processor.process_data(all_tickers_file)
        df.show(truncate = False)
        processor.manager.write_partitioned_df(
            df=df,
            table_name="Stock_tickers_details_enriched",
            mode="overwrite"
        )
        print("Pipeline complete!")
    except Exception as e:
        print(e)

    # finally:
    #     processor.spark.stop()