import time, os, requests
from configparser import ConfigParser
from datetime import datetime, timedelta
from snowflake_config_handler import SnowflakeTableHandler
import yaml
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import snowflake.connector
from dotenv import load_dotenv
from typing import Dict, Optional
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
print("JAVA_HOME is set to:", os.environ.get("JAVA_HOME"))

# Get the path to AWS credentials file
aws_credentials_path = os.path.expanduser('~/.aws/credentials')

# Read the credentials
config = ConfigParser()
config.read(aws_credentials_path)

# Access polygon profile credentials
polygon_access_key = config['polygon']['aws_access_key_id']
polygon_secret_key = config['polygon']['aws_secret_access_key']

BASE_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/2025-02-02"


def fetch_all_tickers(filter_columns):
    # Calculate the date 5 years ago
    # start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")

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
    url = BASE_URL
    count = 0
    print(data)

    if "results" in data:
        # If filter_columns is provided, filter each result dictionary
        if filter_columns:
            filtered_results = [
                {col: item[col] for col in filter_columns if col in item}
                for item in data["results"]
            ]
            all_tickers.extend(filtered_results)
        else:
            all_tickers.extend(data["results"])


        # Get the next URL from response
        # print(all_splits)
        count+=1
    print(f"executed the loop {count} times... returning the all_tickers data.....")
    return all_tickers

if __name__=='__main__':

    try:
        from datetime import datetime

        # Given timestamp in milliseconds
        timestamp_ms = 1738616400000

        # Convert to seconds
        timestamp_s = timestamp_ms / 1000

        # Get a UTC datetime object
        dt_utc = datetime.utcfromtimestamp(timestamp_s)

        print(dt_utc)
        # Initialize Snowflake manager
        current_dir = Path(__file__).parent
        dbt_project_path = current_dir.parent / "dbt_project"


        manager = SnowflakeTableHandler(
            project_dir=dbt_project_path,
            profile_name="jaffle_shop"
        )

        spark = manager.spark

        schema_sql = "ticker STRING, volume STRING, open STRING, close STRING, high STRING, low STRING, date STRING, transactions STRING"

        all_tickers = fetch_all_tickers(filter_columns=['T','v','o','c','h','l','t','n'])
        print(all_tickers)
        mapped_tickers = []
        for rec in all_tickers:
            mapped_tickers.append({
                "ticker": rec.get("T"),
                "volume": rec.get("v", None),
                "open": rec.get("o"),
                "close": rec.get("c"),
                "high": rec.get("h"),
                "low": rec.get("l"),
                "date": rec.get("t"),
                "transactions": rec.get("n", None)
            })
        df = spark.createDataFrame(mapped_tickers, schema_sql)
        new_df = df.withColumn("date", F.expr("CAST(from_unixtime(date/1000) AS DATE)")) \
            .withColumn("close", F.expr("CAST(close AS DOUBLE)")) \
            .withColumn("open", F.expr("CAST(open AS DOUBLE)")) \
            .withColumn("high", F.expr("CAST(high AS DOUBLE)")) \
            .withColumn("low", F.expr("CAST(low AS DOUBLE)")) \
            .withColumn("volume", F.expr("CAST(volume AS DOUBLE)")) \
            .withColumn("transactions", F.expr("CAST(transactions AS INT)"))
        # for record in all_tickers:
        #     print(record)
        #
        #
        # manager.write_partitioned_df(
        #     df=df,
        #     table_name="Stock_tickers",
        #     mode="overwrite"
        # )
        new_df.show(1000,truncate = False)
        new_df.selectExpr("min(date) as start","max(date) as end").show(truncate=False)




    except Exception as e:
        print(f"Error in main execution: {str(e)}")

