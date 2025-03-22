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

# Get the path to AWS credentials file
aws_credentials_path = os.path.expanduser('~/.aws/credentials')

# Read the credentials
config = ConfigParser()
config.read(aws_credentials_path)

# Access polygon profile credentials
polygon_access_key = config['polygon']['aws_access_key_id']
polygon_secret_key = config['polygon']['aws_secret_access_key']

BASE_URL = "https://api.polygon.io/v3/reference/tickers"

def fetch_all_tickers(limit=1000, filter_columns=None):
    # Calculate the date 5 years ago
    # start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")

    params = {
        "limit": limit,
        "apiKey": polygon_secret_key # Fetch only splits from the last 5 years
    }

    all_tickers = []
    url = BASE_URL
    count = 0

    while url:
        response = requests.get(url, params=params)
        data = response.json()

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
        url = data.get("next_url")

        if url:
            time.sleep(1)  # Avoid hitting API rate limits
        # print(all_splits)
        count+=1
    print(f"executed the loop {count} times... returning the all_tickers data.....")
    return all_tickers

if __name__=='__main__':

    try:
        # Initialize Snowflake manager
        current_dir = Path(__file__).parent
        dbt_project_path = current_dir.parent / "dbt_project"


        manager = SnowflakeTableHandler(
            project_dir=dbt_project_path,
            profile_name="jaffle_shop"
        )

        spark = manager.spark

        schema_sql = "ticker STRING, name STRING, market STRING, type STRING, active boolean"

        all_tickers = fetch_all_tickers(filter_columns=['ticker','name','market','type','active'])
        df = spark.createDataFrame(all_tickers, schema_sql)

        # final_df = df.selectExpr('ticker', 'split_from', 'split_to', 'execution_date as date')


        manager.write_partitioned_df(
            df=df,
            table_name="Stock_tickers",
            mode="overwrite"
        )



    except Exception as e:
        print(f"Error in main execution: {str(e)}")

