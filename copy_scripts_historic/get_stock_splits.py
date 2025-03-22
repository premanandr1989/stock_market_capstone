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

BASE_URL = "https://api.polygon.io/v3/reference/splits"

def fetch_splits_last_5_years(limit=1000):
    # Calculate the date 5 years ago
    start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")

    params = {
        "limit": limit,
        "apiKey": polygon_secret_key,
        "reverse_split_date.gte": start_date  # Fetch only splits from the last 5 years
    }

    all_splits = []
    url = BASE_URL
    count = 0

    while url:
        response = requests.get(url, params=params)
        data = response.json()

        if "results" in data:
            all_splits.extend(data["results"])

        # Get the next URL from response
        url = data.get("next_url")

        if url:
            time.sleep(1)  # Avoid hitting API rate limits
        # print(all_splits)
        count+=1
    print(f"executed the loop {count} times... returning the all_splits data.....")
    return all_splits

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
        all_splits = fetch_splits_last_5_years()

        schema_sql = "execution_date STRING, id STRING, split_from STRING, split_to STRING, ticker STRING"

        df = spark.createDataFrame(all_splits, schema_sql)
        new_df = df.withColumn("execution_date", F.expr("CAST(execution_date AS DATE)")) \
            .withColumn("split_from", F.expr("CAST(split_from AS INT)")) \
            .withColumn("split_to", F.expr("CAST(split_to AS INT)"))

        final_df = new_df.selectExpr('ticker', 'split_from', 'split_to', 'execution_date as date')

        cluster_cols = ("ticker","date")

        manager.write_partitioned_df(
            df=final_df,
            table_name="Stock_ticker_splits",
            mode="overwrite",
            create_table=True,
            cluster_cols=cluster_cols
        )



    except Exception as e:
        print(f"Error in main execution: {str(e)}")

