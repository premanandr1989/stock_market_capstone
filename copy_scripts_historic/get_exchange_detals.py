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

BASE_URL = "https://api.polygon.io/v3/reference/exchanges"

def fetch_exchange_details(limit=1000):

    params = {
        "apiKey": polygon_secret_key # Fetch only splits from the last 5 years
    }

    all_excahnges = []
    url = BASE_URL
    count = 0

    response = requests.get(url, params=params)
    data = response.json()

    if "results" in data:
        all_excahnges.extend(data["results"])

    # Get the next URL from response
    # url = data.get("next_url")

    # print(all_splits)
    return all_excahnges

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
        all_ticker_details = fetch_exchange_details()

        schema_sql = "id INT, type STRING, asset_class STRING, locale STRING, name STRING, operating_mic STRING, participant_id STRING, url STRING"

        df = spark.createDataFrame(all_ticker_details, schema_sql)
        # new_df = df.withColumn("execution_date", F.expr("CAST(execution_date AS DATE)")) \
        #     .withColumn("split_from", F.expr("CAST(split_from AS INT)")) \
        #     .withColumn("split_to", F.expr("CAST(split_to AS INT)"))

        # final_df = df.selectExpr('ticker', 'split_from', 'split_to', 'execution_date as date')
        #
        # cluster_cols = ("ticker","date")

        manager.write_partitioned_df(
            df=df,
            table_name="Stock_exchange_details",
            mode="overwrite",
            create_table=True
        )



    except Exception as e:
        print(f"Error in main execution: {str(e)}")

