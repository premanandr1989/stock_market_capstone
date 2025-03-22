import os
# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
# print("JAVA_HOME is set to:", os.environ.get("JAVA_HOME"))
import requests
import snowflake.connector
import time, os, requests
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from snowflake_config_handler import SnowflakeTableHandler
import yaml
from pathlib import Path

aws_credentials_path = os.path.expanduser('~/.aws/credentials')

# Read the credentials
config = ConfigParser()
config.read(aws_credentials_path)

# Access polygon profile credentials
polygon_access_key = config['polygon']['aws_access_key_id']
polygon_secret_key = config['polygon']['aws_secret_access_key']

BASE_URL = "https://api.polygon.io/v2/reference/news"
all_tickers_news = []
start_date = datetime.strptime("2025-02-03", "%Y-%m-%d") \
    .replace(tzinfo=timezone.utc) \
    .strftime("%Y-%m-%dT%H:%M:%SZ")

print(start_date)

def fetch_news(limit=1000, filter_columns=None):
    """
    Fetch news data from the Polygon API endpoint.
    """
    params = {
        "limit": limit,
        "apiKey": polygon_secret_key,
        "published_utc":'2025-02-03'
    }

    url = BASE_URL
    count = 0

    while url:
        response = requests.get(url, params=params)
        data = response.json()
        print(data)

        if "results" in data:
            # If filter_columns is provided, filter each result dictionary
            if filter_columns:
                filtered_results = [
                    {col: item[col] for col in filter_columns if col in item}
                    for item in data["results"]
                ]
                all_tickers_news.extend(filtered_results)
            else:
                all_tickers_news.extend(data["results"])


        # Get the next URL from response
        url = data.get("next_url")

        if url:
            time.sleep(1)  # Avoid hitting API rate limits
        # print(all_splits)
        count+=1
        print(f"executed the loop {count} times... there are totally {len(all_tickers_news)} as of {count} execution and last_date is {all_tickers_news[-1]['published_utc']}.....")
    print(f"executed the loop {count} times... returning the all_tickers data.....")
    return all_tickers_news


def transform_news_item(item):
    """
    Transform a news item into the desired format.
    Adjust the keys as needed based on the actual API response.
    """
    transformed = {
        "published_utc": item.get("published_utc"),  # Snowflake will auto-cast ISO strings to TIMESTAMP
        "article_url": item.get("article_url")
    }

    # Flatten the insights field.
    # For demonstration, we extract the first insight in the list if available.
    insights = item.get("insights")
    if insights and isinstance(insights, list) and len(insights) > 0:
        first_insight = insights[0]
        transformed["ticker"] = first_insight.get("ticker")
        transformed["sentiment"] = first_insight.get("sentiment")
        transformed["sentiment_reasoning"] = first_insight.get("sentiment_reasoning")
    else:
        transformed["ticker"] = None
        transformed["sentiment"] = None
        transformed["sentiment_reasoning"] = None

    return transformed

def load_news_to_snowflake(news_items):
    """
    Connects to Snowflake, creates the target table if it doesn't exist,
    and loads the news items into the table.
    """
    # Establish a connection to Snowflake.
    current_dir = Path(__file__).parent
    dbt_project_path = current_dir.parent / "dbt_project"

    load_handler = SnowflakeTableHandler(
        project_dir=dbt_project_path,
        profile_name="jaffle_shop"
    )

    if not load_handler.conn:
        load_handler.connect()
    cur_exc = load_handler.conn.cursor()

    # Create the table if it doesn't exist.
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS polygon_news cluster by (ticker,date) (
        ticker STRING,
        sentiment STRING,
        sentiment_reasoning STRING,
        date DATE,
        article_url STRING
    )
    """
    cur_exc.execute(create_table_sql)

    # Prepare the INSERT statement.
    insert_sql = """
    INSERT INTO polygon_news (ticker, sentiment, sentiment_reasoning, date, article_url)
    VALUES (%(ticker)s, %(sentiment)s, %(sentiment_reasoning)s, TO_DATE(%(published_utc)s), %(article_url)s)
    """

    # Transform news items.
    transformed_data = [transform_news_item(item) for item in news_items]

    # Insert data in bulk.
    cur_exc.executemany(insert_sql, transformed_data)
    load_handler.conn.commit()

    cur_exc.close()
    load_handler.close()
    print("News data loaded into Snowflake successfully.")

def main():
    news_items = fetch_news(filter_columns=['insights','published_utc','article_url'])
    if not news_items:
        print("No news data was fetched.")
        return
    load_news_to_snowflake(news_items)

if __name__ == '__main__':
    main()
