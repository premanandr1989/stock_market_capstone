from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from airflow.decorators import task, dag
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta, timezone
import requests, time
from airflow.models import Variable
from dotenv import load_dotenv
from dags.configs.snowflake_config import SnowflakeTableHandler
import os, finnhub


finnhub_client = finnhub.Client(api_key="**********")
# Get credentials
# credentials_dict = Variable.get('POLYGON_CREDENTIALS', deserialize_json=True)
# polygon_api_key = credentials_dict['AWS_SECRET_ACCESS_KEY']

POLYGON_CREDENTIALS = {"AWS_ACCESS_KEY_ID":"","AWS_SECRET_ACCESS_KEY":""}
polygon_api_key = POLYGON_CREDENTIALS["AWS_SECRET_ACCESS_KEY"]

# Initialize paths
dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dbt_project', 'dbt.env')
load_dotenv(dbt_env_path)

#Check what is causing the error
# Retrieve environment variables
airflow_home = os.getenv('AIRFLOW_HOME')
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_PROFILES = f'{airflow_home}/dbt_project/profiles.yml'

# Define profile paths for the dbt_capstone project file
profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

default_args = {
  "owner": "Prem",
  "retries": 0,
  "execution_timeout": timedelta(hours=1),
}


schema = 'PREM'
@dag(
    dag_id='prem_capstone_stock_metrics_pipeline',
    default_args={
        "owner": schema,
        "start_date": datetime(2025, 3, 3),
        "retries": 0,
        "max_active_tasks": 2,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 3, 3),
    max_active_runs=1,
    schedule_interval="0 0 * * 1-5",
    catchup=True,
    tags=["prem","capstone"],
)
def stock_metrics_pipeline():
    @task(retries=1, retry_delay=timedelta(seconds=10))
    def fetch_and_load_stock_data(**context):
        try:
            ds = context['ds']
            BASE_URL = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{ds}"

            params = {
                "adjusted": "true",
                "apiKey": polygon_api_key
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
            else:
                raise AirflowSkipException(f"No stock data available for {ds}")

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
            snowflake_handler = SnowflakeTableHandler(project_dir=PATH_TO_DBT_PROJECT, profile_name="jaffle_shop")

            if not snowflake_handler.conn:
                snowflake_handler.connect()
            # cur_exc = snowflake_handler.conn.cursor()

            cursor = snowflake_handler.conn.cursor()

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
            snowflake_handler.conn.commit()
            cursor.close()
            snowflake_handler.conn.close()

            print(f"Successfully loaded {len(transformed_data)} records for {ds}")

            # return {
            #     "records_processed": len(mapped_tickers),
            #     "records_loaded": len(transformed_data),
            #     "date": ds
            # }

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch data: {str(e)}")
        except Exception as e:
            raise Exception(f"Error in pipeline: {str(e)}")


    @task(retries=1, retry_delay=timedelta(seconds=10))
    def fetch_and_load_news(**context):
        """
        Fetch news data from the Polygon API endpoint.
        """
        ds = context['ds']
        BASE_URL = "https://api.polygon.io/v2/reference/news"

        all_tickers_news=[]
        params = {
            "apiKey": polygon_api_key,
            "published_utc": ds
        }

        url = BASE_URL
        filter_columns = ['insights', 'published_utc', 'article_url']
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
                print("No data for today... returning")
                return

            # Get the next URL from response
            url = data.get("next_url")

            if url:
                time.sleep(1)  # Avoid hitting API rate limits
            # print(all_splits)
        transformed_data = []
        for item in all_tickers_news:

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

            transformed_data.append(transformed)


        snowflake_handler = SnowflakeTableHandler(project_dir=PATH_TO_DBT_PROJECT, profile_name="jaffle_shop")

        if not snowflake_handler.conn:
            snowflake_handler.connect()
        cur_exc = snowflake_handler.conn.cursor()



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

        delete_from_current_date = f"""
                       DELETE FROM polygon_news
                       where date = '{ds}'
                       """
        cur_exc.execute(delete_from_current_date)
        print("table polygon_news cleaned for current date and ready for loading...")

        # Prepare the INSERT statement.
        insert_sql = """
            INSERT INTO polygon_news (ticker, sentiment, sentiment_reasoning, date, article_url)
            VALUES (%(ticker)s, %(sentiment)s, %(sentiment_reasoning)s, TO_DATE(%(published_utc)s), %(article_url)s)
            """

        # Transform news items.


        # Insert data in bulk.
        cur_exc.executemany(insert_sql, transformed_data)
        snowflake_handler.conn.commit()

        cur_exc.close()
        snowflake_handler.close()
        print("News data loaded into Snowflake successfully.")

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def fetch_and_load_earning_calendar(filter_columns=None, **context):
        """
        Fetch finnhub earning calendar data from the finnhub API endpoint.
        """
        ds = context['ds']
        tickers_earnings = []
        try:
            data = finnhub_client.earnings_calendar(_from=f'{ds}', to=f'{ds}', symbol="")

            if "earningsCalendar" in data:
                # If filter_columns is provided, filter each result dictionary
                if filter_columns:
                    filtered_results = [
                        {col: item[col] for col in filter_columns if col in item}
                        for item in data["earningsCalendar"]
                    ]
                    tickers_earnings.extend(filtered_results)
                else:
                    tickers_earnings.extend(data["earningsCalendar"])
                print(f"Fetched {len(tickers_earnings)} records for the {ds}")
            else:
                print(f"No records for the day {ds}")
        except Exception as e:
            print(f"Exception occurred {e}")

        snowflake_handler = SnowflakeTableHandler(project_dir=PATH_TO_DBT_PROJECT, profile_name="jaffle_shop")

        if not snowflake_handler.conn:
            snowflake_handler.connect()
        cur_exc = snowflake_handler.conn.cursor()

        # data = finnhub_client.company_earnings('NVDA', limit=1)

        records = []

        # Create the table if it doesn't exist.
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS earnings_calendar cluster by (ticker,date) (
                ticker STRING,
                year INTEGER,
                date DATE,
                hour INTEGER,
                quarter INTEGER,
                epsActual DOUBLE,
                epsEstimate DOUBLE,
                revenueActual BIGINT,
                revenueEstimate BIGINT
            )
            """
        cur_exc.execute(create_table_sql)

        # Prepare the INSERT statement.
        insert_sql = """
            INSERT INTO earnings_calendar (ticker, year, date, hour, quarter, epsActual, epsEstimate, revenueActual, revenueEstimate)
            VALUES (%(ticker)s, %(year)s, TO_DATE(%(date)s), %(hour)s, %(quarter)s, %(epsActual)s, %(epsEstimate)s, %(revenueActual)s, %(revenueEstimate)s)
            """

        # Delete records for the day before inserting to ensure idempotency
        delete_sql = f"""
            DELETE FROM earnings_calendar where date = '{ds}'
            """

        cur_exc.execute(delete_sql)

        print(f"table cleaned for {ds} and ready to load...")

        for result in tickers_earnings:
            # Format the record as needed for your Iceberg table
            record = {
                'ticker': result.get('symbol'),
                'year': result.get('year', None),  # Adding None as a default in case there are missing keys.
                'date': result.get('date', None),
                'hour': result.get('hour', None),
                'quarter': result.get('quarter', None),
                'epsActual': result.get('epsActual', None),
                'epsEstimate': result.get('epsEstimate', None),
                'revenueActual': result.get('revenueActual', None),
                'revenueEstimate': result.get('revenueEstimate', None),
            }
            records.append(record)

        # Insert data in bulk.
        cur_exc.executemany(insert_sql, records)
        snowflake_handler.conn.commit()

        cur_exc.close()
        snowflake_handler.close()
        print(f"earnings surprises data loaded into Snowflake successfully for {ds}")

    @task(retries=1, retry_delay=timedelta(seconds=5))
    def fetch_and_load_graders_data(filter_columns=None):
        """
        Fetch finnhub earning calendar data from the finnhub API endpoint.
        """
        BASE_URL = "https://financialmodelingprep.com/stable/grades"
        api_key = ""
        snowflake_handler = SnowflakeTableHandler(project_dir=PATH_TO_DBT_PROJECT, profile_name="jaffle_shop")

        try:
            if not snowflake_handler.conn:
                snowflake_handler.connect()
            cur_exc = snowflake_handler.conn.cursor()

            # Fetch tickers from the database
            # cur_exc.execute("Select distinct ticker from stock_graders")
            # ticker_tuples = cur_exc.fetchall()
            tickers = ['AAPL', 'TSLA', 'AMZN', 'MSFT', 'NVDA', 'GOOGL', 'META', 'NFLX', 'JPM', 'V', 'BAC', 'AMD',
                       'PYPL', 'DIS', 'T', 'PFE', 'COST', 'INTC', 'KO', 'TGT', 'NKE', 'SPY', 'BA', 'BABA', 'XOM', 'WMT',
                       'GE', 'CSCO', 'VZ', 'JNJ', 'CVX', 'PLTR', 'SQ', 'SHOP', 'SBUX', 'SOFI', 'HOOD', 'RBLX', 'SNAP',
                       'AMD', 'UBER', 'FDX', 'ABBV', 'ETSY', 'MRNA', 'LMT', 'GM', 'F', 'RIVN', 'LCID', 'CCL', 'DAL',
                       'UAL', 'AAL', 'TSM', 'SONY', 'ET', 'NOK', 'MRO', 'COIN', 'RIVN', 'SIRI', 'SOFI', 'RIOT', 'CPRX',
                       'PYPL', 'TGT', 'VWO', 'SPYG', 'NOK', 'ROKU', 'HOOD', 'VIAC', 'ATVI', 'BIDU', 'DOCU', 'ZM',
                       'PINS', 'TLRY', 'WBA', 'VIAC', 'MGM', 'NFLX', 'NIO', 'C', 'GS', 'WFC', 'ADBE', 'PEP', 'UNH',
                       'CARR', 'FUBO', 'HCA', 'TWTR', 'BILI', 'SIRI', 'VIAC', 'FUBO', 'RKT']

            print(
                f"{len(tickers)} no of tickers fetched and now moving onto fetching grades historic data for each of these tickers")

            stock_grades = []
            url = BASE_URL
            total_tickers = len(tickers)
            rate_limit_reached = False

            for i, ticker in enumerate(tickers):
                try:
                    params = {
                        "symbol": ticker,
                        "apikey": api_key
                    }
                    response = requests.get(url, params=params)
                    data = response.json()

                    # Check if response is an error message about rate limit
                    if isinstance(data, dict) and 'Error Message' in data:
                        if 'Limit Reach' in data['Error Message']:
                            print("Rate Limit exhausted, proceeding with data collected so far...")
                            rate_limit_reached = True
                            break

                    # Only process data if it's a list (valid response)
                    if isinstance(data, list) and data:
                        # If filter_columns is provided, filter each result dictionary
                        if filter_columns:
                            filtered_results = [
                                {col: item[col] for col in filter_columns if col in item}
                                for item in data
                            ]
                            stock_grades.extend(filtered_results)
                        else:
                            stock_grades.extend(data)
                        print(
                            f"Data for {ticker} is fetched, {i + 1}/{total_tickers} completed and {len(data)} records fetched.")
                    else:
                        print(f"No valid data returned for ticker {ticker}")

                except Exception as e:
                    print(f"Exception occurred for ticker {ticker}: {e}")
                    # Continue with the next ticker rather than terminating the entire function
                    continue

            # CRITICAL CHECK: Only proceed with truncate and load if we actually have data
            if stock_grades and len(stock_grades) > 0:
                try:
                    # Create the table if it doesn't exist
                    create_table_sql = """
                           CREATE TABLE IF NOT EXISTS stock_graders cluster by (ticker,date) (
                               ticker STRING,
                               date DATE,
                               gradingCompany STRING,
                               previousGrade STRING,
                               newGrade STRING,
                               action STRING
                           )
                           """
                    cur_exc.execute(create_table_sql)

                    # Prepare records for insertion
                    records = []
                    for result in stock_grades:
                        # Make sure symbol key exists to avoid KeyError
                        if 'symbol' not in result:
                            print(f"Warning: Skipping record without 'symbol' key: {result}")
                            continue

                        record = {
                            'ticker': result.get('symbol'),
                            'date': result.get('date', None),
                            'gradingCompany': result.get('gradingCompany', None),
                            'previousGrade': result.get('previousGrade', None),
                            'newGrade': result.get('newGrade', None),
                            'action': result.get('action', None)
                        }
                        records.append(record)

                    # Double-check again that we have records to insert
                    if records and len(records) > 0:
                        # Only truncate the table if we have data to replace it with
                        cur_exc.execute("TRUNCATE TABLE stock_graders")
                        print(f"Table cleaned and ready for load with {len(records)} records...")

                        # Insert data in bulk
                        insert_sql = """
                               INSERT INTO stock_graders (ticker, date, gradingCompany, previousGrade, newGrade, action)
                               VALUES (%(ticker)s, TO_DATE(%(date)s), %(gradingCompany)s, %(previousGrade)s, %(newGrade)s, %(action)s)
                               """
                        cur_exc.executemany(insert_sql, records)
                        snowflake_handler.conn.commit()

                        print(f"Stock grades data loaded into Snowflake successfully. {len(records)} records inserted.")
                    else:
                        print("WARNING: No valid records to insert. Table was NOT truncated or modified.")

                    if rate_limit_reached:
                        print("Note: Process completed with partial data due to API rate limit.")

                except Exception as e:
                    print(f"Database operation failed: {e}")
                    # Ensure we don't leave the transaction open
                    try:
                        snowflake_handler.conn.rollback()
                        print("Transaction rolled back due to error.")
                    except:
                        pass

            else:
                print("WARNING: No data was collected. Table was NOT truncated or modified.")

            cur_exc.close()
            snowflake_handler.close()

        except Exception as e:
            print(f"Critical error in fetch_and_load_graders_data: {e}")
            # try:
            #     if 'snowflake_handler' in locals() and snowflake_handler.conn:
            #         snowflake_handler.conn.rollback()
            # except:
            #     pass

        # Always return success to ensure downstream tasks continue
        finally:
            return True

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def process_sma_data(**context):
        """
        Complete SMA data processing including fetching from Polygon API and error analysis

        Args:
            ds: Execution date in YYYY-MM-DD format (from Airflow context)
        """
        ds = context['ds']
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
        snowflake_handler = SnowflakeTableHandler(project_dir=PATH_TO_DBT_PROJECT, profile_name="jaffle_shop")

        if not snowflake_handler.conn:
            snowflake_handler.connect()
        cursor = snowflake_handler.conn.cursor()

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
                            "apiKey": polygon_api_key,
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
                            CASE WHEN ABS((p.sma_10 - f10.sma_10) / NULLIF(f10.sma_10, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS sma_10_is_critical,

                            p.sma_20 AS sma_20_polygon,
                            f20.sma_20 AS sma_20_calculated,
                            ABS((p.sma_20 - f20.sma_20) / NULLIF(f20.sma_20, 0)) * 100 AS sma_20_error_percent,
                            CASE WHEN ABS((p.sma_20 - f20.sma_20) / NULLIF(f20.sma_20, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS sma_20_is_critical,

                            p.sma_50 AS sma_50_polygon,
                            f50.sma_50 AS sma_50_calculated,
                            ABS((p.sma_50 - f50.sma_50) / NULLIF(f50.sma_50, 0)) * 100 AS sma_50_error_percent,
                            CASE WHEN ABS((p.sma_50 - f50.sma_50) / NULLIF(f50.sma_50, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS sma_50_is_critical,

                            p.sma_100 AS sma_100_polygon,
                            f100.sma_100 AS sma_100_calculated,
                            ABS((p.sma_100 - f100.sma_100) / NULLIF(f100.sma_100, 0)) * 100 AS sma_100_error_percent,
                            CASE WHEN ABS((p.sma_100 - f100.sma_100) / NULLIF(f100.sma_100, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS sma_100_is_critical,

                            p.sma_200 AS sma_200_polygon,
                            f200.sma_200 AS sma_200_calculated,
                            ABS((p.sma_200 - f200.sma_200) / NULLIF(f200.sma_200, 0)) * 100 AS sma_200_error_percent,
                            CASE WHEN ABS((p.sma_200 - f200.sma_200) / NULLIF(f200.sma_200, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS sma_200_is_critical,

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
                snowflake_handler.conn.commit()

            except Exception as e:
                print(f"Error processing ticker {ticker}: {str(e)}")
                continue

        # Close connection
        cursor.close()
        # load_handler.disconnect()

        print(f"SMA data processing completed for {ds}")

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def process_ema_data(**context):
        """
        Complete ema data processing including fetching from Polygon API and error analysis

        Args:
            ds: Execution date in YYYY-MM-DD format (from Airflow context)
        """
        ds = context['ds']
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
        snowflake_handler = SnowflakeTableHandler(project_dir=PATH_TO_DBT_PROJECT, profile_name="jaffle_shop")

        if not snowflake_handler.conn:
            snowflake_handler.connect()
        cursor = snowflake_handler.conn.cursor()

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
                            "apiKey": polygon_api_key,
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
                            CASE WHEN ABS((p.ema_10 - f10.ema_10) / NULLIF(f10.ema_10, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS ema_10_is_critical,

                            p.ema_20 AS ema_20_polygon,
                            f20.ema_20 AS ema_20_calculated,
                            ABS((p.ema_20 - f20.ema_20) / NULLIF(f20.ema_20, 0)) * 100 AS ema_20_error_percent,
                            CASE WHEN ABS((p.ema_20 - f20.ema_20) / NULLIF(f20.ema_20, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS ema_20_is_critical,

                            p.ema_50 AS ema_50_polygon,
                            f50.ema_50 AS ema_50_calculated,
                            ABS((p.ema_50 - f50.ema_50) / NULLIF(f50.ema_50, 0)) * 100 AS ema_50_error_percent,
                            CASE WHEN ABS((p.ema_50 - f50.ema_50) / NULLIF(f50.ema_50, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS ema_50_is_critical,

                            p.ema_100 AS ema_100_polygon,
                            f100.ema_100 AS ema_100_calculated,
                            ABS((p.ema_100 - f100.ema_100) / NULLIF(f100.ema_100, 0)) * 100 AS ema_100_error_percent,
                            CASE WHEN ABS((p.ema_100 - f100.ema_100) / NULLIF(f100.ema_100, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS ema_100_is_critical,

                            p.ema_200 AS ema_200_polygon,
                            f200.ema_200 AS ema_200_calculated,
                            ABS((p.ema_200 - f200.ema_200) / NULLIF(f200.ema_200, 0)) * 100 AS ema_200_error_percent,
                            CASE WHEN ABS((p.ema_200 - f200.ema_200) / NULLIF(f200.ema_200, 0)) * 100 > 0.5 THEN TRUE ELSE FALSE END AS ema_200_is_critical,

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
                snowflake_handler.conn.commit()

            except Exception as e:
                print(f"Error processing ticker {ticker}: {str(e)}")
                continue

        # Close connection
        cursor.close()
        # load_handler.disconnect()

        print(f"ema data processing completed for {ds}")




    # Just one DBT step
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config = RenderConfig(  # Use render_config instead
            select=["staging.stg_stock_daily_agg"],  # specify models here
    ),
    )

    dbt_graders = DbtTaskGroup(
        group_id="dbt_graders",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(  # Use render_config instead
            select=["staging.stg_stock_graders"],  # specify models here
        ),
    )

    dbt_earnings = DbtTaskGroup(
        group_id="dbt_earnings",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(  # Use render_config instead
            select=["staging.stg_earnings_calendar"],  # specify models here
        ),
    )

    # Intermediate Models - First Layer
    int_metrics = DbtTaskGroup(
        group_id="int_metrics",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["Intermediate.int_daily_metrics"],
        ),
    )

    int_earnings = DbtTaskGroup(
        group_id="int_earnings",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["Intermediate.int_earnings_surprises"],
        ),
    )

    int_agg = DbtTaskGroup(
        group_id="int_agg",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["Intermediate.int_cumulated_daily_aggregation","test_type:singular"],
        ),
    )

    mart_candlestick = DbtTaskGroup(
        group_id="mart_candlestick",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.mart_candlestick_flexible"],
        ),
    )

    mart_ticker_classifier = DbtTaskGroup(
        group_id="mart_ticker_classifier",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.dim_ticker_classifier"],
        ),
    )

    mart_ticker_final = DbtTaskGroup(
        group_id="mart_ticker_final",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.dim_ticker_final"],
        ),
    )

    mart_macd_tracker = DbtTaskGroup(
        group_id="mart_macd_tracker",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.mart_macd"],
        ),
    )

    mart_earnings = DbtTaskGroup(
        group_id="mart_earnings",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.mart_earning_surprises"],
        ),
    )

    mart_graders = DbtTaskGroup(
        group_id="mart_graders",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.mart_stock_graders"],
        ),
    )

    mart_sma_tracker = DbtTaskGroup(
        group_id="mart_sma_tracker",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.mart_sma_flexible"],
        ),
    )

    mart_wma_tracker = DbtTaskGroup(
        group_id="mart_wma_tracker",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.mart_wma_flexible"],
        ),
    )



    mart_ema_tracker = DbtTaskGroup(
        group_id="mart_ema_tracker",
        project_config=ProjectConfig(dbt_project_path=PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # Path to dbt executable
        ),
        render_config=RenderConfig(
            select=["marts.mart_ema_flexible"],
        ),
    )




    # dependency chain
    fetch_and_load_stock_data() >> fetch_and_load_news() >> fetch_and_load_earning_calendar() >> fetch_and_load_graders_data() >> dbt_transform >> dbt_graders >> dbt_earnings >> int_metrics >> int_agg >> int_earnings >> mart_candlestick >> mart_ticker_classifier >> mart_ticker_final >> mart_macd_tracker >> mart_earnings >> mart_graders >> mart_sma_tracker >> mart_wma_tracker >> mart_ema_tracker >> process_sma_data() >> process_ema_data()


stock_metrics_pipeline()