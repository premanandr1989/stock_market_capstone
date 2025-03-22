
import requests
from snowflake_config_handler import SnowflakeTableHandler

from pathlib import Path

BASE_URL = "https://financialmodelingprep.com/stable/grades"
api_key = ""

# params = {
#             "symbol": "WELL",
#             "apikey": api_key
#             }
# response = requests.get(BASE_URL, params=params)
# data = response.json()
# print(data)


def fetch_tickers(filter_columns=None):
    """
    Fetch finnhub earning calendar data from the finnhub API endpoint.
    """
    current_dir = Path(__file__).parent
    dbt_project_path = current_dir.parent / "dbt_capstone"

    load_handler = SnowflakeTableHandler(
        project_dir=dbt_project_path,
        profile_name="jaffle_shop"
    )

    if not load_handler.conn:
        load_handler.connect()
    cur_exc = load_handler.conn.cursor()

    # data = finnhub_client.company_earnings('NVDA', limit=1)

    cur_exc.execute("SELECT ticker FROM dim_ticker_classifier where ticker not in (Select distinct ticker from stock_graders) and ticker not in ('GOOG ', 'AVGO', 'BRK.B', 'LLY', 'MA', 'ORCL', 'PG', 'CRM', 'TMUS', 'ACN', 'IBM', 'PM', 'ABT', 'MS', 'MCD', 'AXP', 'LIN', 'ISRG', 'NOW', 'TMO', 'QCOM', 'APP', 'CAT', 'BKNG', 'TXN', 'SPGI', 'INTU', 'RTX', 'AMGN', 'BSX', 'PGR', 'UNP', 'BLK', 'DHR', 'SYK', 'SCHW', 'LOW', 'AMAT', 'NEE', 'TJX', 'ANET', 'CMCSA', 'FI', 'HON', 'DE', 'GILD', 'BX', 'ADP', 'KKR', 'COP') and overall_rank>=7695 order by overall_rank limit 1")
    ticker_tuples = cur_exc.fetchall()  # Returns a list of tuples

    tickers = [ticker[0] for ticker in ticker_tuples]

    print(f"{len(tickers)} no of tickers fetched and now moving onto fetching earning surprises")
    return tickers

def fetch_stock_grades(tickers,filter_columns=None):
    """
    Fetch finnhub earning calendar data from the finnhub API endpoint.
    """
    stock_grades = []
    url = BASE_URL
    total_tickers = len(tickers)
    for i,ticker in enumerate(tickers):
        try:
            params = {
                "symbol": ticker,
                "apikey": api_key
            }
            response = requests.get(url, params=params)
            data = response.json()
            print(data)

            if data.ErrorMessage.contains('Limit Reach'):
                print("Rate Limit exhausted, returning...")
                return



            if data:
                # If filter_columns is provided, filter each result dictionary
                if filter_columns:
                    filtered_results = [
                        {col: item[col] for col in filter_columns if col in item}
                        for item in data
                    ]
                    stock_grades.extend(filtered_results)
                else:
                    stock_grades.extend(data)
            print(f"data for {ticker} is fetched, {i+1}/{total_tickers} completed and {len(data)} no of records was fetched. ")

        # except requests.exceptions.HTTPError as http_err:
        #     if response.status_code == 429:  # Rate limit exceeded
        #         print(f"Rate limit exceeded for ticker {ticker}. Skipping...")
        #         # DO NOT raise an exception here; just log and continue.
        #         continue
        except Exception as e:
            print(f"Exception occurred for ticker {ticker}: {e}")

    return stock_grades

def load_news_to_snowflake(stock_grades):
    """
    Connects to Snowflake, creates the target table if it doesn't exist,
    and loads the news items into the table.
    """
    # Establish a connection to Snowflake.
    current_dir = Path(__file__).parent
    dbt_project_path = current_dir.parent / "dbt_capstone"

    load_handler = SnowflakeTableHandler(
        project_dir=dbt_project_path,
        profile_name="jaffle_shop"
    )
    records = []

    if not load_handler.conn:
        load_handler.connect()
    cur_exc = load_handler.conn.cursor()

    # Create the table if it doesn't exist.
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

    # Prepare the INSERT statement.
    insert_sql = """
    INSERT INTO stock_graders (ticker, date, gradingCompany, previousGrade, newGrade, action)
    VALUES (%(ticker)s, TO_DATE(%(date)s), %(gradingCompany)s, %(previousGrade)s, %(newGrade)s, %(action)s)
    """

    for result in stock_grades:
        # Format the record as needed for your Iceberg table
        record = {
            'ticker': result.get('symbol'),  # Adding None as a default in case there are missing keys.
            'date': result.get('date', None),
            'gradingCompany': result.get('gradingCompany', None),
            'previousGrade': result.get('previousGrade', None),
            'newGrade': result.get('newGrade', None),
            'action': result.get('action', None)
        }
        records.append(record)

    # Transform news items.

    # Insert data in bulk.
    cur_exc.executemany(insert_sql, records)
    load_handler.conn.commit()

    cur_exc.close()
    load_handler.close()
    print("earnings surprises data loaded into Snowflake successfully.")

def main():
    # tickers_earnings = fetch_earning_calendar()
    # if not tickers_earnings:
    #     print("No earnings data was fetched.")
    #     return
    # load_news_to_snowflake(tickers_earnings)
    tickers = fetch_tickers()
    stock_grades = fetch_stock_grades(tickers)
    if not stock_grades:
        print("No grades data was fetched.")
        return
    load_news_to_snowflake(stock_grades)


if __name__ == '__main__':
    main()