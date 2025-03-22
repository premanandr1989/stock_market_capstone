import finnhub, time
from snowflake_config_handler import SnowflakeTableHandler

from pathlib import Path

finnhub_client = finnhub.Client(api_key="")

# print(finnhub_client.earnings_calendar(_from="", to="", symbol="AAPL"))

# print(finnhub_client.company_earnings('COST', limit=1))
def fetch_earning_surprises(tickers,filter_columns=None):
    """
    Fetch finnhub earning calendar data from the finnhub API endpoint.
    """
    tickers_earning_surprises = []
    total_tickers = len(tickers)
    for i,ticker in enumerate(tickers):
        try:
            data = finnhub_client.company_earnings(f'{ticker}', limit=1)

            if data:
                # If filter_columns is provided, filter each result dictionary
                if filter_columns:
                    filtered_results = [
                        {col: item[col] for col in filter_columns if col in item}
                        for item in data
                    ]
                    tickers_earning_surprises.extend(filtered_results)
                else:
                    tickers_earning_surprises.extend(data)
            print(f"data for {ticker} is fetched, {i+1}/{total_tickers} completed and {len(data)} no of records was fetched. ")
        except Exception as e:
            print(f"Exception occurred for ticker {ticker}: {e}")
        time.sleep(1)

    return tickers_earning_surprises

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

    cur_exc.execute("SELECT ticker FROM dim_ticker_classifier")
    ticker_tuples = cur_exc.fetchall()  # Returns a list of tuples

    tickers = [ticker[0] for ticker in ticker_tuples]

    print(f"{len(tickers)} no of tickers fetched and now moving onto fetching earning surprises")
    return tickers

    # if "earningsCalendar" in data:
    #     # If filter_columns is provided, filter each result dictionary
    #     if filter_columns:
    #         filtered_results = [
    #             {col: item[col] for col in filter_columns if col in item}
    #             for item in data["earningsCalendar"]
    #         ]
    #         tickers_earnings.extend(filtered_results)
    #     else:
    #         tickers_earnings.extend(data["earningsCalendar"])
    #
    # return tickers_earnings

def load_news_to_snowflake(tickers_earning_surprises):
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
    CREATE TABLE IF NOT EXISTS earnings_surprises cluster by (ticker,date) (
        ticker STRING,
        year INTEGER,
        date DATE,
        quarter INTEGER,
        actual DOUBLE,
        estimate DOUBLE,
        surprise DOUBLE,
        surprisePercent DOUBLE
    )
    """
    cur_exc.execute(create_table_sql)

    # Prepare the INSERT statement.
    insert_sql = """
    INSERT INTO earnings_surprises (ticker, year, date, quarter, actual, estimate, surprise, surprisePercent)
    VALUES (%(ticker)s, %(year)s, TO_DATE(%(date)s), %(quarter)s, %(actual)s, %(estimate)s, %(surprise)s, %(surprisePercent)s)
    """

    for result in tickers_earning_surprises:
        # Format the record as needed for your Iceberg table
        record = {
            'ticker': result.get('symbol'),
            'year': result.get('year', None),  # Adding None as a default in case there are missing keys.
            'date': result.get('period', None),
            'quarter': result.get('quarter', None),
            'actual': result.get('actual', None),
            'estimate': result.get('estimate', None),
            'surprise': result.get('surprise', None),
            'surprisePercent': result.get('surprisePercent', None),
        }
        records.append(record)

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
    ticker_earnings_surprises_historic = fetch_earning_surprises(tickers)
    if not ticker_earnings_surprises_historic:
        print("No earnings data was fetched.")
        return
    load_news_to_snowflake(ticker_earnings_surprises_historic)


if __name__ == '__main__':
    main()



