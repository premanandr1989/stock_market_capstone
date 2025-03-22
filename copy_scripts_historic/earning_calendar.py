import finnhub, time
from snowflake_config_handler import SnowflakeTableHandler

from pathlib import Path

finnhub_client = finnhub.Client(api_key="")

# print(finnhub_client.earnings_calendar(_from="2025-03-13", to="2025-03-13",symbol=""))

# print(finnhub_client.company_earnings('COST', limit=1))
def fetch_earning_calendar(filter_columns=None):
    """
    Fetch finnhub earning calendar data from the finnhub API endpoint.
    """
    tickers_earnings = []
    try:
        data = finnhub_client.earnings_calendar(_from="2025-03-14", to="2025-03-14",symbol="")

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
    except Exception as e:
        print(f"Exception occurred {e}")

    return tickers_earnings

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

    # delete_sql = f"""
    # DELETE FROM earnings_calendar where date = {ds}
    # """

    for result in tickers_earning_surprises:
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

    # Ensure deletes for idempotency
    # cur_exc.execute(delete_sql)


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

    ticker_earnings_calendar = fetch_earning_calendar()
    if not ticker_earnings_calendar:
        print("No earnings data was fetched.")
        return
    for record in ticker_earnings_calendar:
        print(record)
    # load_news_to_snowflake(ticker_earnings_calendar)


if __name__ == '__main__':
    main()



