from flask import Flask, jsonify, request
from flask_cors import CORS
from snowflake import connector
import os
from dotenv import load_dotenv
from datetime import datetime
from snowflake_config_handler import SnowflakeTableHandler
from pathlib import Path
import logging
# Add this at the top of your app.py file, after your existing imports
import socket
import time
from functools import wraps

# Set global socket timeout
socket.setdefaulttimeout(600)  # 2 minutes

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# def get_snowflake_connection(handler):
#     """
#     Get a Snowflake connection with proper timeout settings
#     """
#     try:
#         # Try to close any existing connection first to ensure clean state
#         if hasattr(handler, 'conn') and handler.conn:
#             try:
#                 handler.conn.close()
#             except:
#                 pass
#
#         # Connect with explicit timeout parameters
#         # These parameters need to be supported by your connect method
#         # Modify as needed based on your actual SnowflakeTableHandler implementation
#         handler.connect()
#
#         # After connecting, directly set timeout on connection object
#         if hasattr(handler, 'conn') and handler.conn:
#             # Try to access the internal connection properties
#             if hasattr(handler.conn, '_connection'):
#                 # Set socket_timeout directly
#                 handler.conn._connection.socket_timeout = 600
#
#             # Test connection with a simple query
#             cursor = handler.conn.cursor()
#             cursor.execute("SELECT 1")
#             cursor.fetchone()
#             cursor.close()
#
#         return handler.conn
#     except Exception as e:
#         logger.error(f"Connection error: {str(e)}")
#         # If we get here, re-raise the exception
#         raise

current_dir = Path(__file__).parent
dbt_project_path = current_dir.parent / "dbt_capstone"

load_handler = SnowflakeTableHandler(
    project_dir=dbt_project_path,
    profile_name="jaffle_shop"
)

app = Flask(__name__)
CORS(app)

def get_time_filter(conn):
    try:
        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()
        cur.execute("""
            SELECT MAX(date) 
            FROM int_daily_metrics
        """)
        max_date = cur.fetchone()[0]

        if not max_date:
            return ""

        return {
            'last_5_days': f"AND date >= DATEADD(day, -5, '{max_date}')",
            'mtd': f"AND YEAR(date) = YEAR('{max_date}') AND MONTH(date) = MONTH('{max_date}')",
            'last_3_months': f"AND date >= DATEADD(month, -3, '{max_date}')",
            'last_6_months': f"AND date >= DATEADD(month, -6, '{max_date}')",
            'last_1_year': f"AND date >= DATEADD(year, -1, '{max_date}')",
            'last_4_years': f"AND date >= DATEADD(year, -4, '{max_date}')",
            'ytd': f"AND YEAR(date) = YEAR('{max_date}')"
        }

    except Exception as e:
        logger.error(f"Error in get_time_filter: {str(e)}")
        if "socket timeout" in str(e).lower():
            return jsonify({'error': 'Request timed out. Please try again.'}), 504
        return jsonify({'error': str(e)}), 500


# Existing market cap endpoint
@app.route('/market-cap-categories')
def get_market_cap_categories():
    try:
        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT market_cap_category 
            FROM dim_ticker_classifier
            ORDER BY 
            CASE market_cap_category
                WHEN 'Mega Cap' THEN 1
                WHEN 'Large Cap' THEN 2
                WHEN 'Mid Cap' THEN 3
                WHEN 'Small Cap' THEN 4
                WHEN 'Micro Cap' THEN 5
            END
        """)

        categories = [row[0] for row in cur.fetchall()]
        # cur.close()
        return jsonify(categories)
    except Exception as e:
        logger.error(f"Error fetching market cap categories: {str(e)}")
        if "socket timeout" in str(e).lower():
            return jsonify({'error': 'Request timed out. Please try again.'}), 504
        return jsonify({'error': str(e)}), 500


# New endpoint for tickers based on market cap
@app.route('/tickers')
def get_tickers():
    try:
        market_cap = request.args.get('market_cap_category', 'Mega Cap')

        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        if market_cap:
            cur.execute("""
                SELECT ticker, name 
                FROM dim_ticker_classifier
                WHERE market_cap_category = %s
                ORDER BY liquidity_rank_in_category
            """, (market_cap,))
        else:
            cur.execute("""
                SELECT ticker, name 
                FROM dim_ticker_classifier
                ORDER BY ticker
            """)

        tickers = [{"text": f"{row[0]} - {row[1]}", "value": row[0]} for row in cur.fetchall()]
        # cur.close()
        return jsonify(tickers)
    except Exception as e:
        logger.error(f"Error fetching tickers: {str(e)}")
        if "socket timeout" in str(e).lower():
            return jsonify({'error': 'Request timed out. Please try again.'}), 504
        return jsonify({'error': str(e)}), 500


@app.route('/candlestick')
def get_candlestick_data():
    logger.debug("Candlestick endpoint called")
    conn = None
    cur = None
    try:
        # Get parameters
        ticker = request.args.get('ticker', '')
        time_period = request.args.get('time_period', 'last_5_days')
        aggregation = request.args.get('aggregation', 'daily')
        market_cap_category = request.args.get('market_cap_category', None)

        logger.debug(
            f"Parameters received: ticker={ticker}, time_period={time_period}, aggregation={aggregation}, market_cap_category={market_cap_category}")

        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400


        ticker = request.args.get('ticker', '')
        logger.debug(f"Received request for ticker: {ticker}")

        # Create connection
        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        time_filters = get_time_filter(conn)
        time_filter = time_filters.get(time_period, "")



        if aggregation == 'daily':
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                unpacked_data AS (
                    SELECT 
                        c.ticker,
                        CAST(f.value:date::string AS DATE) as date,
                        CAST(f.value:open::string as float) as open,
                        CAST(f.value:high::string as float) as high,
                        CAST(f.value:low::string as float) as low,
                        CAST(f.value:close::string as float) as close,
                        CAST(f.value:volume::string as float) as volume
                    FROM mart_candlestick_flexible c,
                    LATERAL FLATTEN(input => composite_col) f
                    WHERE ticker = %(ticker)s
                    {time_filter}
                ),
                joined_data as (
                    Select u.ticker,
                            u.date as time,
                            u.open,
                            u.high,
                            u.low,
                            u.close,
                            u.volume, 
                            m.market_cap_category 
                    from unpacked_data u 
                    INNER JOIN market_cap_filter m on u.ticker = m.ticker
                )
                Select * from joined_data
                ORDER BY time
            """
        else:
            date_trunc = 'week' if aggregation == 'weekly' else 'month'
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                unpacked_data AS (
                    SELECT 
                        c.ticker,
                        CAST(f.value:date::string AS DATE) as date,
                        CAST(f.value:open::string as float) as open,
                        CAST(f.value:high::string as float) as high,
                        CAST(f.value:low::string as float) as low,
                        CAST(f.value:close::string as float) as close,
                        CAST(f.value:volume::string as float) as volume
                    FROM mart_candlestick_flexible c,
                    LATERAL FLATTEN(input => composite_col) f
                    WHERE ticker = %(ticker)s
                    {time_filter}
                ),
                joined_data as (
                    Select u.*, m.market_cap_category 
                    from unpacked_data u 
                    INNER JOIN market_cap_filter m on u.ticker = m.ticker
                    
                )
                SELECT 
                    DATE_TRUNC('{date_trunc}', date) as time,
                    FIRST_VALUE(open) OVER (PARTITION BY DATE_TRUNC('{date_trunc}', date) ORDER BY date) as open,
                    MAX(high) OVER (PARTITION BY DATE_TRUNC('{date_trunc}', date)) as high,
                    MIN(low) OVER (PARTITION BY DATE_TRUNC('{date_trunc}', date)) as low,
                    LAST_VALUE(close) OVER (PARTITION BY DATE_TRUNC('{date_trunc}', date) ORDER BY date) as close,
                    SUM(volume) OVER (PARTITION BY DATE_TRUNC('{date_trunc}', date)) as volume
                FROM joined_data
                QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{date_trunc}', date) ORDER BY date) = 1
                ORDER BY time
            """

        market_cap_filter = f"AND market_cap_category = '{market_cap_category}'" if market_cap_category else ""

        query = base_query.format(
            time_filter=time_filter,
            date_trunc=date_trunc if aggregation != 'daily' else '',
            market_cap_filter=market_cap_filter
        )

        # Log the final query and parameters
        logger.debug("\n=== Query Parameters ===")
        logger.debug(f"Time Filter: {time_filter}")
        logger.debug(f"Market Cap Filter: {market_cap_filter}")
        logger.debug("\n=== Final Query ===")
        logger.debug(query)
        logger.debug(f"Parameters: {{'ticker': {ticker}}}")

        # Execute query
        cur.execute(query, {'ticker': ticker})
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in results]

        data = [{k.lower(): v for k, v in item.items()} for item in data]

        return jsonify(data)

    except Exception as e:
        logger.error(f"Error in candlestick endpoint: {str(e)}")
        if "socket timeout" in str(e).lower():
            return jsonify({'error': 'Request timed out. Please try again.'}), 504
        return jsonify({'error': str(e)}), 500


@app.route('/moving-averages/ema')
def get_ema_data():
    logger.debug("EMA endpoint called")
    try:
        ticker = request.args.get('ticker', '')
        from_date = request.args.get('from', '')
        to_date = request.args.get('to', '')
        aggregation = request.args.get('aggregation', 'daily')
        market_cap_category = request.args.get('market_cap_category', None)
        sma_period = request.args.get('sma_period', '20')

        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400

        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        if aggregation == 'daily':
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                base_data AS (
                    SELECT 
                        m.*,
                        mcf.market_cap_category,
                        LAG(close) OVER (ORDER BY date) as prev_close,
                        LAG(ema_{sma_period}) OVER (ORDER BY date) as prev_ema
                    FROM mart_ema_flexible m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                )
                SELECT 
                    ticker,
                    date,
                    close,
                    ema_{sma_period} as ema,
                    market_cap_category,
                    CASE 
                        WHEN close > ema_{sma_period} AND prev_close <= prev_ema THEN 'BUY'
                        WHEN close < ema_{sma_period} AND prev_close >= prev_ema THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN close > ema_{sma_period} AND prev_close <= prev_ema THEN close
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN close < ema_{sma_period} AND prev_close >= prev_ema THEN close
                        ELSE NULL
                    END as sell_signal
                FROM base_data
                ORDER BY date
            """
        else:
            date_trunc = 'week' if aggregation == 'weekly' else 'month'
            base_query = """
                            WITH market_cap_filter AS (
                                SELECT ticker, market_cap_category  
                                FROM dim_ticker_classifier
                                WHERE ticker = %(ticker)s
                                {market_cap_filter}
                            ),
                            aggregated_data AS (
                                SELECT 
                                    m.ticker,
                                    DATE_TRUNC('{date_trunc}', date) as period_date,
                                    EXTRACT(YEAR FROM date) as year,
                                    EXTRACT({date_trunc} FROM date) as period_number,
                                    LAST_VALUE(close) OVER (
                                        PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                                        ORDER BY date
                                    ) as close,
                                    LAST_VALUE(ema_{sma_period}) OVER (
                                        PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                                        ORDER BY date
                                    ) as ema,
                                    mcf.market_cap_category
                                FROM mart_ema_flexible m
                                INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                                WHERE m.ticker = %(ticker)s
                                AND date BETWEEN %(from_date)s AND %(to_date)s
                                QUALIFY ROW_NUMBER() OVER (
                                    PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                                    ORDER BY date DESC
                                ) = 1
                            ),
                            signals AS (
                                SELECT 
                                    ticker,
                                    period_date as date,
                                    close,
                                    ema,
                                    market_cap_category,
                                    LAG(close) OVER (ORDER BY year, period_number) as prev_close,
                                    LAG(ema) OVER (ORDER BY year, period_number) as prev_ema
                                FROM aggregated_data
                            )
                            SELECT 
                                ticker,
                                date,
                                close,
                                ema,
                                market_cap_category,
                                CASE 
                                    WHEN close > ema AND prev_close <= prev_ema THEN 'BUY'
                                    WHEN close < ema AND prev_close >= prev_ema THEN 'SELL'
                                    ELSE NULL
                                END as signal,
                                CASE 
                                    WHEN close > ema AND prev_close <= prev_ema THEN close
                                    ELSE NULL
                                END as buy_signal,
                                CASE 
                                    WHEN close < ema AND prev_close >= prev_ema THEN close
                                    ELSE NULL
                                END as sell_signal
                            FROM signals
                            ORDER BY date
                        """

        market_cap_filter = f"AND market_cap_category = '{market_cap_category}'" if market_cap_category else ""

        query = base_query.format(
            date_trunc=date_trunc if aggregation != 'daily' else '',
            market_cap_filter=market_cap_filter,
            sma_period=sma_period
        )

        params = {
            'ticker': ticker,
            'from_date': from_date,
            'to_date': to_date
        }

        logger.debug("\n=== Query Parameters ===")
        logger.debug(f"Market Cap Filter: {market_cap_filter}")
        logger.debug("\n=== Final Query ===")
        logger.debug(query)
        logger.debug(f"Parameters: {{'ticker': {ticker}}}")

        cur.execute(query, params)

        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in results]
        data = [{k.lower(): v for k, v in item.items()} for item in data]

        return jsonify(data)

    except Exception as e:
        logger.error(f"Error in EMA endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/moving-averages/sma')
def get_sma_data():
    logger.debug("SMA endpoint called")
    try:
        ticker = request.args.get('ticker', '')
        from_date = request.args.get('from', '')
        to_date = request.args.get('to', '')
        aggregation = request.args.get('aggregation', 'daily')
        market_cap_category = request.args.get('market_cap_category', None)
        sma_period = request.args.get('sma_period', '20')

        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400

        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        if aggregation == 'daily':
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                base_data AS (
                    SELECT 
                        m.*,
                        mcf.market_cap_category,
                        LAG(close) OVER (ORDER BY date) as prev_close,
                        LAG(sma_{sma_period}) OVER (ORDER BY date) as prev_sma
                    FROM mart_sma_flexible m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                )
                SELECT 
                    ticker,
                    date,
                    close,
                    sma_{sma_period} as sma,
                    market_cap_category,
                    CASE 
                        WHEN close > sma_{sma_period} AND prev_close <= prev_sma THEN 'BUY'
                        WHEN close < sma_{sma_period} AND prev_close >= prev_sma THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN close > sma_{sma_period} AND prev_close <= prev_sma THEN close
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN close < sma_{sma_period} AND prev_close >= prev_sma THEN close
                        ELSE NULL
                    END as sell_signal
                FROM base_data
                ORDER BY date
            """
        else:
            date_trunc = 'week' if aggregation == 'weekly' else 'month'
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                aggregated_data AS (
                    SELECT 
                        m.ticker,
                        DATE_TRUNC('{date_trunc}', date) as period_date,
                        EXTRACT(YEAR FROM date) as year,
                        EXTRACT({date_trunc} FROM date) as period_number,
                        LAST_VALUE(close) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                            ORDER BY date
                        ) as close,
                        LAST_VALUE(sma_{sma_period}) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                            ORDER BY date
                        ) as sma,
                        mcf.market_cap_category
                    FROM mart_sma_flexible m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                        ORDER BY date DESC
                    ) = 1
                ),
                signals AS (
                    SELECT 
                        ticker,
                        period_date as date,
                        close,
                        sma,
                        market_cap_category,
                        LAG(close) OVER (ORDER BY year, period_number) as prev_close,
                        LAG(sma) OVER (ORDER BY year, period_number) as prev_sma
                    FROM aggregated_data
                )
                SELECT 
                    ticker,
                    date,
                    close,
                    sma,
                    market_cap_category,
                    CASE 
                        WHEN close > sma AND prev_close <= prev_sma THEN 'BUY'
                        WHEN close < sma AND prev_close >= prev_sma THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN close > sma AND prev_close <= prev_sma THEN close
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN close < sma AND prev_close >= prev_sma THEN close
                        ELSE NULL
                    END as sell_signal
                FROM signals
                ORDER BY date
            """

        market_cap_filter = f"AND market_cap_category = '{market_cap_category}'" if market_cap_category else ""

        query = base_query.format(
            date_trunc=date_trunc if aggregation != 'daily' else '',
            market_cap_filter=market_cap_filter,
            sma_period=sma_period
        )

        params = {
            'ticker': ticker,
            'from_date': from_date,
            'to_date': to_date
        }
        cur.execute(query, params)

        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in results]
        data = [{k.lower(): v for k, v in item.items()} for item in data]

        return jsonify(data)

    except Exception as e:
        logger.error(f"Error in SMA endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/moving-averages/wma')
def get_wma_data():
    logger.debug("SMA endpoint called")
    try:
        ticker = request.args.get('ticker', '')
        from_date = request.args.get('from', '')
        to_date = request.args.get('to', '')
        aggregation = request.args.get('aggregation', 'daily')
        market_cap_category = request.args.get('market_cap_category', None)
        sma_period = request.args.get('sma_period', '20')

        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400

        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        if aggregation == 'daily':
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                base_data AS (
                    SELECT 
                        m.*,
                        mcf.market_cap_category,
                        LAG(close) OVER (ORDER BY date) as prev_close,
                        LAG(wma_{sma_period}) OVER (ORDER BY date) as prev_wma
                    FROM mart_wma_flexible m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                )
                SELECT 
                    ticker,
                    date,
                    close,
                    wma_{sma_period} as wma,
                    market_cap_category,
                    CASE 
                        WHEN close > wma_{sma_period} AND prev_close <= prev_wma THEN 'BUY'
                        WHEN close < wma_{sma_period} AND prev_close >= prev_wma THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN close > wma_{sma_period} AND prev_close <= prev_wma THEN close
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN close < wma_{sma_period} AND prev_close >= prev_wma THEN close
                        ELSE NULL
                    END as sell_signal
                FROM base_data
                ORDER BY date
            """
        else:
            date_trunc = 'week' if aggregation == 'weekly' else 'month'
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                aggregated_data AS (
                    SELECT 
                        m.ticker,
                        DATE_TRUNC('{date_trunc}', date) as period_date,
                        EXTRACT(YEAR FROM date) as year,
                        EXTRACT({date_trunc} FROM date) as period_number,
                        LAST_VALUE(close) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                            ORDER BY date
                        ) as close,
                        LAST_VALUE(wma_{sma_period}) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                            ORDER BY date
                        ) as wma,
                        mcf.market_cap_category
                    FROM mart_wma_flexible m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY DATE_TRUNC('{date_trunc}', date), EXTRACT(YEAR FROM date)
                        ORDER BY date DESC
                    ) = 1
                ),
                signals AS (
                    SELECT 
                        ticker,
                        period_date as date,
                        close,
                        wma,
                        market_cap_category,
                        LAG(close) OVER (ORDER BY year, period_number) as prev_close,
                        LAG(wma) OVER (ORDER BY year, period_number) as prev_wma
                    FROM aggregated_data
                )
                SELECT 
                    ticker,
                    date,
                    close,
                    wma,
                    market_cap_category,
                    CASE 
                        WHEN close > wma AND prev_close <= prev_wma THEN 'BUY'
                        WHEN close < wma AND prev_close >= prev_wma THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN close > wma AND prev_close <= prev_wma THEN close
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN close < wma AND prev_close >= prev_wma THEN close
                        ELSE NULL
                    END as sell_signal
                FROM signals
                ORDER BY date
            """

        market_cap_filter = f"AND market_cap_category = '{market_cap_category}'" if market_cap_category else ""

        query = base_query.format(
            date_trunc=date_trunc if aggregation != 'daily' else '',
            market_cap_filter=market_cap_filter,
            sma_period=sma_period
        )

        params = {
            'ticker': ticker,
            'from_date': from_date,
            'to_date': to_date
        }
        cur.execute(query, params)

        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in results]
        data = [{k.lower(): v for k, v in item.items()} for item in data]

        return jsonify(data)

    except Exception as e:
        logger.error(f"Error in WMA endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/moving-averages/bollinger')
def get_bollinger_data():
    logger.debug("Bollinger Bands endpoint called")
    try:
        ticker = request.args.get('ticker', '')
        from_date = request.args.get('from', '')
        to_date = request.args.get('to', '')
        aggregation = request.args.get('aggregation', 'daily')
        market_cap_category = request.args.get('market_cap_category', None)
        sma_period = int(request.args.get('sma_period', '20'))
        std_dev = float(request.args.get('std_dev', '2'))

        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400

        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        if aggregation == 'daily':
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                base_data AS (
                    SELECT 
                        m.*,
                        mcf.market_cap_category,
                        sma_{sma_period} as middle_band,
                        STDDEV(close) OVER (
                            ORDER BY date 
                            ROWS BETWEEN {sma_period} PRECEDING AND CURRENT ROW
                        ) * {std_dev} as std_dev_band,
                        LAG(close) OVER (ORDER BY date) as prev_close
                    FROM mart_sma_flexible m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                ),
                signals_data AS (
                    SELECT 
                        *,
                        (middle_band + std_dev_band) as upper_band,
                        (middle_band - std_dev_band) as lower_band,
                        LAG(middle_band + std_dev_band) OVER (ORDER BY date) as prev_upper_band,
                        LAG(middle_band - std_dev_band) OVER (ORDER BY date) as prev_lower_band
                    FROM base_data
                )
                SELECT 
                    ticker,
                    date,
                    CAST(close AS DECIMAL(20,2)) as close,
                    CAST(middle_band AS DECIMAL(20,2)) as middle_band,
                    CAST(upper_band AS DECIMAL(20,2)) as upper_band,
                    CAST(lower_band AS DECIMAL(20,2)) as lower_band,
                    market_cap_category,
                    CASE 
                        WHEN close < lower_band AND prev_close >= prev_lower_band THEN 'BUY'
                        WHEN close > upper_band AND prev_close <= prev_upper_band THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN close < lower_band AND prev_close >= prev_lower_band THEN CAST(close AS DECIMAL(20,2))
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN close > upper_band AND prev_close <= prev_upper_band THEN CAST(close AS DECIMAL(20,2))
                        ELSE NULL
                    END as sell_signal
                FROM signals_data
                ORDER BY date
            """
        else:
            date_trunc = 'week' if aggregation == 'weekly' else 'month'
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                period_data AS (
                    SELECT 
                        m.ticker,
                        mcf.market_cap_category,
                        DATE_TRUNC('{date_trunc}', date) as period_date,
                        LAST_VALUE(close) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date) 
                            ORDER BY date
                        ) as close,
                        LAST_VALUE(sma_{sma_period}) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date) 
                            ORDER BY date
                        ) as middle_band,
                        STDDEV(close) OVER (
                            ORDER BY DATE_TRUNC('{date_trunc}', date) 
                            ROWS BETWEEN {sma_period} PRECEDING AND CURRENT ROW
                        ) * {std_dev} as std_dev_band
                    FROM mart_sma_flexible m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY DATE_TRUNC('{date_trunc}', date) 
                        ORDER BY date DESC
                    ) = 1
                ),
                signals AS (
                    SELECT 
                        ticker,
                        period_date as date,
                        close,
                        middle_band,
                        (middle_band + std_dev_band) as upper_band,
                        (middle_band - std_dev_band) as lower_band,
                        market_cap_category,
                        LAG(close) OVER (ORDER BY period_date) as prev_close,
                        LAG(middle_band + std_dev_band) OVER (ORDER BY period_date) as prev_upper_band,
                        LAG(middle_band - std_dev_band) OVER (ORDER BY period_date) as prev_lower_band
                    FROM period_data
                )
                SELECT 
                    ticker,
                    date,
                    CAST(close AS DECIMAL(20,2)) as close,
                    CAST(middle_band AS DECIMAL(20,2)) as middle_band,
                    CAST(upper_band AS DECIMAL(20,2)) as upper_band,
                    CAST(lower_band AS DECIMAL(20,2)) as lower_band,
                    market_cap_category,
                    CASE 
                        WHEN close < lower_band AND prev_close >= prev_lower_band THEN 'BUY'
                        WHEN close > upper_band AND prev_close <= prev_upper_band THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN close < lower_band AND prev_close >= prev_lower_band THEN CAST(close AS DECIMAL(20,2))
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN close > upper_band AND prev_close <= prev_upper_band THEN CAST(close AS DECIMAL(20,2))
                        ELSE NULL
                    END as sell_signal
                FROM signals
                ORDER BY date
            """

        market_cap_filter = f"AND market_cap_category = '{market_cap_category}'" if market_cap_category else ""

        query = base_query.format(
            date_trunc=date_trunc if aggregation != 'daily' else '',
            market_cap_filter=market_cap_filter,
            sma_period=sma_period,
            std_dev=std_dev
        )

        params = {
            'ticker': ticker,
            'from_date': from_date,
            'to_date': to_date
        }

        logger.debug("\n=== Query Parameters ===")
        logger.debug(f"Market Cap Filter: {market_cap_filter}")
        logger.debug("\n=== Final Query ===")
        logger.debug(query)
        logger.debug(f"Parameters: {{'ticker': {ticker}}}")

        cur.execute(query, params)

        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in results]
        data = [{k.lower(): v for k, v in item.items()} for item in data]

        return jsonify(data)

    except Exception as e:
        logger.error(f"Error in Bollinger Bands endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/moving-averages/macd')
def get_macd_data():
    logger.debug("MACD endpoint called")
    try:
        ticker = request.args.get('ticker', '')
        from_date = request.args.get('from', '')
        to_date = request.args.get('to', '')
        aggregation = request.args.get('aggregation', 'daily')
        market_cap_category = request.args.get('market_cap_category', 'Mega Cap')

        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400

        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        if aggregation == 'daily':
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                base_data AS (
                    SELECT 
                        m.*,
                        mcf.market_cap_category,
                        LAG(close) OVER (ORDER BY date) as prev_close
                    FROM mart_macd m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                )
                SELECT 
                    ticker,
                    date,
                    CAST(close AS DECIMAL(20,2)) as close,
                    CAST(macd_line AS DECIMAL(20,2)) as macd_line,
                    CAST(signal_line AS DECIMAL(20,2)) as signal_line,
                    CASE 
                        WHEN CAST(macd_histogram AS DECIMAL(20,2)) >=0 THEN macd_histogram
                    END as postive_histogram,
                    CASE 
                        WHEN CAST(macd_histogram AS DECIMAL(20,2)) <0 THEN macd_histogram
                    END as negative_histogram,
                    market_cap_category,
                    CASE 
                        WHEN macd_line > signal_line AND prev_macd_line <= prev_signal_line THEN 'BUY'
                        WHEN macd_line < signal_line AND prev_macd_line >= prev_signal_line THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN macd_line > signal_line AND prev_macd_line <= prev_signal_line THEN CAST(close AS DECIMAL(20,2))
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN macd_line < signal_line AND prev_macd_line >= prev_signal_line THEN CAST(close AS DECIMAL(20,2))
                        ELSE NULL
                    END as sell_signal
                FROM base_data
                ORDER BY date
            """
        else:
            date_trunc = 'week' if aggregation == 'weekly' else 'month'
            base_query = """
                WITH market_cap_filter AS (
                    SELECT ticker, market_cap_category  
                    FROM dim_ticker_classifier
                    WHERE ticker = %(ticker)s
                    {market_cap_filter}
                ),
                period_data AS (
                    SELECT 
                        m.ticker,
                        mcf.market_cap_category,
                        DATE_TRUNC('{date_trunc}', date) as period_date,
                        LAST_VALUE(close) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date) 
                            ORDER BY date
                        ) as close,
                        LAST_VALUE(macd_line) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date) 
                            ORDER BY date
                        ) as macd_line,
                        LAST_VALUE(signal_line) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date) 
                            ORDER BY date
                        ) as signal_line,
                        LAST_VALUE(macd_histogram) OVER (
                            PARTITION BY DATE_TRUNC('{date_trunc}', date) 
                            ORDER BY date
                        ) as macd_histogram
                    FROM mart_macd m
                    INNER JOIN market_cap_filter mcf ON m.ticker = mcf.ticker
                    WHERE m.ticker = %(ticker)s
                    AND date BETWEEN %(from_date)s AND %(to_date)s
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY DATE_TRUNC('{date_trunc}', date) 
                        ORDER BY date DESC
                    ) = 1
                ),
                signals AS (
                    SELECT 
                        ticker,
                        period_date as date,
                        close,
                        macd_line,
                        signal_line,
                        macd_histogram,
                        market_cap_category,
                        LAG(macd_line) OVER (ORDER BY period_date) as prev_macd_line,
                        LAG(signal_line) OVER (ORDER BY period_date) as prev_signal_line
                    FROM period_data
                )
                SELECT 
                    ticker,
                    date,
                    CAST(close AS DECIMAL(20,2)) as close,
                    CAST(macd_line AS DECIMAL(20,2)) as macd_line,
                    CAST(signal_line AS DECIMAL(20,2)) as signal_line,
                    CASE 
                        WHEN CAST(macd_histogram AS DECIMAL(20,2)) >=0 THEN macd_histogram
                    END as postive_histogram,
                    CASE 
                        WHEN CAST(macd_histogram AS DECIMAL(20,2)) <0 THEN macd_histogram
                    END as negative_histogram,
                    market_cap_category,
                    CASE 
                        WHEN macd_line > signal_line AND prev_macd_line <= prev_signal_line THEN 'BUY'
                        WHEN macd_line < signal_line AND prev_macd_line >= prev_signal_line THEN 'SELL'
                        ELSE NULL
                    END as signal,
                    CASE 
                        WHEN macd_line > signal_line AND prev_macd_line <= prev_signal_line THEN CAST(close AS DECIMAL(20,2))
                        ELSE NULL
                    END as buy_signal,
                    CASE 
                        WHEN macd_line < signal_line AND prev_macd_line >= prev_signal_line THEN CAST(close AS DECIMAL(20,2))
                        ELSE NULL
                    END as sell_signal
                FROM signals
                ORDER BY date
            """

        market_cap_filter = f"AND market_cap_category = '{market_cap_category}'" if market_cap_category else ""

        query = base_query.format(
            date_trunc=date_trunc if aggregation != 'daily' else '',
            market_cap_filter=market_cap_filter
        )

        params = {
            'ticker': ticker,
            'from_date': from_date,
            'to_date': to_date
        }

        logger.debug("\n=== Query Parameters ===")
        logger.debug(f"Market Cap Filter: {market_cap_filter}")
        logger.debug("\n=== Final Query ===")
        logger.debug(query)
        logger.debug(f"Parameters: {{'ticker': {ticker}}}")

        cur.execute(query, params)

        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in results]
        data = [{k.lower(): v for k, v in item.items()} for item in data]

        return jsonify(data)

    except Exception as e:
        logger.error(f"Error in MACD endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/market-dashboard')
def get_market_dashboard():
    logger.debug("Market Dashboard endpoint called")
    try:
        if not load_handler.conn:
            load_handler.connect()
        conn = load_handler.conn
        cur = conn.cursor()

        # 1. Get Market Overview Data (Bullish/Bearish indicator)
        cur.execute("""
            WITH ordered_market_data as (
            Select ticker,
                    composite_col,
                    record_count as current_record_count,
                    year,
                    month_num,
                    LAG(composite_col) over(partition by ticker order by year, month_num) as prev_composite_col,
                    LAG(record_count) over (partition by ticker order by year, month_num) as prev_record_count
            from int_cumulated_daily_aggregation
            ),
            market_classification as (
                SELECT ticker, 
                    market_cap_category, 
                    row_number() over(order by overall_rank) as rnk 
                FROM dim_ticker_classifier
            ),
            recent_market_data AS (
                SELECT 
                    ticker,
                    CASE 
                        WHEN composite_col IS NOT NULL AND current_record_count IS NOT NULL 
                             AND current_record_count > 0 
                        THEN composite_col[current_record_count-1]:"close"::float 
                        ELSE NULL 
                    END as close,
                    CASE 
                        WHEN current_record_count IS NOT NULL AND current_record_count > 1 
                             AND composite_col IS NOT NULL
                        THEN composite_col[current_record_count-2]:"close"::float
                        WHEN prev_composite_col IS NOT NULL AND prev_record_count IS NOT NULL 
                             AND prev_record_count > 0
                        THEN prev_composite_col[prev_record_count-1]:"close"::float 
                        ELSE NULL
                    END as prev_close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY year DESC, month_num DESC) as row_num
                FROM ordered_market_data
                WHERE ticker IN (Select ticker from market_classification where rnk <= 20) -- Major indices
                AND month_num IN (EXTRACT(month from CURRENT_DATE), EXTRACT(month from CURRENT_DATE)-1) and year = EXTRACT(year from CURRENT_DATE)
            )
            SELECT
                SUM(CASE 
                        WHEN close IS NOT NULL AND prev_close IS NOT NULL AND close > prev_close THEN 1 
                        WHEN close IS NOT NULL AND prev_close IS NOT NULL AND close < prev_close THEN -1 
                        ELSE 0 
                    END) as market_sentiment
            FROM recent_market_data
            WHERE row_num = 1
        """)
        market_sentiment_result = cur.fetchone()
        market_sentiment = "Bullish" if market_sentiment_result[0] > 0 else "Bearish"

        # 2. Get Leaders (Major index with best performance)
        cur.execute("""
            WITH ordered_market_data as (
            Select ticker,
                    composite_col,
                    record_count as current_record_count,
                    year,
                    month_num,
                    LAG(composite_col) over(partition by ticker order by year, month_num) as prev_composite_col,
                    LAG(record_count) over (partition by ticker order by year, month_num) as prev_record_count
            from int_cumulated_daily_aggregation
            ),
            market_classification as (
                SELECT ticker, 
                    market_cap_category, 
                    row_number() over(order by overall_rank) as rnk 
                FROM dim_ticker_classifier
            ),
            recent_market_data AS (
                SELECT 
                    ticker,
                    CASE 
                        WHEN composite_col IS NOT NULL AND current_record_count IS NOT NULL 
                             AND current_record_count > 0 
                        THEN composite_col[current_record_count-1]:"close"::float 
                        ELSE NULL 
                    END as close,
                    CASE 
                        WHEN current_record_count IS NOT NULL AND current_record_count > 1 
                             AND composite_col IS NOT NULL
                        THEN composite_col[current_record_count-2]:"close"::float
                        WHEN prev_composite_col IS NOT NULL AND prev_record_count IS NOT NULL 
                             AND prev_record_count > 0
                        THEN prev_composite_col[prev_record_count-1]:"close"::float 
                        ELSE NULL
                    END as prev_close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY year DESC, month_num DESC) as row_num
                FROM ordered_market_data
                WHERE ticker IN (Select ticker from market_classification where rnk <= 20) -- Major indices
                AND month_num IN (EXTRACT(month from CURRENT_DATE), EXTRACT(month from CURRENT_DATE)-1) and year = EXTRACT(year from CURRENT_DATE)
            ),
            ranked_indices AS (
                SELECT 
                    ticker,
                    close,
                    CASE 
                        WHEN close IS NOT NULL AND prev_close IS NOT NULL AND prev_close <> 0
                        THEN ((close - prev_close) / prev_close * 100) 
                        ELSE 0 
                    END as pct_change
                FROM recent_market_data
                WHERE row_num = 1
                AND close IS NOT NULL 
                AND prev_close IS NOT NULL
                AND prev_close <> 0
            )
            SELECT top 1 rd.*, dt.current_sentiment, dt.branding_logo_url, dt.branding_icon_url  FROM ranked_indices rd
            left join (Select * from (
                        Select ticker,
                        current_sentiment,
                        branding_logo_url, 
                        branding_icon_url, 
                        row_number() over(partition by ticker order by date desc) as rn
                        from dim_ticker_final
                        )
                        where rn = 1) dt on dt.ticker = rd.ticker
                        ORDER BY pct_change DESC
        """)
        leader_result = cur.fetchone()
        leader = {
            "ticker": leader_result[0] if leader_result and leader_result[0] is not None else "N/A",
            "price": leader_result[1] if leader_result and leader_result[1] is not None else 0,
            "change_pct": leader_result[2] if leader_result and leader_result[2] is not None else 0,
            "current_sentiment": leader_result[3] if leader_result and leader_result[3] is not None else "N/A",
            "branding_logo": leader_result[4] if leader_result and leader_result[4] is not None else "N/A",
            "branding_icon": leader_result[5] if leader_result and leader_result[5] is not None else "N/A"
        }

        # 3. Get Hottest and Worst Sectors
        cur.execute("""
            WITH ordered_market_data as (
            Select ticker,
                    composite_col,
                    record_count as current_record_count,
                    year,
                    month_num,
                    LAG(composite_col) over(partition by ticker order by year, month_num) as prev_composite_col,
                    LAG(record_count) over (partition by ticker order by year, month_num) as prev_record_count
            from int_cumulated_daily_aggregation
            ),
            market_classification as (
                SELECT ticker, 
                    market_cap_category, 
                    row_number() over(order by overall_rank) as rnk 
                FROM dim_ticker_classifier
            ),
            recent_market_data AS (
                SELECT 
                    ticker,
                    CASE 
                        WHEN composite_col IS NOT NULL AND current_record_count IS NOT NULL 
                             AND current_record_count > 0 
                        THEN composite_col[current_record_count-1]:"close"::float 
                        ELSE NULL 
                    END as close,
                    CASE 
                        WHEN current_record_count IS NOT NULL AND current_record_count > 1 
                             AND composite_col IS NOT NULL
                        THEN composite_col[current_record_count-2]:"close"::float
                        WHEN prev_composite_col IS NOT NULL AND prev_record_count IS NOT NULL 
                             AND prev_record_count > 0
                        THEN prev_composite_col[prev_record_count-1]:"close"::float 
                        ELSE NULL
                    END as prev_close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY year DESC, month_num DESC) as row_num
                FROM ordered_market_data
                WHERE year = EXTRACT(year from CURRENT_DATE)
            ),
            sector_performance AS (
            SELECT 
                t.industry as sector,
                AVG(
                    CASE 
                        WHEN close IS NOT NULL AND prev_close IS NOT NULL AND prev_close <> 0
                        THEN (close-prev_close)/prev_close * 100
                        ELSE 0
                    END
                ) as avg_change_pct
            FROM recent_market_data m
            JOIN dim_ticker_enrichment t ON m.ticker = t.ticker
            WHERE m.close IS NOT NULL
            AND m.prev_close IS NOT NULL
            AND m.prev_close <> 0
            AND t.industry IS NOT NULL
            GROUP BY t.industry
            )
            ,ranked_sectors AS (
                SELECT 
                    sector,
                    avg_change_pct,
                    ROW_NUMBER() OVER (ORDER BY avg_change_pct DESC) as best_rank,
                    ROW_NUMBER() OVER (ORDER BY avg_change_pct ASC) as worst_rank
                FROM sector_performance
            )
            SELECT 
                (SELECT sector FROM ranked_sectors WHERE best_rank = 1) as hottest_sector,
                (SELECT avg_change_pct FROM ranked_sectors WHERE best_rank = 1) as hottest_sector_change,
                (SELECT sector FROM ranked_sectors WHERE worst_rank = 1) as worst_sector,
                (SELECT avg_change_pct FROM ranked_sectors WHERE worst_rank = 1) as worst_sector_change
        """)
        sector_result = cur.fetchone()
        hottest_sector = {
            "name": sector_result[0] if sector_result and sector_result[0] is not None else "N/A",
            "change_pct": sector_result[1] if sector_result and sector_result[1] is not None else 0
        }
        worst_sector = {
            "name": sector_result[2] if sector_result and sector_result[2] is not None else "N/A",
            "change_pct": sector_result[3] if sector_result and sector_result[3] is not None else 0
        }

        # 4. Get Top and Worst Stocks
        cur.execute("""
            WITH ordered_market_data as (
            Select ticker,
                    composite_col,
                    record_count as current_record_count,
                    year,
                    month_num,
                    last_date,
                    LAG(composite_col) over(partition by ticker order by year, month_num) as prev_composite_col,
                    LAG(record_count) over (partition by ticker order by year, month_num) as prev_record_count
            from int_cumulated_daily_aggregation
            ),
            market_classification as (
                SELECT ticker, 
                    market_cap_category, 
                    row_number() over(order by overall_rank) as rnk 
                FROM dim_ticker_classifier
            ),
            recent_market_data AS (
                SELECT 
                    ticker,
                    CASE 
                        WHEN composite_col IS NOT NULL AND current_record_count IS NOT NULL 
                             AND current_record_count > 0 
                        THEN composite_col[current_record_count-1]:"close"::float 
                        ELSE NULL 
                    END as close,
                    CASE 
                        WHEN current_record_count IS NOT NULL AND current_record_count > 1 
                             AND composite_col IS NOT NULL
                        THEN composite_col[current_record_count-2]:"close"::float
                        WHEN prev_composite_col IS NOT NULL AND prev_record_count IS NOT NULL 
                             AND prev_record_count > 0
                        THEN prev_composite_col[prev_record_count-1]:"close"::float 
                        ELSE NULL
                    END as prev_close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY year DESC, month_num DESC) as row_num
                FROM ordered_market_data
                WHERE last_date = (Select max(last_date) from ordered_market_data)
            ),
            ranked_stocks AS (
                SELECT 
                    ticker,
                    close,
                    CASE 
                        WHEN close IS NOT NULL AND prev_close IS NOT NULL AND prev_close <> 0
                        THEN ((close - prev_close) / prev_close * 100)
                        ELSE 0
                    END as pct_change,
                    ROW_NUMBER() OVER (ORDER BY 
                        CASE 
                            WHEN close IS NOT NULL AND prev_close IS NOT NULL AND prev_close <> 0
                            THEN ((close - prev_close) / prev_close * 100)
                            ELSE 0
                        END DESC) as best_rank,
                    ROW_NUMBER() OVER (ORDER BY 
                        CASE 
                            WHEN close IS NOT NULL AND prev_close IS NOT NULL AND prev_close <> 0
                            THEN ((close - prev_close) / prev_close * 100)
                            ELSE 0
                        END ASC) as worst_rank
                FROM recent_market_data
                WHERE close IS NOT NULL
                AND prev_close IS NOT NULL
                AND prev_close <> 0
            )
            SELECT 
                (SELECT ticker FROM ranked_stocks WHERE best_rank = 1) as top_stock_ticker,
                (SELECT close FROM ranked_stocks WHERE best_rank = 1) as top_stock_price,
                (SELECT pct_change FROM ranked_stocks WHERE best_rank = 1) as top_stock_change,
                (SELECT ticker FROM ranked_stocks WHERE worst_rank = 1) as worst_stock_ticker,
                (SELECT close FROM ranked_stocks WHERE worst_rank = 1) as worst_stock_price,
                (SELECT pct_change FROM ranked_stocks WHERE worst_rank = 1) as worst_stock_change
        """)
        stock_result = cur.fetchone()
        top_stock = {
            "ticker": stock_result[0] if stock_result and stock_result[0] is not None else "N/A",
            "price": stock_result[1] if stock_result and stock_result[1] is not None else 0,
            "change_pct": stock_result[2] if stock_result and stock_result[2] is not None else 0
        }
        worst_stock = {
            "ticker": stock_result[3] if stock_result and stock_result[3] is not None else "N/A",
            "price": stock_result[4] if stock_result and stock_result[4] is not None else 0,
            "change_pct": stock_result[5] if stock_result and stock_result[5] is not None else 0
        }

        # 5. Get Market Summary (Watchlist of popular stocks)
        cur.execute("""
            WITH ordered_market_data as (
            Select ticker,
                    composite_col,
                    record_count as current_record_count,
                    year,
                    month_num,
                    last_date,
                    LAG(composite_col) over(partition by ticker order by year, month_num) as prev_composite_col,
                    LAG(record_count) over (partition by ticker order by year, month_num) as prev_record_count
            from int_cumulated_daily_aggregation
            ),
            market_classification as (
                SELECT ticker, 
                    market_cap_category, 
                    row_number() over(order by overall_rank) as rnk 
                FROM dim_ticker_classifier
            ),
            recent_market_data AS (
                SELECT 
                    ticker,
                    CASE 
                        WHEN composite_col IS NOT NULL AND current_record_count IS NOT NULL 
                             AND current_record_count > 0 
                        THEN composite_col[current_record_count-1]:"close"::float 
                        ELSE NULL 
                    END as close,
                    CASE 
                        WHEN current_record_count IS NOT NULL AND current_record_count > 1 
                             AND composite_col IS NOT NULL
                        THEN composite_col[current_record_count-2]:"close"::float
                        WHEN prev_composite_col IS NOT NULL AND prev_record_count IS NOT NULL 
                             AND prev_record_count > 0
                        THEN prev_composite_col[prev_record_count-1]:"close"::float 
                        ELSE NULL
                    END as prev_close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY year DESC, month_num DESC) as row_num
                FROM ordered_market_data
                WHERE last_date = (Select max(last_date) from ordered_market_data)
            ),
            popular_stocks AS (
                SELECT 
                    m.ticker,
                    t.name,
                    t.branding_logo_url,
                    t.branding_icon_url,
                    close,
                    prev_close
                FROM recent_market_data m
                LEFT JOIN dim_ticker_enrichment t ON m.ticker = t.ticker
                WHERE m.ticker IN ('AAPL', 'AMZN', 'MSFT', 'GOOGL', 'META', 'TSLA', 'AMD', 'NVDA', 'QQQ', 'SPY', 'IWM', 'UBER')
                AND m.close IS NOT NULL
                AND m.prev_close IS NOT NULL
            )
            SELECT 
                ticker,
                name,
                branding_icon_url,
                branding_logo_url,
                close as price,
                CASE 
                    WHEN close IS NOT NULL AND prev_close IS NOT NULL AND prev_close <> 0
                    THEN ((close - prev_close) / prev_close * 100)
                    ELSE 0
                END as change_pct,
                CASE
                    WHEN close IS NOT NULL AND prev_close IS NOT NULL AND close > prev_close THEN 'up'
                    WHEN close IS NOT NULL AND prev_close IS NOT NULL AND close < prev_close THEN 'down'
                    ELSE 'unchanged'
                END as direction
            FROM popular_stocks
            WHERE prev_close IS NOT NULL
            AND prev_close <> 0
            order by ticker
        """)
        market_summary_results = cur.fetchall()
        market_summary = []
        for row in market_summary_results:
            market_summary.append({
                'ticker': row[0] if row[0] is not None else "N/A",
                'company_name': row[1] if row[1] is not None else "N/A",
                'branding_icon': row[2] if row[2] is not None else "N/A",
                'branding_logo': row[3] if row[3] is not None else "N/A",
                'price': row[4] if row[4] is not None else 0,
                'change_pct': row[5] if row[5] is not None else 0,
                'direction': row[6] if row[6] is not None else "unchanged"
            })

        # Prepare the response
        response = {
            "market_sentiment": market_sentiment,
            "leader": leader,
            "hottest_sector": hottest_sector,
            "worst_sector": worst_sector,
            "top_stock": top_stock,
            "worst_stock": worst_stock,
            "market_summary": market_summary
        }

        return jsonify(response)

    except Exception as e:
        import traceback
        logger.error(f"Error in Market Dashboard endpoint: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/stock-trend')
def get_stock_trend():
    logger.debug("Stock Trend endpoint called")
    try:
        ticker = request.args.get('ticker')

        # If not in query params, check JSON body (for POST requests)
        if not ticker and request.is_json:
            ticker = request.json.get('ticker')

        # Fallback to referrer if available (for user clicks from other pages)
        if not ticker and request.referrer:
            # Extract ticker from referrer URL if it exists
            referrer_parts = request.referrer.split('/')
            if len(referrer_parts) > 0 and 'ticker=' in referrer_parts[-1]:
                ticker_part = [p for p in referrer_parts[-1].split('&') if 'ticker=' in p]
                if ticker_part:
                    ticker = ticker_part[0].split('=')[1]

        # Set default ticker to AAPL if none provided
        if not ticker:
            ticker = 'AAPL'
            logger.info("No ticker specified, using default: AAPL")

        # Handle period parameter - ensure it's a positive integer
        try:
            period = int(request.args.get('period', 30))
            if period <= 0:  # Ensure period is positive
                period = 30
                logger.info("Period must be positive, using default: 30")
        except (TypeError, ValueError):
            period = 30
            logger.info("Invalid period specified, using default: 30")

        # Ensure database connection
        if not hasattr(load_handler, 'conn') or load_handler.conn is None:
            load_handler.connect()

        conn = load_handler.conn
        cur = conn.cursor()

        # Get historical data for the selected ticker - using more defensive SQL
        cur.execute("""
            WITH historical_data AS (
                    SELECT 
                        *
                    FROM int_daily_metrics m
                    WHERE m.ticker = %s 
                    AND date IS NOT NULL
                    AND date >= DATEADD(day, -%s, CURRENT_DATE)
            )
            SELECT 
                ticker,
                date,
                open,
                high,
                low,
                close
            FROM historical_data
            WHERE ticker IS NOT NULL
            AND date IS NOT NULL
            AND open IS NOT NULL
            AND high IS NOT NULL
            AND low IS NOT NULL
            AND close IS NOT NULL
            ORDER BY date
        """, (ticker, period))

        trend_results = cur.fetchall()

        # Handle empty results case
        if not trend_results:
            logger.warning(f"No trend data found for ticker: {ticker}, period: {period}")
            trend_data = []
        else:
            trend_columns = [desc[0].lower() for desc in cur.description]
            trend_data = [dict(zip(trend_columns, row)) for row in trend_results]

        # Get stock info - using parameterized query
        cur.execute("""
            SELECT 
                t.ticker,
                t.name,
                t.industry,
                t.office,
                t.AVG_DAILY_VOLUME,
                t.AVG_DAILY_TRADED_VALUE_MILLIONS,
                t.LIQUIDITY_SCORE,
                t.market_cap_category,
                t.branding_icon_url,
                t.branding_logo_url,
                t.PRIMARY_EXCHANGE
            FROM dim_ticker_classifier t
            WHERE t.ticker = %s
        """, (ticker,))

        info_result = cur.fetchone()
        if info_result:
            info_columns = [desc[0].lower() for desc in cur.description]
            stock_info = dict(zip(info_columns, info_result))
        else:
            stock_info = {}
            logger.warning(f"No stock info found for ticker: {ticker}")

        # Return response in Grafana Infinity compatible format
        response = {
            "ticker": ticker,
            "stock_info": stock_info,
            "trend_data": trend_data
        }

        return jsonify(response)
    except Exception as e:
        import traceback
        logger.error(f"Error in stock trend: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500

@app.route('/market-sentiment')
def get_market_sentiment():
    # Get your market data
    response = get_market_dashboard()  # This already returns a dictionary

    # Extract just the market sentiment
    data = response.get_json()

    # Return just the market sentiment
    return data["market_sentiment"]


if __name__ == '__main__':
    app.run(debug=True, port=5001)