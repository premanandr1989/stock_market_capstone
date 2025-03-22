import asyncio
import json
import os
import redis
import threading
import time
from datetime import datetime
from dotenv import load_dotenv
from polygon import WebSocketClient
from polygon.websocket.models import Market, WebSocketMessage, EquityAgg
from typing import List, Dict, Any, Union

# Load environment variables
load_dotenv()

# Configuration
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

print(
    f"Starting with Redis configuration: Host={REDIS_HOST}, Port={REDIS_PORT}, Auth={'Yes' if REDIS_PASSWORD else 'No'}")

# Redis connection
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    # Test connection
    redis_client.ping()
    print("✅ Successfully connected to Redis!")
except Exception as e:
    print(f"❌ Redis connection error: {e}")
    print("Please check your Redis connection settings and make sure Redis server is running.")
    exit(1)

# Redis pipeline for batch operations
pipe = redis_client.pipeline()
event_count = 0
last_flush_time = datetime.now()

# Track symbols that have had TimeSeries created
symbol_timeseries_created = set()


def create_timeseries_for_symbol(symbol):
    """
    Create TimeSeries for a symbol if they don't already exist.
    This is called once per symbol to avoid errors.

    Args:
        symbol: Stock symbol to create TimeSeries for
    """
    if symbol in symbol_timeseries_created:
        return

    # Create the TimeSeries outside the main pipeline
    try:
        # Create TimeSeries for each metric with labels
        redis_client.execute_command('TS.CREATE', f"ts:{symbol}:open", 'LABELS', 'symbol', symbol, 'metric', 'open',
                                     'RETENTION', 86400, 'IF_NOT_EXISTS')
        redis_client.execute_command('TS.CREATE', f"ts:{symbol}:high", 'LABELS', 'symbol', symbol, 'metric', 'high',
                                     'RETENTION', 86400, 'IF_NOT_EXISTS')
        redis_client.execute_command('TS.CREATE', f"ts:{symbol}:low", 'LABELS', 'symbol', symbol, 'metric', 'low',
                                     'RETENTION', 86400, 'IF_NOT_EXISTS')
        redis_client.execute_command('TS.CREATE', f"ts:{symbol}:close", 'LABELS', 'symbol', symbol, 'metric', 'close',
                                     'RETENTION', 86400, 'IF_NOT_EXISTS')
        redis_client.execute_command('TS.CREATE', f"ts:{symbol}:volume", 'LABELS', 'symbol', symbol, 'metric', 'volume',
                                     'RETENTION', 86400, 'IF_NOT_EXISTS')
        redis_client.execute_command('TS.CREATE', f"ts:{symbol}:vwap", 'LABELS', 'symbol', symbol, 'metric', 'vwap',
                                     'RETENTION', 86400, 'IF_NOT_EXISTS')

        print(f"Created TimeSeries for new symbol: {symbol}")
    except redis.exceptions.ResponseError as e:
        # If there's an error, it might be because the keys already exist - that's fine
        print(f"Note: Some TimeSeries may already exist for {symbol}: {e}")

    # Add to our tracking set regardless of whether we created them or they already existed
    symbol_timeseries_created.add(symbol)


def handle_msg(msgs):
    """
    Process messages from Polygon WebSocket and store in Redis as TimeSeries.

    Args:
        msgs: List of messages from Polygon WebSocket
    """
    global event_count, last_flush_time, pipe

    try:
        for m in msgs:
            # For the first few messages, print for debugging
            if event_count < 5:
                print(f"Sample message #{event_count + 1}: {m}")

            # Check if this is an EquityAgg object from Polygon SDK
            if hasattr(m, 'symbol') and hasattr(m, 'event_type') and m.event_type == 'A':
                # Extract data from EquityAgg object
                symbol = m.symbol
                timestamp = m.start_timestamp
                open_price = m.open
                high_price = m.high
                low_price = m.low
                close_price = m.close
                volume = m.volume
                vwap = getattr(m, 'vwap', 0)

                # Ensure TimeSeries exist for this symbol (called once per symbol)
                if symbol not in symbol_timeseries_created:
                    create_timeseries_for_symbol(symbol)

                # Create a timestamp in seconds for Redis TimeSeries
                ts_seconds = int(timestamp)  # Convert from milliseconds to seconds

                # STORE AS TIMESERIES FOR GRAFANA - only TS.ADD commands in the main pipeline
                try:
                    # Add data points for each metric
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:open", ts_seconds, open_price)
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:high", ts_seconds, high_price)
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:low", ts_seconds, low_price)
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:close", ts_seconds, close_price)
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:volume", ts_seconds, volume)

                    if vwap:
                        pipe.execute_command('TS.ADD', f"ts:{symbol}:vwap", ts_seconds, vwap)
                except Exception as e:
                    # If we hit an error adding data, try creating the time series again
                    # and then retry the add operation
                    print(f"Error adding data for {symbol}, attempting to create time series: {e}")
                    symbol_timeseries_created.discard(symbol)  # Remove from our tracking set
                    create_timeseries_for_symbol(symbol)  # Try to create again

                    # Retry the data additions
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:open", ts_seconds, open_price)
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:high", ts_seconds, high_price)
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:low", ts_seconds, low_price)
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:close", ts_seconds, close_price)
                    pipe.execute_command('TS.ADD', f"ts:{symbol}:volume", ts_seconds, volume)

                    if vwap:
                        pipe.execute_command('TS.ADD', f"ts:{symbol}:vwap", ts_seconds, vwap)

                # Store latest data per symbol (still useful for quick lookups)
                pipe.set(f"latest:{symbol}", json.dumps({
                    "price": close_price,
                    "timestamp": timestamp
                }))

                # Keep track of all symbols for discovery
                pipe.sadd("market:symbols", symbol)

                # Publish updates for real-time subscribers
                pipe.publish(f"updates:{symbol}", json.dumps({
                    "symbol": symbol,
                    "price": close_price,
                    "timestamp": timestamp,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "volume": volume,
                    "vwap": vwap
                }))

                event_count += 1

        # Execute the pipeline after batch threshold or time interval
        current_time = datetime.now()
        time_diff = (current_time - last_flush_time).total_seconds()

        # Flush data more frequently (every 0.1s or 100 events)
        if event_count >= 100 or time_diff >= 0.1:
            try:
                pipe.execute()
                event_count = 0
                last_flush_time = current_time
                print(f"Flushed data at {current_time.isoformat()}")
            except redis.exceptions.ResponseError as e:
                # If we hit a pipeline error, print it but continue
                print(f"Pipeline execution error: {e}")
                # Reset the pipeline to avoid carrying over any failed commands
                pipe = redis_client.pipeline()
                event_count = 0
                last_flush_time = current_time

    except Exception as e:
        print(f"Error processing message: {e}")
        import traceback
        traceback.print_exc()


# Track active symbols and message rates
async def track_statistics():
    """
    Track and store market statistics for monitoring.
    Runs as a background task to collect and store metadata.
    """
    # Create stats TimeSeries only once
    try:
        redis_client.execute_command('TS.CREATE', "stats:active_symbols", 'LABELS', 'metric', 'active_symbols',
                                     'RETENTION', 86400, 'IF_NOT_EXISTS')
        print("Created stats:active_symbols TimeSeries")
    except redis.exceptions.ResponseError:
        print("Stats TimeSeries may already exist")

    while True:
        try:
            # Get all active symbols
            symbol_keys = redis_client.keys("latest:*")
            symbols = [key.split(":")[1] for key in symbol_keys]
            active_symbols = len(symbols)

            # Get message rates for active symbols
            message_rates = {}
            for symbol in symbols[:10]:  # Limit to 10 symbols for performance
                # Count timeseries entries in the last minute
                now = int(time.time())
                one_minute_ago = now - 60
                try:
                    count = redis_client.execute_command('TS.RANGE', f"ts:{symbol}:close", one_minute_ago * 1000,
                                                         now * 1000)
                    message_rates[symbol] = len(count) if count else 0
                except redis.exceptions.ResponseError:
                    # TimeSeries may not exist yet for this symbol
                    message_rates[symbol] = 0

            # Store stats as timeseries for Grafana
            ts_now = int(time.time() * 1000)
            redis_client.execute_command('TS.ADD', "stats:active_symbols", ts_now, active_symbols)

            # Store top symbols information
            if message_rates:
                for symbol, count in message_rates.items():
                    # Create the message rate TimeSeries if it doesn't exist
                    try:
                        redis_client.execute_command('TS.CREATE', f"stats:message_rate:{symbol}",
                                                     'LABELS', 'symbol', symbol, 'metric', 'message_rate',
                                                     'RETENTION', 86400, 'IF_NOT_EXISTS')
                    except redis.exceptions.ResponseError:
                        # TimeSeries may already exist, continue
                        pass

                    # Add the data point
                    redis_client.execute_command('TS.ADD', f"stats:message_rate:{symbol}", ts_now, count)

            print(f"[{datetime.now().isoformat()}] Active symbols: {active_symbols}, Sample rates: {message_rates}")

        except Exception as e:
            print(f"Stats error: {e}")
            import traceback
            traceback.print_exc()

        await asyncio.sleep(10)  # Update every 10 seconds


def run_statistics_tracker():
    """Run the statistics tracker in a separate thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(track_statistics())


def main():
    """
    Main entry point for the Polygon-Redis integration.
    Initializes and runs the WebSocket client.
    """
    print("Starting Polygon.io WebSocket client with Redis TimeSeries integration")

    # Validate Polygon API key
    if not POLYGON_API_KEY:
        print("❌ Error: POLYGON_API_KEY not set in environment variables")
        exit(1)

    # Start the statistics tracker in a separate thread
    stats_thread = threading.Thread(target=run_statistics_tracker, daemon=True)
    stats_thread.start()
    print("Statistics tracker started in background thread")

    try:
        # Create the WebSocket client
        client = WebSocketClient(
            api_key=POLYGON_API_KEY,
            feed='delayed.polygon.io',  # Use 'delayed' for free tier, 'realtime' for paid
            market="stocks",
            # For specific symbols:
            # subscriptions=["A.AAPL", "A.MSFT", "A.GOOGL", "A.AMZN", "A.TSLA"]
            # For all symbols (warning: high volume):
            subscriptions=["A.*"]
        )

        print("Connecting to Polygon.io WebSocket...")
        client.run(handle_msg)

    except Exception as e:
        print(f"Error initializing Polygon WebSocket client: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback

        traceback.print_exc()