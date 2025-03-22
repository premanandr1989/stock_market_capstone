import redis
import time
import sys

# Connect to Redis with authentication
r = redis.Redis(
    host='localhost',
    port=6379,
    password='******',  # Add your Redis password here (masked)
    decode_responses=True
)

# Focus on tickers that have data
tickers = ["GOOGL", "NVDA","TSLA","AAPL","AMZN","GOOG","PLTR","NFLX","V","CRM","ADBE","ORCL","BRK.B"]

# Function to create velocity time series
def setup_velocity_series(ticker):
    try:
        velocity_key = f"ts:{ticker}:velocity"
        alert_key = f"ts:{ticker}:velocity_alert"

        r.execute_command('TS.CREATE', velocity_key, 'RETENTION', 86400000)
        r.execute_command('TS.CREATE', alert_key, 'RETENTION', 86400000,
                          'LABELS', 'type', 'alert', 'ticker', ticker)
    except:
        pass  # Ignore if already exists


# Set up velocity series for our tickers
for ticker in tickers:
    setup_velocity_series(ticker)

# Run continuously
while True:
    for ticker in tickers:
        try:
            price_key = f"ts:{ticker}:close"
            velocity_key = f"ts:{ticker}:velocity"
            alert_key = f"ts:{ticker}:velocity_alert"

            # Get ALL price points
            prices = r.execute_command('TS.RANGE', price_key, '-', '+')

            if len(prices) >= 2:
                # Use the last two price points
                current_price = float(prices[-1][1])
                previous_price = float(prices[-2][1])
                current_time = int(prices[-1][0])
                previous_time = int(prices[-2][0])
                time_diff = (current_time - previous_time) / 1000  # seconds

                # Print detailed information
                print(f"\n{ticker} info:")
                print(
                    f"  Last price: {current_price} at {time.strftime('%H:%M:%S', time.localtime(current_time / 1000))}")
                print(
                    f"  Previous price: {previous_price} at {time.strftime('%H:%M:%S', time.localtime(previous_time / 1000))}")
                print(f"  Time difference: {time_diff:.2f} seconds")

                # Calculate velocity (% change per second)
                velocity = ((current_price - previous_price) / previous_price) * 100 / time_diff

                velocity_timestamp = current_time + 1

                # Store velocity using the SAME TIMESTAMP as the price point
                r.execute_command('TS.ADD', velocity_key, velocity_timestamp, velocity)

                # Check alert threshold
                if abs(velocity) > 0.5:
                    r.execute_command('TS.ADD', alert_key, velocity_timestamp, velocity)
                    print(f"  ALERT: Velocity exceeds threshold!")

                print(f"  Calculated velocity: {velocity:.4f}%/s")
            else:
                print(f"{ticker}: Not enough price data points")

        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    # Wait before next calculation
    time.sleep(5)
