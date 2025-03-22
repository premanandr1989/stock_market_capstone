## Table of Contents
- [About](#about)
- [Stock Market Analytics Platform Overview](#overview-and-data-sources)
- [Problem Statement](#problem-statement)
- [Platform Overview](#platform-overview)
- [Architecture Overview](#architecture-overview)
- [Technologies Used](#technologies-used)
- [Real-time Analytics Architecture](#real-time-analytics-architecture-deep-dive)
  - [Real-time Data Flow](#real-time-data-flow)
  - [Key Components of the Code](#key-components-of-the-code-for-real-time-processing)
  - [Real-time Dashboards Overview](#real-time-dashboards-overview)
- [Historical Data Processing](#historical-data-processing)
- [Data Transformation with dbt](#data-transformation-with-dbt)
- [Technical Analysis Indicators](#technical-analysis-indicators)
- [Challenges](#challenges)
- [Future Enhancements](#future-enhancements)
- [Conclusion](#conclusion)

# About
I enrolled in Zach Wilson's Dataexpert.io Data Engineering bootcamp In this project, i wanted to work on my learnings on data Engineering. Inorder to validate my learnings and also build a unique use-case around it, I used both daily as well as real-time data from various end-points of polygon.io. The goal is to create a unified historical as well as real-time stock trading metrics platform that can help trading analysts uncover patterns from candlestock charts, bollinger bands, MACD, moving averages. It also aims to provide a holistic view of the current market condition and analyse top and worst performing stocks on a day-to-day basis.

## Overview and Data Sources
This project is an attempt to develop a data engineering solution for real-time and historical stock market analytics (>3 years of historic data) covering stock tickers of the scale of 2.5k which are clearly demarcated according to market-cap and liquidity ratios. It combines streaming data processing with robust ETL pipelines to deliver actionable trading insights.

Data Sources are as follows:-
**Polygon API**
1. Daily OHLC,Volume data
2. Ticker Details
3. Ticker Identifiers
4. SIC codes (For sector based performances)
5. Split ratio data for adjusting prices of the historical data.

**FinnHub API**
1. Earnings Calendar
2. Earning Surprises

**FMP API**
1. Stock graders ratings for 99 stocks (under free-tier)

## Problem Statement
1️⃣ Unifying historical context of stock data with real-time analytics so that trading analysts dont have to look for fragmented pieces of data across disparate sources.

2️⃣ Build Advanced Technical Indicators - Built custom metrics like Moving Averages, Bollinger Bands, MACD, Candlesticks. I also added buy/sell indicators so trading analysts can get a clear picture to make data-driven decisions.

3️⃣ Market Segmentation - To differentiate tickers by Market Cap and monitor their performances with >3 yr historic data as well as real-time data

4️⃣ Real-Time Metrics - Velocity rate (with alerts for sudden spikes), Real-time moving averages with customizable window lookback points.

5️⃣ On demand data in an instant - For more than 2.4k stock tickers with the flexibility of viewing these dashboards either daily, weekly or monthly.


## Platform Overview
This trading platform provides instant, on-demand access to real-time and historical stock data—spanning four years—enabling traders to quickly analyze any ticker at the click of a button.

**Key Features**

**Market Segmentation:** Stocks are neatly categorized by market cap and liquidity ranking, so users can pinpoint relevant symbols faster.
**Advanced Indicators:** Custom calculations of SMA, EMA, WMA, Bollinger Bands, MACD etc..—tested for ~99.5% accuracy—give traders deeper insights into price trends and volatility.
**Unified Data Access:** Both real-time streams and historical records are consolidated into one platform, eliminating the hassle of scattered data sources.

**Benefits**

**Immediate Insights:** Traders can access critical metrics and charts instantly, accelerating decision-making.
**Enhanced Accuracy:** Rigorous testing ensures reliable indicators and minimizes analytical errors.
**Better Strategy Formation:** By combining historical context with up-to-the-second data, users can craft more informed trading strategies in one integrated environment.

   
## Architecture Overview

![image](https://github.com/user-attachments/assets/97e7c753-84fc-4833-872e-03f0bffff7e1)

For detailed view- link: [https://www.canva.com/design/DAGgjhJhDX4/6GFEYzOfiSoBidm_WKfr_g/edit?utm_content=DAGgjhJhDX4&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton](https://www.canva.com/design/DAGgjhJhDX4/6GFEYzOfiSoBidm_WKfr_g/edit?utm_content=DAGgjhJhDX4&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton)


This project implements a dual architecture approach to provide both real-time market monitoring and in-depth historical analysis for stock market data:

**Real-time Data Pipeline:** Redis TimeSeries + Grafana for live market monitoring
**Historical Data Pipeline:** s3 + Airflow + dbt + Snowflake + Grafana for comprehensive historical analysis

## Technologies Used

The project uses the following technologies

**Redis:** Acts as pub/sub platform alongside being a low-latency store. Sub-millisecond response times, seamless integration with Grafana,  making it the best fit for this use-case.

**Snowflake:** Functions as your data warehouse with a three-layer architecture (storage, compute/query processing, and cloud services), offering scalability and near-zero management overhead.

**Airflow:** Provides workflow management through Directed Acyclic Graphs (DAGs) that define task dependencies and execution order, enabling you to author, schedule, and monitor complex data pipelines.

**Astronomer:** A managed platform built on top of Apache Airflow that simplifies deployment, scaling, and monitoring while adding enterprise-grade features and reducing operational complexity.

**Apache Spark:** A platform for managing unstructured data which comes alongside great capability to tranform as well as validate data. I used it for DQ checks on historic-data and performing data profiling to uncover certain outliers.

**DBT:** Handles data transformations and testing within your data pipeline.

**Grafana:** Creates interactive dashboards and provides great solutions for real-time dashboards with as low as 5 second refreshes in Open source and 1 second refreshes in the cloud version.


## Real time Analytics Architecture Deep Dive
The real-time component leverages Redis TimeSeries to ingest and process stock market data from Polygon.io's WebSocket API. This architecture was selected for several compelling reasons:

Why Redis for Real-time Processing?

**In-Memory Performance :** Redis provides microsecond data access times by storing data primarily in memory, which is critical for real-time market data where even milliseconds matter.

**Purpose-Built Time Series Database:** Redis TimeSeries module offers specialized data structures optimized for time-stamped financial data, supporting automatic downsampling, retention policies, and efficient range queries essential for financial analytics.

**Pub/Sub Messaging System:** Redis's publish/subscribe functionality enables immediate notification of market events to multiple subscribers without polling, facilitating real-time dashboards and alerts.

**Horizontal Scalability:** Redis can scale out to handle thousands of stock symbols simultaneously with minimal latency degradation, making it capable of processing the entire US equities market (~8,000 stocks).

**Low Operational Complexity:** Compared to more complex streaming solutions, Redis provides a balance of performance and operational simplicity, reducing maintenance overhead.

**Performance Advantages**

**Lower Latency** - sub-millisecond response times, optimal for stock ticks

**Simpler Architecture** - fewer moving parts than Kafka's broker/zookeeper architecture

**High Throughput for Small Messages** - excels at handling many small messages (like stock ticks) at extremely high rates

**Use Case Alignment**
**Last Value Caching** - Useful for maintaining current state

**Rich Data Structures** -  provides sorted sets, hashes, and lists that are useful for time-series data organization

**Seamless Integration** - Customizable Redis functions that align with Grafana Dashboards through custom keys


The real-time pipeline ingests market data for all US stocks, processing approximately 1,000+ messages per second during active trading hours. Each message contains critical price information (open, high, low, close), volume data, and VWAP (Volume Weighted Average Price). The data is transformed, stored, and immediately available for visualization in Grafana dashboards, enabling traders to act on market movements with minimal delay.

### Real time Data Flow
The Redis integration code implements several key components:

<img width="1303" alt="image" src="https://github.com/user-attachments/assets/7981e881-6269-49b3-b2ec-4e7c48e3a8f2" />



**WebSocket Connection Management:** Maintains a stable connection to Polygon.io's WebSocket API, handling reconnection logic and authentication.

**Time Series Creation and Management:** Automatically creates appropriately labeled time series for each stock symbol as it appears in the data stream.

**Efficient Batch Processing:** Uses Redis pipelining to batch operations, dramatically reducing network overhead while maintaining near-real-time updates.

**Market Statistics Tracking:** A background task monitors system performance metrics, tracking active symbols and message rates to ensure system health.

**Real-time Publishing:** Updates are published to Redis channels, allowing multiple applications to subscribe to specific symbols or market segments.

## Key components of the code for real time processing

### Data Processing Flow

<img width="1303" alt="image" src="https://github.com/user-attachments/assets/86d7bb97-ce2b-4b3d-9541-78968695dcb3" />


**WebSocket Connection:** Connects to Polygon.io's WebSocket feed for stock data
**Message Handling:** handle_msg() processes incoming messages from Polygon's WebSocket
**TimeSeries Creation:** create_timeseries_for_symbol() creates Redis TimeSeries for new stock symbols
**Data Storage:** Stores OHLCV (Open, High, Low, Close, Volume) and VWAP data points in Redis TimeSeries. Uses Redis pipeline for batched writes to improve performance
Stores latest data per symbol for quick lookups
Maintains a set of all symbols for discovery

**Monitoring & Statistics**
Runs a background task (track_statistics()) to monitor: Number of active symbols, Message rates for active symbols, Stores these statistics in separate TimeSeries for monitoring
**Error Handling**
Robust error handling throughout the pipeline
Automatic recovery from connection issues
Retries failed operations with TimeSeries creation

**Performance Considerations**
Batches operations using Redis pipeline
Flushes data every 100 events or 0.1 seconds
Uses separate thread for statistics tracking to avoid blocking main processing

This architecture is designed to handle market data surges during high-volatility periods when trading volumes can increase 5-10x over normal conditions.

### Real time Dashboards Overview**

**velocity rate**: It also alerts when there's a price spike >=.5% of previous close price within the 5 second window
<img width="1298" alt="image" src="https://github.com/user-attachments/assets/58ab8c24-00f4-4c3b-8475-ce041d6dc1aa" />

**candlestick chart**
<img width="1536" alt="image" src="https://github.com/user-attachments/assets/26c67bdf-d50a-4950-aed2-77ed26d0f490" />

**moving averages**
<img width="1663" alt="image" src="https://github.com/user-attachments/assets/789f6dbb-43a3-4684-80a0-755403f696a0" />

All 3 dashboards have a latency of 5 seconds. Grafana open source doestn allow to go below the 5 second threshold for refreshes (although this can be tweaked in the .ini file to as low as 1s).

## Historical Data Processing
For historical analysis, the platform implements a robust ETL pipeline:
Data Ingestion and Storage Strategy

**Historical Depth:** > Three years of historical data extracted from Polygon.io API, providing sufficient historical context for technical analysis while balancing storage costs.
**Data Quality Assurance:** Apache Spark is used to perform comprehensive data quality checks before loading, including:
All the below aspects were taken by Apache Spark.
1. Detecting and handling missing values
2. Identifying and removing duplicate records
3. Validating price data against expected ranges
4. Ensuring timestamp continuity and proper format

Snowflake Data Architecture: Data is organized in Snowflake using a multi-layered approach:

Raw data zone/ Staging: Preserves original data in its unmodified form
Transformation zone/ Intermediate: Hosts intermediate processing results
Analytics zone/ Marts: Contains final modeled data ready for consumption


**Partitioning Strategy:** Data is partitioned by date and clustered by ticker symbol to optimize query performance for both time-series analysis (single stock over time) and cross-sectional analysis (multiple stocks at a point in time).

ETL Orchestration with Airflow
The Airflow DAG orchestrates the entire ETL process:

![image](https://github.com/user-attachments/assets/36ff229d-2493-4b9f-8c29-61242d6716fb)

<img width="1728" alt="image" src="https://github.com/user-attachments/assets/571f5a62-baa0-4ce0-8901-8aecb912baa4" />





**Data Extraction:** Fetches daily stock aggregates and news data from Polygon.io API, daily earning calendar from finnHub API and daily stock analysts gradings from FMP
**Data Validation:** Performs quality checks using DBT transformations
**Snowflake Loading:** Loads validated data into Snowflake staging tables
**dbt Transformation:** Triggers dbt transformations to build analytical models
**Failure Handling:** Implements retry logic and notification systems for pipeline errors
**Skipping Execution using AirflowSkipExecution:** On public holidays, the DAG is designed to skip executing downstream tasks by checking if any data exists on that day

This architecture enables efficient processing of large historical datasets while maintaining complete data lineage and transformation history.

### Data Transformation with dbt

**dbt Model Organization**

<img width="1728" alt="image" src="https://github.com/user-attachments/assets/d0fc85dd-e2fd-495f-bf4f-2d2c81018f07" />


The dbt implementation follows a modular pattern to maximize reusability and maintainability
Implemented the cumulative-table design ensuring on-demand unpacking based on time period requested by the user from params passed from the dashboard instead of full-table scan traditonal approach hence optimising the table search. The tables within Snowflake are clustered by ticker, month and year ensuring that cumulation happens over a 30 day period.


### dbt Testing Framework

![image](https://github.com/user-attachments/assets/09094a2a-e3b0-406d-aed1-00fd53c61bcb)

<img width="1672" alt="image" src="https://github.com/user-attachments/assets/c733d5ed-ecdd-469e-a9e4-b0bd8c67415f" />


Also, custom tests have been used to test the logic of moving averages computation.
Generic Tests: Apply standard integrity checks (uniqueness, not_null, referential integrity)
Custom Tests: Implement domain-specific validations:

Technical indicator accuracy validation against Polygon.io reference data as indicated in pic above. The validation report suggests that the models created for showing various indicators have ~99.5% accuracy compared to referenced data from Polygon.io

**Ensured Idempotency using incremental strategies**

<img width="641" alt="image" src="https://github.com/user-attachments/assets/2b8338f5-387c-43ea-898a-0b54129170c9" />

<img width="641" alt="image" src="https://github.com/user-attachments/assets/6efb6d23-a044-4940-a8ff-0c7debbb9d9d" />


**dbt Macros for Reusability**
Custom macros encapsulate complex calculation logic:

<img width="1183" alt="image" src="https://github.com/user-attachments/assets/bb48dc25-3279-45d1-95bc-cd114f8efd21" />

The above macro helps calculate wma in a seamless fashion since it generates weights proportional to the number of days for lookback.
**sma**
<img width="1622" alt="image" src="https://github.com/user-attachments/assets/42d4ea41-a3e3-400c-bf9c-2a3cbdad979f" />

**MACD**
Used recursive CTEs to calculate MACD:-

<img width="1226" alt="image" src="https://github.com/user-attachments/assets/e34f1bc5-9589-4bb3-b550-c6b7cbe4d585" />


**calculate_moving_average:** Standardized calculation for different moving average types
**calculate_macd:** Standarized approach to calculated MACD and having buy sell indicators makes it insight driven for trend analysts.

## Technical Analysis Indicators
The platform provides a comprehensive suite of technical analysis tools, each serving a specific analytical purpose:

**1. Candlestick Charts**

![image](https://github.com/user-attachments/assets/b66113cf-63a4-44d7-9352-5d9c02726677)


What They Are: Candlestick charts display the open, high, low, and close prices within a specified timeframe (day, week, month etc.). Each "candle" visually represents the price movement, with colors typically differentiating between upward (green) and downward (red) movements.
Technical Implementation:

Data for candlesticks is prepared using a flexible dbt model (mart_candlestick_flexible) that allows for different time aggregations (daily, weekly, monthly)

Pattern Recognition: Trained traders can identify dozens of established patterns like Doji (indecision), Hammer (potential reversal), and Engulfing (strong reversal signal)
Momentum Assessment: The size and color of candles help assess price momentum and potential exhaustion
Support/Resistance Identification: Clusters of similar candlesticks often indicate price levels where buying or selling pressure emerges
Decision Making Example: When a bullish engulfing pattern appears at a known support level after a downtrend, it provides a strong buy signal with a clear stop-loss point just below the pattern

**2. Moving Averages SMA EMA WMA**

![image](https://github.com/user-attachments/assets/6dfa5757-eeaa-429b-9712-15fa6828ec36)

What They Are: Moving averages smooth price data to reveal underlying trends by averaging prices over specified periods. The platform implements three types:

Simple Moving Average (SMA): Equal weight to all prices in the period
Exponential Moving Average (EMA): Greater weight to recent prices
Weighted Moving Average (WMA): Linearly increasing weights to more recent prices

Technical Implementation:

Implemented as separate dbt models (mart_sma_flexible, mart_ema_flexible, mart_wma_flexible)
Supports configurable periods (10, 20, 50, 100, 200 days) via parametrization

Trading Applications:

Trend Identification: MAs with different periods identify short, medium, and long-term trends
Support/Resistance Levels: MAs often act as dynamic support/resistance levels
Crossover Signals: When shorter-period MAs cross longer-period MAs, they generate trading signals:

Golden Cross (short-term MA crosses above long-term MA): Bullish signal
Death Cross (short-term MA crosses below long-term MA): Bearish signal

Decision Making Example: A stock trading above its 50-day and 200-day SMAs with the 50-day recently crossing above the 200-day presents a strong bullish trend confirmation, suggesting holding or adding to long positions

**3. MACD (Moving Average Convergence Divergence)**

![image](https://github.com/user-attachments/assets/c95900d6-93c9-45d6-8f13-d5c8d00f4afa)

Clicking on the purple dot (which represents the analyst rating for the stock on that day) takes you to the graders dashboard which looks like this :point_down:. This basically gives the user a disposition to what decision they can make on that day based on analyst stock graders. 

![image](https://github.com/user-attachments/assets/d4c0066b-6960-4a54-8ce2-4d5f18b94075)


What It Is: MACD is a momentum oscillator that shows the relationship between two moving averages of a security's price. It's calculated by subtracting the 26-period EMA from the 12-period EMA. A 9-period EMA of the MACD, called the "signal line," is then plotted on top of the MACD line.
Technical Implementation:

Implemented in mart_macd dbt model
Calculates three key components:

MACD Line: (12-day EMA - 26-day EMA)
Signal Line: 9-day EMA of MACD Line
Histogram: MACD Line - Signal Line


Trading Applications:

Momentum Measurement: The MACD histogram shows the rate of change in momentum, with widening bars indicating accelerating price movement
Signal Line Crossovers: When the MACD line crosses above the signal line, it generates a bullish signal; a cross below is bearish
Decision Making Example: A MACD line crossing above its signal line while both are below the zero line suggests a possible reversal from a downtrend, especially if accompanied by increased volume and supporting candlestick patterns

**4. Bollinger Bands**
![image](https://github.com/user-attachments/assets/811513b1-71fc-45b9-88dc-6ecdc558bc57)

Clicking on the purple dot (which represents the analyst rating for the stock on that day) takes you to the graders dashboard which looks like this :point_down:. This basically gives the user a disposition to what decision they can make on that day based on analyst stock graders. 

![image](https://github.com/user-attachments/assets/d4c0066b-6960-4a54-8ce2-4d5f18b94075)


What They Are: Bollinger Bands consist of three lines: a 20-period simple moving average (middle band) and two standard deviation lines above and below the SMA (upper and lower bands). These bands expand and contract based on market volatility.
Technical Implementation:

Implemented as a dbt model that calculates:

Middle Band: 20-day SMA - Kept Standard Devitaion flexible.
Upper Band: Middle Band + (2 × Standard Deviation)
Lower Band: Middle Band - (2 × Standard Deviation)


Trading Applications:

Volatility Measurement: Band width indicates market volatility, with wider bands showing higher volatility
Overbought/Oversold Conditions: Price touching or exceeding the upper band suggests overbought conditions, while touching or falling below the lower band suggests oversold conditions
Mean Reversion: Price tends to return to the middle band, making extreme band touches potential reversal points
Breakouts: Price moving outside the bands with high volume often signals the start of a sustained move
Decision Making Example: A stock price that bounces off the lower Bollinger Band while the MACD forms a bullish divergence suggests a potential buying opportunity, with a stop-loss just below the recent low

**5. Market Health Dashboard**

![image](https://github.com/user-attachments/assets/3100e57b-70ad-4f6d-98ac-bdd47e1978d9)

What It Is: A comprehensive dashboard showing broad market metrics across various sectors and market capitalization ranges.
Technical Implementation:

Aggregates data across multiple stocks to create market-wide metrics. Indicates the overall market health based on the top performing stocks recent performance.
Calculates sector-specific indicators based on current month metrics, Top and worst performing stocks based on previous close price

Trading Applications:

Market Context: Provides essential background for individual stock decisions
Top Stocks under Mega Cap category recent performance
Trend line for whichever stock the user wants to monitor over a previous 30 day period for close price.

Data Validation and Quality Assurance
The platform implements comprehensive validation mechanisms:
Accuracy Validation

Technical indicators calculated by our system are continuously validated against Polygon.io's provided values
Achieved ≥99.5% accuracy across all indicators, with discrepancies primarily due to minor differences in calculation methodologies
Automated daily reconciliation ensures ongoing calculation accuracy

**6. EPS Metrics**
<img width="1728" alt="image" src="https://github.com/user-attachments/assets/39ea0eb1-6781-4f2d-948a-fe29d626ef25" />

The EPS metrics is the key for understanding the volatitlity of the market as well as returns given by the stock on and immediately after the EPS announcement (which generally happens quartely for each stock).

I have 3 charts here:-
1. EPS Surprise percent by Ticker - Filters (Market Cap, Ticker and View By) - This chart allows you to compare the EPS Surprises percentages by multi-selecting stocks and hence giving you a bird's eye view of performance of each stock for the last 4 quarters
2. Normalized price performance comparison over each quarter for a 10 day period - Filters (Market Cap, Ticker, Quarter, Year) - This gives the price returns over a 10 day period to compare how the stock performed after the EPS earnings were announced during the end of the quarter for the particular stock.
3. Surprise Price Correlation quantified by Return Period - Filters (Market Cap, Ticker, Quarter, Year, Return period (flexible)) - This calculates the correlation between the returns percent and surprise percent using numpy libraries. Surprise-price correlation is a valuable analytical tool that helps analysts understand how stock prices respond to earnings surprises. Here's how analysts typically use it.
Analysts track the relationship between earnings surprises (actual earnings vs. consensus estimates) and subsequent stock price movements
This correlation helps predict how a stock might react to future earnings announcements.

**Data Quality Checks**

1. Missing data detection and imputation for non-trading days
2. Price continuity validation to identify and flag potential data errors
3. Temporal consistency checks to ensure proper time sequencing

## Challenges
1. Connect Grafana to Snowflake for historical data visualization - Due to discontinue support for snowflake plugin, there were issues connecting the Snowflake datawarehouse to Grafana.
   Solution: Leveraged Grafana-Infinity connector with custom SQL-based API endpoints. This took considerable time since the API end-points had to be tested thoroughly using cutom SQL logic. Also incorporated on-demand         unpacking of the cumulated prices array hence keeping the optimal structure of the array intact. Refer pic below, the time-series filter represents the period between user wants to see the charts:-

   <img width="1491" alt="image" src="https://github.com/user-attachments/assets/5caf6fe3-7e2c-4ec8-82f2-e57efac92211" />

   Implementation: Created REST API layer with parameterized queries that translate Grafana requests to Snowflake SQL. It was developed using a flask CORS application.

2. Setting up Redis Velocity Rate charts:
   Challenge - Understanding the Redis Key Value generation and time series patterns was a big challenge. Initially, used the hget and hset and encountered issues around latency as well as storing OHLCV data for multiple keys simultaneously.
   Solution - Understood redis Architechture and leveraged the time series modules within Redis to store the keys using TS.CREATE, TS.ADD (for adding data points) and TS.ALTER for altering retention time for the keys. This was important to configure the retention time since the grafana service was crashing within the EC2 instance due to memory outages. Had to limit the retention to less than 30 minutes to achieve seamless running of all services in the t2.micro instance.

3. Duplication policy and eviction strategy within Redis:
   Challenge - The velocity key and the alert keys were not getting updated due to 'BLOCK' as the default DUPLICATE POLICY. 
   Solution - Altered the DUPLICATE policy to last and recreated the keys everytime the systemd service file was restarted within gunicorn so that there is no conflict with the keys previously created.

   <img width="1516" alt="image" src="https://github.com/user-attachments/assets/20acaaef-232b-46dd-839e-e55931f4b801" />

4. Overall Market Dashboard - In retrospect, considered using react. However, thats an enhancement for the future. This is due to attractive custom cards that are not available within grafana and implementing them would be    a significant overhead in terms of time constraints.



### Future Enhancements

**Machine Learning Integration:**

Predictive models for price movements based on technical indicator patterns
Anomaly detection for unusual market behavior
Automated pattern recognition.


**Advanced Backtesting Framework:**

Historical simulation of trading strategies with realistic execution modeling
Performance analytics with risk-adjusted return metrics


**Sentiment Analysis Integration:**

Natural language processing of financial news
Social media sentiment tracking for selected tickers
Correlation analysis between sentiment signals and price movements

**Kafka Integration:**

Enabling a message queue using Kafka. Since, I did not feel the need to use Kafka for my use-case, I did not explore on this. But I would want to implement Kafka as a message queue which acts both as a producer for websockets and temporary storage until data is consumed/loaded into Redis.


**Enhanced Alert System:**

Custom alert criteria based on multiple indicator conditions
Multi-channel notifications (email, SMS, push)
Alert prioritization based on historical signal accuracy


**Portfolio Management Tools:**

Position sizing recommendations based on volatility
Correlation analysis for portfolio diversification
Drawdown and risk exposure visualization



## Conclusion
This Stock Market Analytics Platform is an attempt to creating a data-engineering solution that combines real-time processing capabilities with in-depth historical analysis. By leveraging modern data technologies and implementing financial domain expertise, the system provides traders with a toolkit (more indicators to be added) for making informed market decisions based on technical analysis, pattern recognition, and market context.
The architecture successfully balances performance requirements with cost efficiency, handling both the high-throughput demands of real-time market data and the analytical depth needed for sophisticated technical analysis. The modular design ensures extensibility for future enhancements while maintaining a robust core for daily trading operations.
