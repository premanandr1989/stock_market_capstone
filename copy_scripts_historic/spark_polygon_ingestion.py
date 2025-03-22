def create_polygon_dataframe():
    from dags.spark_config import create_spark_session
    from pyspark.sql.functions import expr

    spark_session = create_spark_session()

    # Get default parallelism
    default_parallelism = spark_session.sparkContext.defaultParallelism
    print(f"Default Parallelism: {default_parallelism}")

    # Get driver memory
    driver_memory = spark_session.conf.get("spark.driver.memory", "8g")
    print(f"Driver Memory: {driver_memory}")

    # Get executor memory
    executor_memory = spark_session.conf.get("spark.executor.memory", "16g")
    print(f"Executor Memory: {executor_memory}")

    s3_path = "s3a://zachwilsonsorganization-522/prem-capstone-data/daily_aggs/us_stocks_sip/day_aggs_v1/*/*/*.csv"

    schema_sql = "ticker STRING,volume STRING,open STRING,close STRING,high STRING,low STRING,window_start LONG,transactions STRING"

    df= spark_session.read.option("header", "true") \
        .option("schema", schema_sql) \
        .csv(s3_path)
    # Once values are taken in, spark allows us to cast each column to their respective type
    new_df = df.withColumn("window_start", expr("CAST(from_unixtime(window_start/1e9) AS DATE)")) \
        .withColumn("close", expr("CAST(close AS DOUBLE)")) \
        .withColumn("open", expr("CAST(open AS DOUBLE)")) \
        .withColumn("high", expr("CAST(high AS DOUBLE)")) \
        .withColumn("low", expr("CAST(low AS DOUBLE)")) \
        .withColumn("volume", expr("CAST(volume AS DOUBLE)")) \
        .withColumn("transactions", expr("CAST(transactions AS INT)"))
    final_df = new_df.selectExpr('ticker','volume','open','high','low','close','transactions', 'window_start as date')
    final_df.printSchema()
    final_df.show(truncate = False)
    print("fetched the dataframe successfully")
    # final_df.where('volume<=0').show(100, truncate = False)
    return final_df

# if __name__ == '__main__':
#     create_polygon_dataframe()
    # result_df = final_df.withColumn("year", year("date")) \
    #     .withColumn("month", month("date")) \
    #     .groupBy("year", "month") \
    #     .count() \
    #     .orderBy("year", "month")

    # Display the counts per month for each year
    # result_df.show(60,truncate=False)
