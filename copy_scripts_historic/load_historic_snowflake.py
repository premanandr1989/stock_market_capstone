from snowflake_config_handler import SnowflakeTableHandler
from spark_polygon_ingestion import create_polygon_dataframe
from pathlib import Path
from dq_checks_historic import check_ticker_data_quality


def write_data_with_quality_checks(results):
    """
    Perform data quality checks and write data to Snowflake if checks pass.

    Args:
        df: Spark DataFrame to write
        manager: Snowflake manager instance
        results: Dictionary containing data quality check results

    Returns:
        bool: True if data was written successfully, False if quality checks failed
    """
    try:
        # Data quality checks
        quality_checks = [
            # Volume checks
            (results['volume_checks']['records_with_invalid_transactions'] /
             results['volume_checks']['total_records'] * 100) < 1,
            results['volume_checks']['volume_less_than_transactions'] == 0,

            # Price consistency checks
            results['price_consistency']['high_less_than_low'] == 0,
            results['price_consistency']['open_above_high'] == 0,
            results['price_consistency']['open_below_low'] == 0,
            results['price_consistency']['close_above_high'] == 0,
            results['price_consistency']['close_below_low'] == 0,
            results['price_consistency']['negative_prices'] == 0,

            # Null percentage checks
            results['null_percentages']['ticker_null_percentage'] == 0,
            results['null_percentages']['volume_null_percentage'] == 0,
            results['null_percentages']['open_null_percentage'] == 0,
            results['null_percentages']['high_null_percentage'] == 0,
            results['null_percentages']['low_null_percentage'] == 0,
            results['null_percentages']['close_null_percentage'] == 0
        ]

        # Check names for better error reporting
        check_names = [
            "Invalid transactions percentage",
            "Volume less than transactions",
            "High less than low",
            "Open above high",
            "Open below low",
            "Close above high",
            "Close below low",
            "Negative prices",
            "Ticker null percentage",
            "Volume null percentage",
            "Open null percentage",
            "High null percentage",
            "Low null percentage",
            "Close null percentage"
        ]

        # Perform all checks and collect failures
        failed_checks = [
            check_names[i] for i, check in enumerate(quality_checks)
            if not check
        ]

        if failed_checks:
            print("Data quality checks failed for the following metrics:")
            for check in failed_checks:
                print(f"- {check}")
            return False

        return True

    except Exception as e:
        print(f"Error during data quality checks or writing: {str(e)}")
        return False


# Example usage
if __name__ == "__main__":
    try:
        # Initialize Snowflake manager
        current_dir = Path(__file__).parent
        dbt_project_path = current_dir.parent / "dbt_project"

        manager = SnowflakeTableHandler(
            project_dir= dbt_project_path,
            profile_name="jaffle_shop"
        )



        # Create sample DataFrame with ticker and date
        # spark = SparkSession.builder.appName("SnowflakeExample").getOrCreate()
        spark = manager.spark
        # sample_data = [
        #     ("BCC", "2024-02-13", 248274, 61.68, 61.99, 62.565, 61.41, 4073),
        #     ("BCC", "2024-02-14", 250000, 62.00, 62.50, 63.00, 61.80, 4200)
        # ]
        df = create_polygon_dataframe()

        results = check_ticker_data_quality(df)
        ok_to_write = write_data_with_quality_checks(results)

        if ok_to_write:
            # Write DataFrame to partitioned Snowflake table
            print("all checks passed.. proceeding to write")
            manager.write_partitioned_df(
                df=df,
                table_name="Stock_daily_agg",
                mode="overwrite",
                create_table=True
            )

        # Read back specific partition
        # result_df = manager.read_partitioned_table(
        #     table_name="STOCK_DATA",
        #     ticker="BCC",
        #     start_date="2024-02-13",
        #     end_date="2024-02-14"
        # )
        # result_df.show()

    except Exception as e:
        print(f"Error in main execution: {str(e)}")