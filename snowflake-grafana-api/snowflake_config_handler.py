import yaml, time
import os
from pathlib import Path
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
import snowflake.connector
from dotenv import load_dotenv
from typing import Dict, Optional
# from spark_config import create_spark_session
from snowflake.connector.errors import DatabaseError, ProgrammingError, OperationalError
from functools import wraps


# from snowflake_config_handler import load_snowflake_config


class SnowflakeTableHandler:
    def __init__(self,
                 project_dir: str,
                 profile_name: str = "default"):
        """
        Initialize Snowflake manager with DBT project configuration

        Args:
            project_dir: Path to dbt_project directory
            profile_name: Name of the profile to use from profiles.yml
        """
        self.project_dir = Path(project_dir)
        self.profile_name = profile_name
        self.conn = None
        self.sf_options = self._load_config()
        # self.spark = create_spark_session()

    def _load_config(self) -> Dict[str, str]:
        """Load configuration from DBT project files"""
        profile_path = self.project_dir / "profiles.yml"
        env_path = self.project_dir / "dbt.env"

        # Load environment variables if they exist
        if env_path.exists():
            load_dotenv(env_path)

        # Read profiles.yml
        with open(profile_path, 'r') as f:
            profiles = yaml.safe_load(f)

        profile = profiles[self.profile_name]
        outputs = profile.get('outputs', {})
        target_name = profile.get('target', next(iter(outputs.keys())))
        target_config = outputs[target_name]

        # Create connection parameters with env var overrides
        return {
            "sfAccount": os.getenv('SNOWFLAKE_ACCOUNT', target_config['account']),
            "sfUser": os.getenv('SNOWFLAKE_USER', target_config['user']),
            "sfPassword": os.getenv('SNOWFLAKE_PASSWORD', target_config['password']),
            "sfDatabase": os.getenv('SNOWFLAKE_DATABASE', target_config['database']),
            "sfSchema": os.environ.get("DBT_SCHEMA"),
            "sfWarehouse": os.getenv('SNOWFLAKE_WAREHOUSE', target_config['warehouse']),
            "sfRole": os.getenv('SNOWFLAKE_ROLE', target_config.get('role', 'ACCOUNTADMIN'))
        }

    def with_retry(max_retries=3, delay=2):
        """Decorator for retry logic"""

        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                retries = 0
                while retries < max_retries:
                    try:
                        return func(self, *args, **kwargs)
                    except (DatabaseError, OperationalError) as e:
                        error_msg = str(e).lower()
                        retries += 1
                        self.logger.warning(f"Connection error: {error_msg}. Retry {retries}/{max_retries}")

                        # Force reconnection on specific errors
                        if "connection" in error_msg or "timeout" in error_msg:
                            self.conn = None

                        if retries < max_retries:
                            time.sleep(delay * retries)  # Exponential backoff
                        else:
                            self.logger.error(f"Max retries reached: {error_msg}")
                            raise

            return wrapper

        return decorator

    @with_retry(max_retries=3)
    def connect(self):
        """Establish Snowflake connection"""
        if not self.conn:
            self.conn = snowflake.connector.connect(
                user=self.sf_options['sfUser'],
                password=self.sf_options['sfPassword'],
                account=self.sf_options['sfAccount'],
                warehouse=self.sf_options['sfWarehouse'],
                database=self.sf_options['sfDatabase'],
                schema=self.sf_options['sfSchema'],
                role=self.sf_options['sfRole'],

                # Add these timeout parameters
                socket_timeout=300,  # Socket operations timeout in seconds
                network_timeout=600,  # Network operations timeout in seconds
                login_timeout=600,  # Login timeout in seconds
                ocsp_response_cache_timeout=86400,  # OCSP response cache timeout

                client_session_keep_alive=True,  # Keep session alive
                client_prefetch_threads=10,  # Parallel query execution
                client_reduce_integration_timeout=True  # Red
            )

    def validate_connection(self):
        """Validate that all required parameters are present"""
        conn_params = self._load_config()
        required_params = [
            "sfAccount", "sfUser", "sfPassword",
            "sfDatabase", "sfSchema", "sfWarehouse"
        ]

        missing_params = [
            param for param in required_params
            if not conn_params.get(param)
        ]

        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")

        return True

    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            self.conn = None


    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in Snowflake"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = '{table_name.upper()}'
                    AND TABLE_SCHEMA = '{self.sf_options['sfSchema'].upper()}'
                """)
                return cursor.fetchone()[0] > 0
        except Exception as e:
            print(f"Error checking table existence: {str(e)}")
            raise

    def create_partitioned_table(self, df, table_name: str, mode: str = "fail", cluster_cols = None):
        """
        Create table in Snowflake based on DataFrame schema

        Parameters:
        - df: PySpark DataFrame
        - table_name: Name of table to create
        - mode: How to handle existing table ("fail", "replace", "append")
        """
        # Generate CREATE TABLE statement from DataFrame schema
        column_definitions = []
        for field in df.schema.fields:
            sf_type = self._map_spark_to_snowflake_type(field.dataType)
            nullable = "NULL" if field.nullable else "NOT NULL"
            column_definitions.append(f"{field.name} {sf_type} {nullable}")

        clustering_columns = cluster_cols
        print(clustering_columns)

        print("executing create statement")
        # Add partitioning clause
        create_stmt = f"""
            CREATE {"OR REPLACE " if mode == "overwrite" else ""}TABLE {self.sf_options['sfSchema']}.{table_name} (
                {",".join(column_definitions)}
            )
            {f"CLUSTER BY ({', '.join(clustering_columns)})" if clustering_columns else ""}
            """

        try:
            with self.conn.cursor() as cursor:
                if mode == "fail" and self.table_exists(table_name):
                    raise Exception(f"Table {table_name} already exists")
                elif mode == 'overwrite':
                    cursor.execute(create_stmt)  # Skip creation if table exists
                else:
                    return
                print(f"Successfully created partitioned table: {self.sf_options['sfSchema']}.{table_name}")

        except Exception as e:
            print(f"Error creating table: {str(e)}")
            raise

    # def _map_spark_to_snowflake_type(self, spark_type):
    #     """Map Spark data types to Snowflake data types"""
    #     type_mapping = {
    #         StringType: "VARCHAR",
    #         IntegerType: "INTEGER",
    #         LongType: "BIGINT",
    #         FloatType: "FLOAT",
    #         DoubleType: "DOUBLE",
    #         DecimalType: "DECIMAL",
    #         BooleanType: "BOOLEAN",
    #         DateType: "DATE",
    #         TimestampType: "TIMESTAMP",
    #         BinaryType: "BINARY",
    #         ArrayType: "ARRAY",
    #         MapType: "OBJECT",
    #         StructType: "OBJECT"
    #     }
    #
    #     for spark_type_class, snowflake_type in type_mapping.items():
    #         if isinstance(spark_type, spark_type_class):
    #             return snowflake_type
    #     return "VARCHAR"  # Default type

    def alter_table_partitioned(self,table_name,cluster_cols):
        alter_stmt = f"""ALTER TABLE {self.sf_options['sfSchema']}.{table_name} {f"CLUSTER BY ({', '.join(cluster_cols)})" if cluster_cols else ""}"""
        with self.conn.cursor() as cursor:
            cursor.execute(alter_stmt)
            print(f"Clustering keys set on {self.sf_options['sfSchema']}.{table_name}")


    def write_partitioned_df(self, df, table_name: str, mode: str = "overwrite", create_table: bool = True, cluster_cols =  None):
        """
        Write DataFrame to Snowflake with partitioning by ticker and date

        Parameters:
        - df: PySpark DataFrame
        - table_name: Name of table to create
        - mode: How to handle existing table ("fail", "replace", "append")
        - create_table: Whether to create the table if it doesn't exist
        """
        try:
            # Ensure required partition columns exist
            if cluster_cols:
                missing_cols = [col for col in cluster_cols if col not in df.columns]
                if missing_cols:
                    raise ValueError(f"Missing required columns for partitioning: {missing_cols}")

            # Connect if not already connected
            if not self.conn:
                self.connect()

            # Create table if needed
            if create_table:
                self.create_partitioned_table(df, table_name, mode, cluster_cols)

            # Qualify table name with schema
            full_table_name = f"{self.sf_options['sfSchema']}.{table_name}"

            # Configure JDBC URL and properties
            jdbc_url = f"jdbc:snowflake://{self.sf_options['sfAccount']}.snowflakecomputing.com"

            connection_properties = {
                "user": self.sf_options['sfUser'],
                "password": self.sf_options['sfPassword'],
                "warehouse": self.sf_options['sfWarehouse'],
                "db": self.sf_options['sfDatabase'],
                "schema": self.sf_options['sfSchema'],
                "role": self.sf_options['sfRole'],
                "driver": "net.snowflake.client.jdbc.SnowflakeDriver"
            }

            if cluster_cols:
                df = df.repartition(*cluster_cols)

            # Write data using PySpark Snowflake connector
            df .write \
                .jdbc(url=jdbc_url,
                      table=full_table_name,
                      mode='append',
                      properties=connection_properties)

            if cluster_cols:
                self.alter_table_partitioned(table_name, cluster_cols)

            print(f"Successfully wrote partitioned data to table: {full_table_name}")

        except Exception as e:
            print(f"Error writing partitioned DataFrame: {str(e)}")
            raise
        finally:
            self.close()


    def read_partitioned_table(self, table_name: str, ticker: Optional[str] = None, start_date: Optional[str] = None,
                               end_date: Optional[str] = None):
        """
        Read from partitioned Snowflake table with optional filters

        Parameters:
        - table_name: Name of table to read
        - ticker: Optional ticker symbol to filter by
        - start_date: Optional start date for filtering (YYYY-MM-DD)
        - end_date: Optional end date for filtering (YYYY-MM-DD)
        """

        try:
            # Build query with filters
            query = f"SELECT * FROM {self.sf_options['sfSchema']}.{table_name}"
            filters = []

            if ticker:
                filters.append(f"ticker = '{ticker}'")
            if start_date:
                filters.append(f"DATE(date) >= '{start_date}'")
            if end_date:
                filters.append(f"DATE(date) <= '{end_date}'")

            if filters:
                query += " WHERE " + " AND ".join(filters)

            # Configure JDBC URL and properties
            jdbc_url = f"jdbc:snowflake://{self.sf_options['sfAccount']}.snowflakecomputing.com"

            connection_properties = {
                "user": self.sf_options['sfUser'],
                "password": self.sf_options['sfPassword'],
                "warehouse": self.sf_options['sfWarehouse'],
                "db": self.sf_options['sfDatabase'],
                "schema": self.sf_options['sfSchema'],
                "role": self.sf_options['sfRole'],
                "driver": "net.snowflake.client.jdbc.SnowflakeDriver"
            }

            # Read data using query
            df = self.spark.read \
                .jdbc(url=jdbc_url,
                      table=f"({query})",
                      properties=connection_properties)

            return df

        except Exception as e:
            print(f"Error reading partitioned table: {str(e)}")
            raise