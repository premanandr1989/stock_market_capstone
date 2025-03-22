import yaml
import os
from pathlib import Path
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
import snowflake.connector
from dotenv import load_dotenv
from typing import Dict, Optional

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
                role=self.sf_options['sfRole']
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