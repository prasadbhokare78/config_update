import sys
import time
import json
import argparse
from pyspark.sql import SparkSession
from app.connectors.postgres_connector import PostgresConnector
from app.utils.update_config import UpdateConfig

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_name', required=True)
    parser.add_argument('--source_type', required=True)
    parser.add_argument('--source_params', required=True, help="JSON string of source parameters")
    parser.add_argument('--jar_path', required=True, help="Path to the JDBC jar file")
    parser.add_argument('--schedule_time', required=True)
    return parser.parse_args()


def create_spark_session(source_name, jar_path):
    spark_attempts = 0
    max_retries = 3
    retry_delay = 5

    while spark_attempts < max_retries:
        try:
            spark = SparkSession.builder \
                .appName(f"spark_{source_name}") \
                .config("spark.jars", jar_path) \
                .getOrCreate()
            return spark
        except Exception as e:
            spark_attempts += 1
            print(f"Failed to create Spark session. Attempt {spark_attempts} of {max_retries}. Error: {e}")
            if spark_attempts < max_retries:
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to create Spark session after {max_retries} attempts. Error: {e}")


def get_connector(source_name, source_params, jar_path):
    try:
        source_params = json.loads(source_params)  
        host = source_params["host"]
        port = source_params["port"]
        user = source_params["user"]
        password = source_params["password"]
        database = source_params["database"]

        spark = create_spark_session(source_name, jar_path)
        return PostgresConnector(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            spark=spark
        )
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in source_params: {e}")
    except Exception as e:
        raise RuntimeError(f"Error in get_connector: {e}")


def main():
    try:
        args = parse_args()

        source_name = args.source_name
        source_type = args.source_type
        source_params = args.source_params 
        jar_path = args.jar_path
        schedule_time = args.schedule_time

        connector = get_connector(source_name, source_params, jar_path)
        source_params  = json.loads(source_params)

        UpdateConfig(source_name, source_type, source_params, schedule_time, connector).update_config_files()

        connector.stop_spark_session()

        print('Spark job completed successfully')

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
