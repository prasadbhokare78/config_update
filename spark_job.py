import sys
import time
import json 
import argparse
from pyspark.sql import SparkSession
from app.connectors.postgres_connector import PostgresConnector
from app.connectors.mssql_connector import MSSQLConnector
from app.connectors.oracle_connector import OracleConnector
from app.utils.db_handler import source_handler, destination_handler

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_name', required=True)
    parser.add_argument('--source_type', required=True)
    parser.add_argument('--source_params', required=True)
    parser.add_argument('--destination_type', required=True)
    parser.add_argument('--destination_params', required=True)
    parser.add_argument('--jars', required=True)
    return parser.parse_args()


def create_spark_session(source_name,jar_path):
    spark_attempts = 0
    max_retries = 3
    retry_delay = 5

    while spark_attempts < max_retries:
        try:
            spark = SparkSession.builder \
                .appName(f"spark_{source_name}") \
                .config("spark.jars", f"{jar_path[0]}, {jar_path[1]}") \
                .getOrCreate()
            return spark
        except Exception as e:
            spark_attempts += 1
            print(f"Failed to create Spark session. Attempt {spark_attempts} of {max_retries}. Error: {e}")
            if spark_attempts < max_retries:
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to create Spark session after {max_retries} attempts. Error: {e}")
            

def get_destination_connector(destination_type, destination_params, jars):
    """Returns the PostgreSQL destination connector."""
    host=destination_params.get("host")
    port=destination_params.get("port")
    database=destination_params.get("database")
    user=destination_params.get("user")
    password=destination_params.get("password")
    spark = create_spark_session(destination_type, jars)

    return PostgresConnector(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,  
        spark=spark
        )


def get_source_connector(source_name, source_type, source_params, jars):
    host=source_params.get("host")
    port=source_params.get("port")
    user=source_params.get("user")
    password=source_params.get("password")
    spark = create_spark_session(source_name, jars)


    if source_type == "mssql":
        return MSSQLConnector(
            host=host,
            port=port,
            user=user,
            password=password,
            spark=spark
        )
    elif source_type == "postgresql":
        return PostgresConnector(
            host=host,
            port=port,
            user=user,
            password=password,
            spark=spark
        )
    elif source_type == "oracle":
        return OracleConnector(
            host=host,
            port=port,
            user=user,
            password=password,
            spark=spark
        )


def main():
    print('Starting the Spark job')

    try:
        args = parse_args()

        source_name = args.source_name
        source_type = args.source_type
        source_params = json.loads(args.source_params)  
        destination_type = args.destination_type
        destination_params = json.loads(args.destination_params) 
        jars = args.jars.split(",")  
        destination_table = destination_params.get("table_name")       
        print(source_type, '-',jars)
        source_connector = get_source_connector(source_name, source_type, source_params, jars)
        fetch_data = source_handler(source_connector, source_name, source_type)
        
        destination_connector = get_destination_connector(destination_type, destination_params, jars)
        destination_handler(destination_connector, fetch_data, destination_table)
        source_connector.stop_spark_session()
        destination_connector.stop_spark_session()
        print("Spark job completed successfully")


    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
