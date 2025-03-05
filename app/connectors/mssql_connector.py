from pyspark.sql import SparkSession
import time
from app.jar_files.jar_manager import JarManager


class MSSQLConnector:
    def __init__(self, host, port, user, password, spark, database="master"):
        self.jdbc_url = f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
        self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        self.user = user
        self.password = password
        self.spark = spark
        self.spark_attempts = 0
        self.read_attempts = 0
        self.max_retries = 3
        self.retry_delay = 5
        self.host = host
        self.port = port
        
        # jar_manager = JarManager(
        #     required_jars=[
        #         'postgresql-42.7.4.jar',
        #         'mssql-jdbc-12.8.1.jre11.jar'
        #     ]
        # )
        # self.jdbc_drivers_path = jar_manager.get_jars()
        # self.all_jdbc_drivers_path = ",".join(self.jdbc_drivers_path)

        # while self.spark_attempts < self.max_retries:
        #     try:
        #         self.spark = SparkSession.builder \
        #             .appName("MSSQLConnection") \
        #             .config("spark.jars", self.all_jdbc_drivers_path) \
        #             .getOrCreate()
        #         break
        #     except Exception as e:
        #         self.spark_attempts += 1
        #         print(f"Failed to create Spark session. Attempt {self.spark_attempts} of {self.max_retries}. Error: {e}")
        #         if self.spark_attempts < self.max_retries:
        #             time.sleep(self.retry_delay)
        #         else:
        #             raise Exception(str(e))
                
    def set_url(self, database):
        """Set the schema (namespace) for PostgreSQL queries."""
        self.jdbc_url = f"jdbc:sqlserver://{self.host}:{self.port};databaseName={database};encrypt=true;trustServerCertificate=true"

    def create_dataframe(self, results, schema_list):
        """
        Create a Spark DataFrame from a list of tuples and a schema list.
        
        :param results: List of tuples containing the data.
        :param schema_list: List of column names as strings.
        :return: Spark DataFrame.
        """
        try:
            df = self.spark.createDataFrame(results, schema=schema_list)
            return df
        except Exception as e:
            print(f"Error creating DataFrame: {e}")
            raise Exception(str(e))


    def read_table(self, query):
        while self.read_attempts < self.max_retries:
            try:
                return self.spark.read \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("query", query) \
                    .option("user", self.user) \
                    .option("password", self.password) \
                    .option("driver", self.driver) \
                    .load()
            except Exception as e:
                self.read_attempts += 1
                print(f"Failed to read data. Attempt {self.read_attempts} of {self.max_retries}. Error: {e}")
                if self.read_attempts < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise Exception(str(e))
                
    def write_table(self, df, table_name, mode="append"):
        """
        Write a Spark DataFrame to a PostgreSQL table.
        
        :param df: Spark DataFrame to write.
        :param table_name: Name of the table to write into.
        :param mode: Writing mode ('append', 'overwrite', etc.). Default is 'append'.
        """
        while self.write_attempts < self.max_retries:
            try:
                df.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", self.user) \
                    .option("password", self.password) \
                    .option("driver", self.driver) \
                    .mode(mode) \
                    .save()
                print(f"Data successfully written to {table_name}.")
                return
            except Exception as e:
                self.write_attempts += 1
                print(f"Failed to write data. Attempt {self.write_attempts} of {self.max_retries}. Error: {e}")
                if self.write_attempts < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise Exception(str(e))


    def stop_spark_session(self):
        try:
            self.spark.stop()
            print("Spark session stopped successfully.")
        except Exception as e:
            raise Exception(str(e))