import json
import datetime
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

class AuditLogFetcher:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("PostgresAuditLog") \
            .config("spark.jars", "/home/prasad-bhokare/python_code/tasks/config_update/config_update_v3/jars/postgresql.jar") \
            .getOrCreate()

        self.jdbc_url = "jdbc:postgresql://localhost:5432/datalake_audit"
        self.properties = {
            "user": "postgres",
            "password": "password",
            "driver": "org.postgresql.Driver"
        }

    def fetch_logs(self):
        df = self.spark.read.jdbc(url=self.jdbc_url, table="datalake_source_tracker", properties=self.properties)
        last_24_hours = datetime.now() - timedelta(days=1)

        filtered_df = df.filter((df["updated_at"] >= last_24_hours) & (df["flag"] == True))
        records = filtered_df.select("source_name", "database_name", "table_name", "table_schema", "fetch_type", 
                                     "hour_interval", "mode", "batch_size", "date_col").collect()
        
        valid_records = [
            (
                row.source_name, row.database_name, row.table_name, row.table_schema, row.fetch_type, 
                row.hour_interval, row.mode, row.batch_size, 
                row.date_col.strftime('%Y-%m-%d') if isinstance(row.date_col, datetime) else str(row.date_col)
            )
            for row in records if None not in [
                row.source_name, row.database_name, row.table_name, row.table_schema, 
                row.fetch_type, row.mode, row.date_col
            ]
        ]

        return valid_records

    def stop_spark(self):
        self.spark.stop()
