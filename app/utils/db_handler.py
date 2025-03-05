from app.utils.source_postgres import source_postgres_data
from app.utils.source_mssql import source_mssql_data
from app.utils.source_oracle import source_oracle_data
from pyspark.sql.functions import col, current_date
from pyspark.sql.types import DateType


def update_postgres(missing_df, connector, table_name):
    try:
        if hasattr(missing_df, "count") and callable(getattr(missing_df, "count")):
            if missing_df.count() > 0:
                connector.write_table(missing_df, table_name)
                print("Successfully inserted missing entries into PostgreSQL.")
            else:
                print("No missing records to insert.")
        else:
            raise TypeError(f"Expected a PySpark DataFrame but got {type(missing_df)}")
    except Exception as e:
        print(f"Error inserting into PostgreSQL: {e}")


def missing_destination(destination_connector, source_df):
    schema = ["source_name", "database_type", "database_name", "table_name", "row_count", "table_schema"]
    
    try:
        query = "SELECT source_name, database_type, database_name, table_name, row_count, table_schema FROM public.datalake_source_tracker"
        dest_df = destination_connector.read_table(query)
    except Exception as e:
        print(f"⚠️ Error reading from MSSQL Destination: {e}")
        destination_connector.stop_spark_session()
        exit()

    source_df = source_df.select(*schema)
    dest_df = dest_df.select(*schema)

    missing_in_dest_df = source_df.join(
        dest_df,
        on=["source_name", "database_type", "database_name", "table_name"],
        how="left_anti"
    )

    missing_in_dest_df = missing_in_dest_df.withColumn("created_at", current_date())
    missing_in_dest_df = missing_in_dest_df.withColumn("updated_at", current_date().cast(DateType()))

    missing_in_dest_df = missing_in_dest_df.withColumn("table_schema", col("table_schema").cast("STRING"))

    return missing_in_dest_df


def destination_handler(destination_connector, fetch_data, destination_table):
    """Handles data updates in the destination database (PostgreSQL)."""
    
    missing_data = missing_destination(destination_connector, fetch_data)
    update_postgres(missing_data, destination_connector, destination_table)


def source_handler(source_connector, source_name, source_type):

    if source_type == 'oracle':
        fetch_data = source_oracle_data(source_connector, source_name, source_type)
    elif source_type == 'mssql':
        fetch_data = source_mssql_data(source_connector, source_name, source_type)
    elif source_type == 'postgresql':
        fetch_data = source_postgres_data(source_connector, source_name, source_type)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    return fetch_data