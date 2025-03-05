from datetime import datetime

def fetch_audit_logs(connector, table_name):
    query = f"""
        SELECT * FROM {table_name} t1
        WHERE updated_at = (
            SELECT MAX(updated_at) 
            FROM {table_name} t2
            WHERE t1.source_name = t2.source_name 
              AND t1.database_type = t2.database_type 
              AND t1.database_name = t2.database_name
        )
    """
    
    df = connector.read_table(query)

    filtered_df = df.filter(df["flag"] == True)

    records = filtered_df.select(
        "source_name", "database_type", "database_name", "table_name", "table_schema", "fetch_type", 
        "hour_interval", "mode", "batch_size", "executor_memory", "executor_cores", 
        "driver_memory", "min_executors", "max_executors", "initial_executors", "driver_cores", "partition_upto", "date_column",
        "mod_date_column", "add_date_column", "min_date_column"

    ).collect()

    valid_records = [
        (
            row.source_name, row.database_type, row.database_name, row.table_name, row.table_schema, row.fetch_type, 
            row.hour_interval, row.mode, row.batch_size, row.executor_memory, row.executor_cores, 
            row.driver_memory, row.min_executors, row.max_executors, row.initial_executors, row.driver_cores, row.partition_upto,
            row.date_column.strftime('%Y-%m-%d') if isinstance(row.date_column, datetime) else str(row.date_column), 
            row.mod_date_column, row.add_date_column, row.min_date_column
        )
        for row in records if None not in [
            row.source_name, row.database_type, row.database_name, row.table_name, row.table_schema, 
            row.fetch_type, row.mode, row.date_column, row.executor_memory, row.executor_cores, 
            row.driver_memory, row.min_executors, row.max_executors, row.initial_executors, row.driver_cores, row.partition_upto,
            row.min_date_column
        ] and (row.mod_date_column is not None or row.add_date_column is not None)  
    ]

    return valid_records
