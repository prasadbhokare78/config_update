from pyspark.sql.functions import col, collect_list, to_json

def source_mssql_data(source_connector, source_name, database_type):
    exclude_dbs = ('master', 'tempdb', 'model', 'msdb')
    
    databases_query = f"""
        SELECT name 
        FROM sys.databases 
        WHERE name NOT IN {exclude_dbs}
    """
    databases_df = source_connector.read_table(databases_query)
    
    results = []
    
    for row in databases_df.collect():
        db_name = row["name"]
        source_connector.set_url(db_name)

        table_query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """
        
        try:
            tables_df = source_connector.read_table(table_query)
            
            for table_row in tables_df.collect():
                table_name = table_row["TABLE_NAME"]
                
                
                count_query = f"SELECT COUNT(*) AS row_count FROM {table_name}"
                row_count_df = source_connector.read_table(count_query)
                row_count = row_count_df.collect()[0]["row_count"]
                
                
                schema_query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}'
                """
                schema_df = source_connector.read_table(schema_query)
                
                schema_dict = schema_df.select(
                    collect_list(col("COLUMN_NAME")).alias("columns"),
                    collect_list(col("DATA_TYPE")).alias("datatypes")
                ).collect()[0]
                
                schema_json = dict(zip(schema_dict["columns"], schema_dict["datatypes"]))
            
                results.append((source_name, database_type, db_name, table_name, row_count, schema_json))
                
        except Exception as e:
            print(f"Error processing {db_name}: {e}")
    
    schema = ["source_name", "database_type", "database_name", "table_name", "row_count", "table_schema"]
    source_mssql_df = source_connector.create_dataframe(results, schema)
    source_mssql_df = source_mssql_df.withColumn("table_schema", to_json(col("table_schema")))
    
    

    return source_mssql_df