### Start Execution (main.py)

Runs ConfigUpdater.update_config_files() to begin processing.

### Fetch Audit Logs (fetch_audit_logs.py)

Connects to PostgreSQL, retrieves audit logs with table details and metadata.

### Load Configuration (update_config.py)

Loads config.json, exits if missing or invalid.

### Process Logs (update_config.py)

Extracts table details, converts table_schema to a dictionary, generates config file names.

### Generate Configurations (config_writer.py)

Updates config with queries for data retrieval, batch processing, and incremental fetch.

### Write Config Files (config_writer.py)

Saves updated configurations as JSON files in the config/ directory.

### Completion

Prints "Config update completed successfully," ready for use.