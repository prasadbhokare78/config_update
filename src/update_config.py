import os
import json
from src.fetch_audit_logs import AuditLogFetcher
from src.config_writer import ConfigWriter

class ConfigUpdater:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_writer = ConfigWriter(self.script_dir)
        self.json_config_path = os.path.join(self.script_dir, "../config.json")
        self.audit_log_fetcher = AuditLogFetcher()

    def load_json_config(self):
        if not os.path.exists(self.json_config_path):
            print("Config file not found. Exiting.")
            return None

        with open(self.json_config_path, "r") as f:
            return json.load(f)

    def update_config_files(self):
        json_config = self.load_json_config()
        if json_config is None:
            return

        audit_logs = self.audit_log_fetcher.fetch_logs()
        if not audit_logs:
            print("No audit logs found. Exiting.")
            return

        configs = {}

        for log in audit_logs:
            source_name, database_name, table_name, table_schema, fetch_type, hour_interval, mode, batch_size, date_col = log

            try:
                table_schema = json.loads(table_schema)  # Parse table_schema as JSON
            except json.JSONDecodeError:
                print(f"Failed to parse JSON for {table_name}. Using empty schema.")
                table_schema = {}

            file_name = f"{source_name}_{database_name}.json".lower()
            configs[file_name] = self.config_writer.generate_config(json_config, source_name, table_name, table_schema, fetch_type, hour_interval, mode, batch_size, date_col)

        self.config_writer.write_updated_configs(configs)
