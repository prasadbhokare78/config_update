import os
import json

class ConfigWriter:
    def __init__(self, script_dir):
        self.config_dir = os.path.join(script_dir, "../config")
        os.makedirs(self.config_dir, exist_ok=True)

    def generate_config(self, json_config, source_name, table_name, table_schema, fetch_type, hour_interval, mode, batch_size, date_col):
        file_path = os.path.join(self.config_dir, f"{source_name}.json".lower())

        existing_config = self.load_existing_config(file_path, json_config)
        matching_sources = [s for s in json_config["sources"] if s["name"] == source_name]

        if not matching_sources:
            print(f"No matching source found for {source_name}. Skipping.")
            return None

        existing_config["sources"] = matching_sources[0]
        self.update_table_queries(existing_config, table_name, table_schema, fetch_type, hour_interval, mode, batch_size, date_col)
        return existing_config

    def load_existing_config(self, file_path, json_config):
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                return json.load(f)
        else:
            return {
                "dag_config": {
                    "dag_name": f"{json_config['sources'][0]['name']}_config_dag".lower(),
                    "schedule_time": json_config["schedule_time"]
                },
                "sources": {},
                "destination": json_config["destination"],
                "table_queries": []
            }

    def update_table_queries(self, existing_config, table_name, table_schema, fetch_type, hour_interval, mode, batch_size, date_col):
        existing_table_entry = next((tq for tq in existing_config["table_queries"] if tq["table_name"] == table_name), None)

        if fetch_type == "batch" and (batch_size is None or hour_interval is None):
            if existing_table_entry:
                print(f"Skipping update for {table_name} as batch_size or hour_interval is missing")
                return

        existing_config["table_queries"] = [tq for tq in existing_config.get("table_queries", []) if tq["table_name"] != table_name]

        total_count_query = f"SELECT count(*) AS total_count FROM {table_name} WHERE {date_col} >= '{{start_date_time}}' AND {date_col} < '{{end_date_time}}'"
        if fetch_type == "batch":
            total_count_query += " ORDER BY {date_col} ASC, name ASC OFFSET {offset} LIMIT {batch_size}"

        table_params = {
            "data_query": f"SELECT * FROM {table_name} WHERE {date_col} >= '{{start_date_time}}' AND {date_col} < '{{end_date_time}}'",
            "date_column": date_col,
            "min_date_query": f"SELECT min({date_col}) AS min_date FROM {table_name}",
            "total_count_query": total_count_query,
            "schema": table_schema,
            "fetch_type": fetch_type,
            "mode": mode
        }

        if fetch_type == "batch":
            table_params["batch_size"] = batch_size if batch_size is not None else existing_table_entry.get("batch_size")
            table_params["batch_interval_in_hr"] = hour_interval if hour_interval is not None else existing_table_entry.get("batch_interval_in_hr")

        existing_config["table_queries"].append({
            "table_name": table_name,
            "table_params": table_params
        })

    def write_updated_configs(self, configs):
        for file_name, config in configs.items():
            if config:
                file_path = os.path.join(self.config_dir, file_name)
                print(f"Writing config to {file_path}")
                with open(file_path, "w") as f:
                    json.dump(config, f, indent=4, default=str)
