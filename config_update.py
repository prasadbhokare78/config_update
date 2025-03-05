import os
import json
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from datetime import datetime, timedelta
from dotenv import load_dotenv  # type: ignore 

load_dotenv()

current_directory = os.path.dirname(os.path.abspath(__file__))
config_folder = os.path.join(current_directory, 'app', 'config')
utils_folder = os.path.join(current_directory, 'app', 'utils')
jar_folder = os.path.join(current_directory, 'app', 'jar_files')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

config_file_path = os.path.join(config_folder, 'config_update.json')
if os.path.exists(config_file_path):
    try:
        with open(config_file_path) as f:
            config = json.load(f)
        
        dag_name = config['dag_config']['dag_name']
        schedule_time = config['dag_config']['schedule_time']
        
        dag = DAG(
            dag_id=dag_name,
            default_args=default_args,
            description=f"Dynamic DAG for {dag_name}",
            schedule=schedule_time,
            catchup=False,
        )

        source_name = config['source'][0]['name']
        source_type = config['source'][0]['database_type']
        source_params = config['source'][0]['param']
        schedule_time = config['dag_config']['schedule_time']
        spark_operator_args = config['spark_operator_args'][0]
        jar_path = f"{jar_folder}/postgresql-42.7.4.jar"
        print(utils_folder)

        task = SparkSubmitOperator(
            task_id=dag_name,
            application=f'{current_directory}/spark_job.py',
            conn_id='spark_config_update',
            jars=jar_path,
            application_args=[
                '--source_name', source_name,
                '--source_type', source_type,
                '--source_params', json.dumps(source_params),
                '--jar_path', jar_path,
                '--schedule_time', schedule_time
            ],
            verbose=True,
            dag=dag,
            env_vars={
                "HADOOP_CONF_DIR": "/home/iauro/hadoop_env/hadoop/etc/hadoop",
                "YARN_CONF_DIR": "/home/iauro/hadoop_env/hadoop/etc/hadoop"
            },
            conf={
                    "spark.executor.memory": spark_operator_args.get('executor_memory', "2g"),
                    "spark.executor.cores": spark_operator_args.get('executor_cores', "2"),
                    "spark.driver.memory": spark_operator_args.get('driver_memory', "2g"),
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.dynamicAllocation.minExecutors": spark_operator_args.get('min_executors', "2"),
                    "spark.dynamicAllocation.maxExecutors": spark_operator_args.get('max_executors', "2"),
                    "spark.dynamicAllocation.initialExecutors": spark_operator_args.get('initial_executors', "2"),
                    "spark.driver.cores": spark_operator_args.get('driver_cores', "2"),
                    "spark.worker.cleanup.enabled": "true",
                    "spark.worker.cleanup.interval": "30",
                    "spark.worker.cleanup.appDataTtl": "30",
                    "spark.jars.ignore": "true"
                }
        )
        globals()[dag_name] = dag
    except (FileNotFoundError, json.JSONDecodeError, KeyError, AttributeError) as e:
        print(f"Error processing config file: {e}") 