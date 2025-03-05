import os
import json
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from datetime import datetime, timedelta
from dotenv import load_dotenv  # type: ignore

load_dotenv()

current_directory = os.path.dirname(os.path.abspath(__file__))
config_folder = os.path.join(current_directory, 'app', 'config')
jar_folder = os.path.join(current_directory, 'app', 'jar_files')

jar_paths = {
    "oracle": f"{jar_folder}/ojdbc8.jar",
    "postgresql": f"{jar_folder}/postgresql-42.7.4.jar",
    "mssql": f"{jar_folder}/mssql-jdbc-12.8.1.jre11.jar"
}

def get_database_jar(db_type):
    return jar_paths.get(db_type, None)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

config_file_path = os.path.join(config_folder, 'config.json')

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

        for source in config['sources']:
            source_name = source['name']
            source_type = source['database_type']
            source_params = json.dumps(source['param'])  
            destination_type = config['destinations'][0]['database_type']
            destination_params = json.dumps(config['destinations'][0]['param'])  
            spark_operator_args = config['spark_operator_args'][0]  
            source_jar = get_database_jar(source_type)
            destination_jar = get_database_jar(destination_type)
            jars = [jar for jar in [source_jar, destination_jar] if jar]


            jars_list = jars if jars else []  
            jars_str = ",".join(jars_list)

            task = SparkSubmitOperator(
                task_id=f"process_{source_name}",
                application=f"{current_directory}/spark_job.py",
                conn_id="spark_datalake_audit",
                jars=jars_str,  
                application_args=[
                    '--source_name', source_name,
                    '--source_type', source_type,
                    '--source_params', source_params,
                    '--destination_type', destination_type,
                    '--destination_params', destination_params,
                    '--jars', jars_str  
                ],
                verbose=True,
                dag=dag,
                env_vars={
                    "HADOOP_CONF_DIR": "/home/iauro/hadoop_env/hadoop/etc/hadoop",
                    "YARN_CONF_DIR": "/home/iauro/hadoop_env/hadoop/etc/hadoop"
                },
                conf={
                        "spark.executor.memory": spark_operator_args.get('executor_memory', "4g"),
                        "spark.executor.cores": spark_operator_args.get('executor_cores', "4"),
                        "spark.driver.memory": spark_operator_args.get('driver_memory', "4g"),
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