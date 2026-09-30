from datetime import datetime, timedelta
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.hooks.filesystem import FSHook

PROJECT_HOME = "/home/yamini/telecom-etl"
READY_FILE = "usage.ready"
PASSWORD_FILE = f"{PROJECT_HOME}/config/mysql.password"

def verify_ready_file():
    hook = FSHook(fs_conn_id="telecom_files")
    base_path = hook.get_path()
    ready_path = os.path.join(base_path, READY_FILE)

    print("Filesystem path:", base_path)
    print("Checking file:", ready_path)

    if not os.path.isfile(ready_path):
        raise FileNotFoundError(f"Ready file not found: {ready_path}")

    print("READY FILE FOUND - Telecom usage data is ready.")

with DAG(
    dag_id="telecom_customer_usage_etl",
    start_date=datetime(2026, 9, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args={
        "owner": "data_engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["telecom", "etl", "sqoop", "hdfs", "hive"],
) as dag:

    wait_for_ready_file = FileSensor(
        task_id="wait_for_usage_ready_file",
        filepath=READY_FILE,
        fs_conn_id="telecom_files",
        poke_interval=30,
        timeout=30 * 60,
        mode="reschedule",
    )

    verify_file = PythonOperator(
        task_id="verify_file_using_hook",
        python_callable=verify_ready_file,
    )

    sqoop_extract = BashOperator(
        task_id="sqoop_extract_mysql_to_hdfs",
        bash_command=f"""
        echo "================================="
        echo "Starting Sqoop Extraction"
        echo "================================="

        hdfs dfs -rm -r -f /telecom/raw/customer_usage

        sqoop import \\
        --connect jdbc:mysql://localhost:3306/telecom_db \\
        --username sqoop_user \\
        --password-file file://{PASSWORD_FILE} \\
        --table customer_usage \\
        --target-dir /telecom/raw/customer_usage \\
        --fields-terminated-by ',' \\
        --m 1

        echo "Sqoop extraction completed."
        hdfs dfs -ls /telecom/raw/customer_usage
        """,
    )

    hive_transform = BashOperator(
        task_id="hive_transform_and_load",
        bash_command=f"""
        echo "================================="
        echo "Starting Hive Transformation"
        echo "================================="

        hive -f {PROJECT_HOME}/scripts/telecom_transform.sql

        echo "Hive transformation completed."
        """,
    )

    usage_summary = BashOperator(
        task_id="generate_usage_summary",
        bash_command="""
        echo "================================="
        echo "Customer Usage Summary"
        echo "================================="

        hive -e "
        USE telecom_dw;
        SELECT
            customer_id,
            plan,
            total_call_minutes,
            total_sms,
            total_data_gb
        FROM customer_usage_summary
        ORDER BY total_data_gb DESC;
        "
        """,
    )

    pipeline_complete = BashOperator(
        task_id="pipeline_complete",
        bash_command="""
        echo "======================================"
        echo " TELECOM ETL PIPELINE COMPLETED"
        echo "======================================"
        """,
    )

    wait_for_ready_file >> verify_file >> sqoop_extract >> hive_transform >> usage_summary >> pipeline_complete