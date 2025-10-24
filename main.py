from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.email import send_email

from datetime import timedelta
from page_counts.pageview_data import setup_environment, download_data, extract_gzip_file, filter_to_csv, csv_to_database, cleanup_files

TABLE_NAME = "Wikipedia_views"

def custom_failure_alert(context):
    ti = context.get('ti')
    subject = f"Task {ti.task_id} failed in DAG {ti.dag_id}"
    body = f"""
    Dear User,

    Task {ti.task_id} failed.
    DAG: {ti.dag_id}
    Try number: {ti.try_number}

    Check the Airflow web UI for more details.
    """
    send_email(to=['akinmejiolalekan7@gmail.com'], subject=subject, html_content=body)


default_args = {
    'owner': 'CDE-Akinmeji_Olalekan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': custom_failure_alert
}

with DAG(
    dag_id="wikipedia_viewscount",
    default_args=default_args,
    start_date=datetime(2025, 10, 22),
    schedule="@daily"
):

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id="conn_id",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                domain_code VARCHAR(50) NOT NULL,
                page_title VARCHAR(50) NOT NULL,
                view_count INTEGER NOT NULL CHECK (view_count >= 0),
                target_date VARCHAR(20) NOT NULL,
                target_hour VARCHAR(20) NOT NULL,
                execution_date DATE NOT NULL
            );
        """,
        autocommit=True,
    )

    setup_env = PythonOperator(
        task_id="Environment_setup",
        python_callable=setup_environment
    )

    download_data = PythonOperator(
        task_id="download_data",
        python_callable=download_data
    )

    extract_gzip = PythonOperator(
        task_id="extracted_gzip_file",
        python_callable=extract_gzip_file
    )

    filter_csv = PythonOperator(
        task_id="filtered_csv",
        python_callable=filter_to_csv
    )

    load_data = PythonOperator(
        task_id="csv_to_postgres",
        python_callable=csv_to_database
    )

    cleanup = PythonOperator(
        task_id="environment_cleanup",
        python_callable=cleanup_files
    )

    create_table >> setup_env >> download_data >> extract_gzip >> filter_csv >> load_data >> cleanup