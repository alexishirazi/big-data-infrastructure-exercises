from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def download_readsb_hist_data(day: str):
    # Implement logic to download 100 files for the given day
    pass

def prepare_readsb_hist_data(day: str):
    # Implement logic to prepare data (e.g., transforming or cleaning)
    pass

def upload_to_s3(day: str):
    # Implement logic to upload the prepared data to S3
    pass

def upload_to_postgresql(day: str):
    # Implement logic to upload the prepared data to PostgreSQL
    pass

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'download_readsb_hist_data',
    default_args=default_args,
    description='Download and prepare readsb-hist data',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 1),
    catchup=False,
)

download_task = PythonOperator(
    task_id='download_readsb_hist_data',
    python_callable=download_readsb_hist_data,
    op_args=['{{ ds }}'],  # Pass the execution date
    dag=dag,
)

prepare_task = PythonOperator(
    task_id='prepare_readsb_hist_data',
    python_callable=prepare_readsb_hist_data,
    op_args=['{{ ds }}'],
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    op_args=['{{ ds }}'],
    dag=dag,
)

upload_postgresql_task = PythonOperator(
    task_id='upload_to_postgresql',
    python_callable=upload_to_postgresql,
    op_args=['{{ ds }}'],
    dag=dag,
)

download_task >> prepare_task >> upload_task >> upload_postgresql_task
