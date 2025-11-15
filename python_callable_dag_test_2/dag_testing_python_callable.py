from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from python_callable_dag_test_2 import data_validator

with DAG(
    dag_id='data_validator_pipeline',
    start_date=datetime(2025,11,15),
    schedule=None,
    catchup=False,
    tags=['python', 'validation']
) as dag:
    validate_data_source = PythonOperator(
        task_id='validate_data_source',
        python_callable=data_validator,
        op_kwargs={
            'record_count':250
        }
    )