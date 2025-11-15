from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from custom_modules.data_walidator import data_validator

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
        },
    )

    read_validation_result = BashOperator(
        task_id = 'read_validation_result',
        bash_command = """
            echo "-----------------------------------"
            echo "Otrzymano wynik z poprzedniego zadania (XCom):"
            echo "{{ task_instance.xcom_pull(task_ids='validate_data_source') }}"
            echo "-----------------------------------"
            """
    )

    validate_data_source >> read_validation_result