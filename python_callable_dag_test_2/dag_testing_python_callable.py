from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
from custom_modules.data_walidator import data_validator
from custom_modules.data_router import data_router

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
            'record_count':50
        },
    )

    data_router = BranchPythonOperator(
        task_id='data_router',
        python_callable=data_router
    )

    read_validation_result = BashOperator(
        task_id='read_validation_result',
        bash_command="""
            echo "-----------------------------------"
            echo "Otrzymano wynik z poprzedniego zadania (XCom):"
            echo "{{ task_instance.xcom_pull(task_ids='validate_data_source') }}"
            echo "-----------------------------------"
            """
    )

    process_data_in_production = BashOperator(
        task_id='process_data_in_production',
        bash_command="""
            echo "-----------------------------------"
            echo "INFO: Uruchomiono produkcyjny proces ETL (>=100 rekordów)."
            """
    )

    send_low_record_warning = BashOperator(
        task_id='send_low_record_warning',
        bash_command="""
            echo "-----------------------------------"
            echo "WARNING: Wysłano alert - za mało danych (poniżej 100)."
            """
    )

    validate_data_source >> data_router
    data_router >> [process_data_in_production, send_low_record_warning]
    process_data_in_production >> read_validation_result
