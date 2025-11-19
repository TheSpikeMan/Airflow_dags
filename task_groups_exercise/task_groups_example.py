from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(
    dag_id='taskgroup_validation_example',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # Zadanie 1: Przygotowanie danych (Przed TaskGroup)
    load_data = BashOperator(
        task_id='1_load_raw_data',
        bash_command='echo "Loading raw data..."'
    )

    # Definicja Grupy Zadań (TaskGroup)
    with TaskGroup("2_data_validation") as validation_group:
        
        # 2a. Zadanie wewnątrz grupy
        check_schema = BashOperator(
            task_id='a_check_schema',
            bash_command='echo "Checking data schema..."'
        )

        # 2b. Zadanie wewnątrz grupy
        check_quality = BashOperator(
            task_id='b_check_quality',
            bash_command='echo "Checking data quality..." && exit 1'
        )
        
        # 2c. Zadanie wewnątrz grupy
        check_integrity = BashOperator(
            task_id='c_check_integrity',
            bash_command='echo "Checking data integrity..."'
        )

        # Wewnętrzna sekwencja w grupie
        check_schema >> check_quality >> check_integrity 
    
    # Zadanie 3: Przetwarzanie (Po TaskGroup)
    transform_data = BashOperator(
        task_id='3_transform_data',
        bash_command='echo "Validation finished. Starting transformation..."',
        trigger_rule='none_failed_min_one_success'
    )
    
    # Definicja Zależności Zewnętrznych
    # Wystarczy podłączyć zadanie do nazwy grupy!
    load_data >> validation_group >> transform_data