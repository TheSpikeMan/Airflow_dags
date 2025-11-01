from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Ustawiamy start_date 
START_DATE = pendulum.datetime(2025, 11, 1, tz="UTC") 

# Przykładowy scheduling: Uruchamiaj raz dziennie o północy.
DAILY_SCHEDULE = "0 21 * * 1-5" 

with DAG(
    # Nazwa DAGa
    dag_id="daily_jobs_dag",
    # Data początkowa dla DAGA
    start_date=START_DATE,
    # Czestotliwość odświeżania
    schedule=DAILY_SCHEDULE,
    # Uruchamianie zaległych przebiegów (True - tak, False - nie)
    catchup=False,
    # Lista tagów podpięta do DAG
    tags=['gcp', 'jobs'],
) as dag:
    
    # Zadanie nr 1 - 
    task1 = BashOperator(
        task_id="Job_initialization",
        bash_command="echo 'Uruchamiam inicjalizację joba...'",
    )

    # Zadanie nr 2
    task2 = BashOperator(
        task_id="Job_Execution",
        bash_command="echo 'Przetwarzam joba...'",
    )

    # Zadanie nr 2
    task3 = BashOperator(
        task_id="Job_Finishing",
        bash_command="echo 'Zakończenie przetwarzania.'",
    )
    
    # Definiuję kolejność wykonywania zadań
    task1 >> task2 >> task3