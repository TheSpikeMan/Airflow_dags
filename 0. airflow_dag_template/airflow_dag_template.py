from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Ustawiamy start_date 
START_DATE = pendulum.datetime(2025, 10, 31, tz="UTC") 

# Przykładowy scheduling: Uruchamiaj raz dziennie o północy.
DAILY_SCHEDULE = "0 0 * * *" 

with DAG(
    # Nazwa DAGa
    dag_id="example",
    # Data początkowa dla DAGA
    start_date=START_DATE,
    # Czestotliwość odświeżania
    schedule=DAILY_SCHEDULE,
    # Uruchamianie zaległych przebiegów (True - tak, False - nie)
    catchup=False,
    # Lista tagów podpięta do DAG
    tags=[],
) as dag:
    
    # Zadanie nr 1 - 
    task1 = BashOperator(
        task_id="",
        bash_command="",
    )

    # Zadanie nr 2
    task2 = BashOperator(
        task_id="",
        bash_command="",
    )
    
    # Ustawienie potoku zadań
    task1 >> task2