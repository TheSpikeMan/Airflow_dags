from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Ustawiamy start_date 
START_DATE = pendulum.datetime(2025, 10, 29, tz="UTC") 

# Przykładowy scheduling: Uruchamiaj raz dziennie o północy.
DAILY_SCHEDULE = "10 0 * * *" 

with DAG (
    # Nazwa DAGa
    dag_id="practice_bash",
    # Data początkowa dla DAGA
    start_date=START_DATE,
    # Czestotliwość odświeżania
    schedule=DAILY_SCHEDULE,
    # Uruchamianie zaległych przebiegów = True
    catchup=True,
    # Lista tagów podpięta do DAG
    tags=['training'],
    # Wprowadzam zabezpieczenie na ilość uruchamianych jednocześnie DAGów
    max_active_runs=1
) as dag:
    
    # Zadanie nr 1 - 
    extract = BashOperator(
        task_id="ExtractData",
        bash_command="""
            echo 'DAG Execution interval start: {{ds}}'
            echo 'Starting data extraction.'
            mkdir -p ~/data/practice_bash/2025/{{ds_nodash}}
            touch ~/data/practice_bash/2025/{{ds_nodash}}/daily_snap.txt
        """,
    )

    # Zadanie nr 2
    transform = BashOperator(
        task_id="TransformDate",
        bash_command="""
            echo 'Starting data transformation.'
            touch ~/data/practice_bash/2025/{{ds_nodash}}/daily_transformed.txt
        """,
    )
    
    # Ustawienie potoku zadań
    extract >> transform