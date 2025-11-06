from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Ustawiamy start_date 
START_DATE = pendulum.datetime(2025, 11, 6, tz="UTC") 

with DAG(
    # Nazwa DAGa
    dag_id="task_time_logger_dag",
    # Data początkowa dla DAGA
    start_date=START_DATE,
    # Czestotliwość odświeżania
    schedule=None,
    # Uruchamianie zaległych przebiegów (True - tak, False - nie)
    catchup=False,
    # Lista tagów podpięta do DAG
    tags=["bash", "variables"],
) as dag:
    
    # Zadanie nr 1 - 
    log_current_time = BashOperator(
        task_id="log_current_time",
        bash_command="echo 'Aktualny czas serwera: '; date -Iseconds",
    )

    # Zadanie nr 2
    log_dag_run_id = BashOperator(
        task_id="log_dag_run_id",
        bash_command="echo 'ID uruchomienia DAG (DAG_RUN_ID): ' {{dag_run.run_id}}",
    )
    
    # Ustawienie potoku zadań
    log_current_time >> log_dag_run_id