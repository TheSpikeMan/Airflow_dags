from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 1. DEFINICJA START_DATE I HARMONOGRAMU
# Ustawiamy start_date na 2025-10-31, aby wywołać jeden zaległy przebieg
START_DATE = pendulum.datetime(2025, 10, 31, tz="UTC") 
DAILY_SCHEDULE = "0 0 * * *"  # Uruchamiaj raz dziennie o północy (00:00 UTC)

with DAG(
    dag_id="second_training_dag",
    start_date=START_DATE,
    schedule=DAILY_SCHEDULE,
    catchup=False, # Najważniejsze: Ignoruj stare zaległości.
    tags=["bash"],
) as dag:
    
    # Krok 1: EXTRACT (Pobieranie/Walidacja)
    # Tworzenie unikalnego folderu dla danych z danego interwału {{ ds_nodash }}
    # W zadaniu 'extract_data'
    extract = BashOperator(
        task_id="extract_data",
        bash_command="""
            echo "Tworzenie folderu wejściowego dla daty: {{ ds }}"
            # Użycie ~/data/raw/
            mkdir -p ~/data/raw/{{ ds_nodash }} 
            echo "Ścieżka dla tego przebiegu: ~/data/raw/{{ ds_nodash }}"
        """,
    )
    
    # W zadaniu 'transform_data'
    transform = BashOperator(
        task_id="transform_data",
        bash_command="""
            echo "Krok 2: TRANSFORM - Przetwarzam dane..."
            mkdir -p ~/data/temp/  # Tworzymy katalog nadrzędny w katalogu domowym
            touch ~/data/temp/processing_{{ ds_nodash }}.tmp
        """,
    )
    
    # W zadaniu 'load_and_cleanup'
    load = BashOperator(
        task_id="load_and_cleanup",
        bash_command="""
            echo "Usuwanie plików tymczasowych z poprzedniego interwału: {{ prev_ds_nodash }}"
            # Usuwanie z katalogu domowego
            rm -rf ~/data/temp/processing_{{ prev_ds_nodash }}.tmp 
        """,
    )
    
    # Definicja kolejności
    extract >> transform >> load