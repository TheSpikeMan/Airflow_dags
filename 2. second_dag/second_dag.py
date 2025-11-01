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
    extract = BashOperator(
        task_id="extract_data",
        bash_command="""
            echo "Krok 1: EXTRACT - Tworzenie folderu wejściowego dla daty: {{ ds }}"
            mkdir -p /data/raw/{{ ds_nodash }}
            echo "Ścieżka dla tego przebiegu: /data/raw/{{ ds_nodash }}"
        """,
    )

    # Krok 2: TRANSFORM (Przetwarzanie Danych)
    # Symulacja przetwarzania danych dla konkretnego interwału
    transform = BashOperator(
        task_id="transform_data",
        bash_command="""
            echo "Krok 2: TRANSFORM - Przetwarzam dane z /data/raw/{{ ds_nodash }}"
            echo "Tworzenie tymczasowego pliku przetwarzania..."
            touch /data/temp/processing_{{ ds_nodash }}.tmp
        """,
    )
    
    # Krok 3: LOAD (Ładowanie i Czyszczenie)
    # Użycie {{ prev_ds_nodash }} do usunięcia plików TYMCZASOWYCH z poprzedniego dnia.
    load = BashOperator(
        task_id="load_and_cleanup",
        bash_command="""
            echo "Krok 3: LOAD - Zapis do Data Warehouse i czyszczenie."
            echo "Usuwanie plików tymczasowych z poprzedniego interwału: {{ prev_ds_nodash }}"
            rm -rf /data/temp/{{ prev_ds_nodash }}.tmp 
            echo "Zakończono ładowanie dla Execution Date: {{ ds }}"
        """,
    )
    
    # Definicja kolejności
    extract >> transform >> load