from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# Importujemy funkcję, którą zdefiniowaliśmy w Kroku 1
from custom_modules.calculate_discount_rate import calculate_discount_rate 

with DAG(
    dag_id='python_operator_test',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['python', 'demo']
) as dag:
    
    # Definicja zadania (Task)
    calculate_task = PythonOperator(
        task_id='calculate_sales_discount',
        
        # 1. WSKAZANIE FUNKCJI
        python_callable=calculate_discount_rate,
        
        # 2. PRZEKAZANIE ARGUMENTÓW DO FUNKCJI
        op_kwargs={
            'sales_amount': 750.50
        } 
    )