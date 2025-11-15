import logging
# Używamy loggera Airflow do wyświetlania komunikatów w logach zadań
log = logging.getLogger(__name__)

def calculate_discount_rate(**kwargs):
    """
    Symuluje skomplikowaną logikę biznesową. 
    Oblicza 10% zniżki na podstawie podanej kwoty.
    """
    
    # 1. Pobieranie Parametru z kontekstu DAG-
    amount = kwargs.get('sales_amount', 1000)
    
    # 2. Logika Biznesowa
    discount = amount * 0.10
    
    # 3. Logowanie Wyniku
    log.info(f"Otrzymana kwota sprzedaży: {amount}")
    log.info(f"Obliczona zniżka (10%): {discount}")
    
    # 4. Zwrot Wartości (Airflow automatycznie zapisze to do XCom)
    return discount