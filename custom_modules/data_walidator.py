import logging

def data_validator(**kwargs):

    # Konfiguracja logownia
    log = logging.getLogger(__name__)

    # Walidacja danych
    record_count = kwargs.get('record_count', 0)
    if record_count >= 100:
        log.info(f"Walidacja OK. Liczba rekordów: {record_count}.")
        return f"Sukces! Liczba rekordów to {record_count}"
    else:
        log.error(f"BŁĄD: Liczba rekordów {record_count} jest poniżej progu minimalnego 100.")
        raise ValueError
    
