from fastapi import FastAPI, Query
from fastapi import HTTPException
from typing import Optional
import pendulum
from pendulum.parsing.exceptions import ParserError
import random

app = FastAPI()

""" Preparing data for API """
# Defining dates to generate random values
DATE_BEGINNING = pendulum.date(2025, 1, 1)
DATE_END = pendulum.date(2025, 12, 31)

# Saving to list
dates_list = [DATE_BEGINNING.add(days=i) for i in range(0, (DATE_END - DATE_BEGINNING).days + 1, 1)]

# Finding random numbers
int_numbers_list = [random.randint(0, 100) * 100 for i in range(10000)]

# Defining random products
products_list = ['laptop', 'mobile_phone', 'charger', 'lamp', 'table', 'TV', 'mouse', 'usb_c_cable', 'keyboard']

# Generating random transaction data
transactions = [{"transaction_id": random.choice(int_numbers_list),
                 "transaction_date": random.choice(dates_list),
                 "product_id": random.choice(int_numbers_list),
                 "product_name": random.choice(products_list)}
                for i in range(0, 1000, 1)]

""" API """


@app.get("/transactions/")
def get_transactions(
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD"),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD")
):
    # Parsing date
    try:
        start_date_parsed = pendulum.parse(start_date, exact=True) if start_date else DATE_BEGINNING
    except ParserError:
        raise HTTPException(status_code=400, detail="start_date has incorrect format, use YYYY-MM-DD")

    try:
        end_date_parsed = pendulum.parse(end_date, exact=True) if end_date else DATE_END
    except ParserError:
        raise HTTPException(status_code=400, detail="end_date has incorrect format, use YYYY-MM-DD")

    # Filtering data
    transactions_filtered = [
        t for t in transactions
        if start_date_parsed <= t["transaction_date"] <= end_date_parsed
    ]

    return transactions_filtered


@app.get("/")
def root():
    return {"Main page": 1}
