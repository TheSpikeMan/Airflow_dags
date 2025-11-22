from fastapi import FastAPI, Query
from fastapi import HTTPException
from typing import Optional
import pendulum
import random

app = FastAPI()

""" Preparing data for API """
# Defining dates to generate random values
date_beginning = pendulum.date(2025, 1, 1)
date_end = pendulum.date(2025, 12, 31)

# Saving to list
current_date = date_beginning
dates_list = [current_date.add(days=i) for i in range(0, (date_end - date_beginning).days + 1, 1)]

# Finding random numbers
int_numbers_list = [int(round(random.random() * 100)) for i in range(0, 10000, 1)]

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
        start_date_parsed = pendulum.parse(start_date) if start_date else date_beginning
    except pendulum.parsing.exceptions.ParserError:
        raise HTTPException(status_code=400, detail="start_date has incorrect format, use YYYY-MM-DD")

    try:
        end_date_parsed = pendulum.parse(end_date) if end_date else date_end
    except pendulum.parsing.exceptions.ParserError:
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
