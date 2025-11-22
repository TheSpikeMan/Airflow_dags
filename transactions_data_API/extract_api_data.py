import requests
import logging
import pandas as pd
from urllib.parse import urljoin


def extract_api_data(url_address: str, sub_url_address: str, start: str, end: str):
    with requests.request(
            method='GET',
            url=urljoin(url_address, sub_url_address),
            params={
                'start_date': start,
                'end_date': end},
            timeout=10) as r:
        try:
            r.raise_for_status()
            logger.info(f"Connection_status: {r.status_code}")
            logger.info(f"Url: {r.url}")
            json_file = r.json()
        except requests.Timeout:
            logger.error("An error occurred: Timeout")
        except requests.JSONDecodeError:
            logger.error("An error occurred: JSON Decode Error")
        except requests.ConnectionError:
            logger.error("An error occurred: Connection Error")
    return json_file


def load_data(json: list[dict]):
    data = pd.DataFrame(json)
    return data


if __name__ == '__main__':
    """ Logger config """
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.WARNING,
        datefmt='%Y-%m-%d %H:%M:%S',
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    """ Defining variables """
    url = "http://127.0.0.1:8000/"
    sub_url = "transactions/"
    start_date = '2025-01-01'
    end_date = '2025-12-31'

    """ Launching function """
    json_data = extract_api_data(url, sub_url, start_date, end_date)

    """ Transforming data"""
    df = load_data(json_data)
    print(df.head())
