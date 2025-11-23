import requests
import logging
import pandas as pd
from urllib.parse import urljoin
from json import JSONDecodeError



def extract_api_data(url_address: str, sub_url_address: str, start: str, end: str) -> list[dict] | None:
    json_file = None
    full_url = urljoin(url_address, sub_url_address)

    try:
        with requests.request(
                method='GET',
                url=full_url,
                params={
                    'start_date': start,
                    'end_date': end},
                timeout=10) as r:

            r.raise_for_status()
            logger.info(f"Connection_status: {r.status_code}")
            logger.info(f"Url: {r.url}")

            json_file = r.json()

            if json_file is None:
                logger.error("No data fetched")
                return None
    except requests.Timeout:
        logger.error("An error occurred: Timeout")
    except JSONDecodeError:
        logger.error("An error occurred: JSON Decode Error")
    except requests.ConnectionError:
        logger.error("An error occurred: Connection Error")
    except requests.HTTPError:
        logger.error("An error occurred: HTTP Error")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    return json_file


def load_data(json: list[dict]):
    if json is None:
        logger.error("No data loaded")
        return pd.DataFrame()
    return pd.DataFrame(json)


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
