import requests
import logging
import pandas as pd
from urllib.parse import urljoin
from json import JSONDecodeError

""" Defining Logging instance """
logger = logging.getLogger(__name__)


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


def transform_data(data: list[dict]) -> pd.DataFrame:
    if data is None:
        logger.error("No data loaded. Returning empty DataFrame.")
        return pd.DataFrame()
    data = pd.DataFrame(data)

    """ Launching functions """
    missing_data = validate_data_quality(data)
    
    valid_statuses = validate_statuses(data, status_expected)
    valid_products = validate_products(data)

    """ Transforming data """
    return data.loc[valid_statuses.index.intersection(valid_products.index)]


def validate_data_quality(df_to_validate: pd.DataFrame) -> pd.Series:
    """
    Validating number of None values in each column
    """
    number_of_nones = df_to_validate.isna().sum()
    number_of_rows = df_to_validate.shape[0]
    missing_data_per_column = round(100 * pd.Series.div(number_of_nones, number_of_rows), 2)

    return missing_data_per_column


def validate_statuses(df_to_validate: pd.DataFrame, status: list[dict]) -> list:
    """
    Validating statuses according to expected and returning restricted data
    Returning: indexes of DataFrame validated
    """

    status_ids_list = [list(dict_item.keys())[0] for dict_item in status]
    status_validated = df_to_validate['status_id'].isin(status_ids_list)
    return status_validated.index.to_list()


def validate_products(df_to_validate: pd.DataFrame) -> list:
    """
    Validating products which are not 'None'.
    Returning: indexes of DataFrame validated.
    """
    products_validated = df_to_validate['product_name'].notna()
    df_validated_by_products = df_to_validate[products_validated]
    return df_validated_by_products.index.to_list()


def load_data(df_to_load: pd.DataFrame) -> pd.DataFrame:
    if df_to_load is None:
        logger.error("No data loaded. Returning empty DataFrame.")
        return pd.DataFrame()
    return df_to_load


if __name__ == '__main__':
    """ Logger config """
    logging.basicConfig(
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    """ Defining variables """
    url = "http://127.0.0.1:8000/"
    sub_url = "transactions/"
    start_date = '2025-01-01'
    end_date = '2025-01-31'

    """ Defining expected values"""
    status_expected = [{2: 'in realization'}, {3: 'realized'}]

    """ Launching function """
    json_data = extract_api_data(url, sub_url, start_date, end_date)

    """ Transforming data"""
    df_transformed = transform_data(json_data)

    """ Loading data"""
    df_loaded = load_data(df_transformed)
