import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .... import config
from ...utils.logger import app_logger


def _fetch_all_invoices(headers: dict) -> list:
    """
    Fetch all invoices from Invoice Ninja by iterating through paginated results.

    Args:
        headers (dict): HTTP headers to include in the requests.

    Returns:
        list: List of client dictionaries fetched from the API.
    """

    @retry(
        stop=stop_after_attempt(7), wait=wait_exponential(multiplier=1, min=2, max=60)
    )
    def _fetch_page(page: int, headers: dict) -> dict:
        app_logger.debug(f"Fetching page {page}")
        response = requests.get(
            f"{config.INVOICE_NINJA_API_URL}/api/v1/invoices?page={page}",
            headers=headers,
        )
        if response.status_code != 200:
            app_logger.error(
                f"Error fetching page {page}: {response.status_code} {response.text}"
            )
            response.raise_for_status()
        return response.json()

    all_invoices = []
    page = 1
    has_more_pages = True

    while has_more_pages:
        data = _fetch_page(page, headers)
        all_invoices.extend(data.get("data", []))
        pagination_links = data.get("meta", {}).get("pagination", {}).get("links", {})
        has_more_pages = pagination_links.get("next") is not None
        page += 1

    app_logger.info(f"Fetched {len(all_invoices)} invoices in total.")
    return all_invoices


def fetch_invoices() -> pd.DataFrame:
    """
    Fetch invoices and line_items from Invoice Ninja and return them as pandas DataFrames.

    Returns:
        pd.DataFrame: DataFrame of invoices.
    """
    app_logger.info("Fetching invoices and line_items from Invoice Ninja")
    headers = {
        "X-API-TOKEN": config.INVOICE_NINJA_API_KEY,
        "X-Requested-With": "XMLHttpRequest",
    }

    invoices_data = _fetch_all_invoices(headers)
    return pd.DataFrame(invoices_data)


# cspell: ignore dtypes iterrows
