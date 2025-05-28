import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .... import config
from ...utils.logger import app_logger


def _fetch_all_credits(headers: dict) -> list:
    """
    Fetch all credits from Invoice Ninja by iterating through paginated results.

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
            f"{config.INVOICE_NINJA_API_URL}/api/v1/credits?page={page}",
            headers=headers,
        )
        if response.status_code != 200:
            app_logger.error(
                f"Error fetching page {page}: {response.status_code} {response.text}"
            )
            response.raise_for_status()
        return response.json()

    all_credits = []
    page = 1
    has_more_pages = True

    while has_more_pages:
        data = _fetch_page(page, headers)
        all_credits.extend(data.get("data", []))
        pagination_links = data.get("meta", {}).get("pagination", {}).get("links", {})
        has_more_pages = pagination_links.get("next") is not None
        page += 1

    app_logger.info(f"Fetched {len(all_credits)} credits in total.")
    return all_credits


def fetch_credits() -> pd.DataFrame:
    """
    Fetch credits from Invoice Ninja and return them as pandas DataFrames.

    Returns:
        pd.DataFrame: DataFrame of credits.
    """
    app_logger.info("Fetching credits from Invoice Ninja")
    headers = {
        "X-API-TOKEN": config.INVOICE_NINJA_API_KEY,
        "X-Requested-With": "XMLHttpRequest",
    }

    credits_data = _fetch_all_credits(headers)
    credits_df = pd.DataFrame(credits_data)
    return credits_df


# cspell: ignore dtypes iterrows
