import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .... import config
from ...utils.logger import app_logger

# Payment Type ID to Payment Type Name mapping
PAYMENT_TYPE_MAP = {
    "1": "Bank Transfer",
    "4": "ACH",
    "5": "Visa Card",
    "6": "MasterCard",
    "7": "American Express",
    "8": "Discover Card",
    "12": "Credit Card Other",
    "13": "PayPal",
    "15": "Check",
    "25": "Venmo",
    "32": "Credit",
    "33": "Zelle",
}

# Status ID to Status Name mapping
STATUS_MAP = {
    "1": "Pending",
    "2": "Deleted",
    "4": "Completed",
    "5": "Partially Refunded",
    "6": "Refunded",
}


def _fetch_all_payments(headers: dict) -> list:
    """
    Fetch all payments from Invoice Ninja by iterating through paginated results.

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
            f"{config.INVOICE_NINJA_API_URL}/api/v1/payments?page={page}",
            headers=headers,
        )
        if response.status_code != 200:
            app_logger.error(
                f"Error fetching page {page}: {response.status_code} {response.text}"
            )
            response.raise_for_status()
        return response.json()

    all_payments = []
    page = 1
    has_more_pages = True

    while has_more_pages:
        data = _fetch_page(page, headers)
        all_payments.extend(data.get("data", []))
        pagination_links = data.get("meta", {}).get("pagination", {}).get("links", {})
        has_more_pages = pagination_links.get("next") is not None
        page += 1

    app_logger.info(f"Fetched {len(all_payments)} payments in total.")
    return all_payments


def _rearrange_columns(payments_df: pd.DataFrame) -> pd.DataFrame:
    """
    Rearrange columns in the DataFrame to a more readable format.
    This function moves the 'payment_type_name' and 'status_name' columns
    to be next to their corresponding 'payment_type_id' and 'status_id' columns.

    Args:
        payments_df (pd.DataFrame): DataFrame of payments.
    Returns:
        pd.DataFrame: DataFrame with rearranged columns.
    """
    cols = list(payments_df.columns)
    if "payment_type_id" in cols and "payment_type_name" in cols:
        cols.remove("payment_type_name")
        idx = cols.index("payment_type_id")
        cols.insert(idx + 1, "payment_type_name")

    if "status_id" in cols and "status_name" in cols:
        cols.remove("status_name")
        idx = cols.index("status_id")
        cols.insert(idx + 1, "status_name")

    payments_df = payments_df[cols]

    return payments_df


def fetch_payments() -> pd.DataFrame:
    """
    Fetch payments from Invoice Ninja and return them as pandas DataFrames.

    Returns:
        pd.DataFrame: DataFrame of payments.
    """
    app_logger.info("Fetching payments from Invoice Ninja")
    headers = {
        "X-API-TOKEN": config.INVOICE_NINJA_API_KEY,
        "X-Requested-With": "XMLHttpRequest",
    }

    payments_data = _fetch_all_payments(headers)
    payments_df = pd.DataFrame(payments_data)
    payments_df.rename(columns={"type_id": "payment_type_id"}, inplace=True)

    payments_df["payment_type_name"] = payments_df["payment_type_id"].map(
        PAYMENT_TYPE_MAP
    )
    payments_df["status_name"] = payments_df["status_id"].map(STATUS_MAP)
    payments_df = _rearrange_columns(payments_df)
    return payments_df


# cspell: ignore dtypes iterrows
