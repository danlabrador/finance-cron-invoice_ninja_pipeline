import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .... import config
from ...utils.logger import app_logger


def _fetch_all_clients(headers: dict) -> list:
    """
    Fetch all clients from Invoice Ninja by iterating through paginated results.

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
            f"{config.INVOICE_NINJA_API_URL}/api/v1/clients?page={page}",
            headers=headers,
        )
        if response.status_code != 200:
            app_logger.error(
                f"Error fetching page {page}: {response.status_code} {response.text}"
            )
            response.raise_for_status()
        return response.json()

    all_clients = []
    page = 1
    has_more_pages = True

    while has_more_pages:
        data = _fetch_page(page, headers)
        all_clients.extend(data.get("data", []))
        pagination_links = data.get("meta", {}).get("pagination", {}).get("links", {})
        has_more_pages = pagination_links.get("next") is not None
        page += 1

    app_logger.info(f"Fetched {len(all_clients)} clients in total.")
    return all_clients


def _extract_clients_and_contacts(clients: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process raw client data to separate clients and their contacts.

    Args:
        clients (list): List of client dictionaries as returned from the API.

    Returns:
        tuple: A tuple containing:
            - clients_df (pd.DataFrame): DataFrame of clients without contacts.
            - contacts_df (pd.DataFrame): DataFrame of contacts with associated client_id.
    """
    clients_df = pd.DataFrame(clients)
    contacts_list = []

    for _, row in clients_df.iterrows():
        client_id = row.get("id")
        contacts = row.get("contacts", [])
        if contacts:
            for contact in contacts:
                contact["client_id"] = client_id
                contacts_list.append(contact)

    contacts_df = pd.DataFrame(contacts_list)

    if "contacts" in clients_df.columns:
        clients_df.drop(columns=["contacts"], inplace=True)

    return clients_df, contacts_df


def fetch_clients_and_contacts() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch clients and contacts from Invoice Ninja and return them as pandas DataFrames.

    Returns:
        tuple: A tuple containing:
            - clients_df (pd.DataFrame): DataFrame of clients.
            - contacts_df (pd.DataFrame): DataFrame of contacts.
    """
    app_logger.info("Fetching clients and contacts from Invoice Ninja")
    headers = {
        "X-API-TOKEN": config.INVOICE_NINJA_API_KEY,
        "X-Requested-With": "XMLHttpRequest",
    }

    clients_data = _fetch_all_clients(headers)
    return _extract_clients_and_contacts(clients_data)


# cspell: ignore dtypes iterrows
