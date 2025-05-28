from . import config
from .services.integrations import bigquery
from .services.integrations import invoice_ninja as ninja
from .services.utils import iso_countries


def main():
    # Sync clients and contacts data
    ninja_clients_df, ninja_contacts_df = ninja.clients.fetch_clients_and_contacts()
    bigquery.sync_with_df(
        ninja_clients_df,
        reference_id="id",
        project=config.BIGQUERY_PROJECT,
        dataset=config.BIGQUERY_DATASET,
        table=config.BIGQUERY_TABLE_CLIENT,
    )
    bigquery.sync_with_df(
        ninja_contacts_df,
        reference_id="id",
        project=config.BIGQUERY_PROJECT,
        dataset=config.BIGQUERY_DATASET,
        table=config.BIGQUERY_TABLE_CONTACT,
    )

    # Sync invoices data
    ninja_invoices_df = ninja.invoices.fetch_invoices()
    bigquery.sync_with_df(
        ninja_invoices_df,
        reference_id="id",
        project=config.BIGQUERY_PROJECT,
        dataset=config.BIGQUERY_DATASET,
        table=config.BIGQUERY_TABLE_INVOICE,
    )

    # Sync payments data
    ninja_payments_df = ninja.payments.fetch_payments()
    bigquery.sync_with_df(
        ninja_payments_df,
        reference_id="id",
        project=config.BIGQUERY_PROJECT,
        dataset=config.BIGQUERY_DATASET,
        table=config.BIGQUERY_TABLE_PAYMENT,
    )

    # Sync credits data
    ninja_credits_df = ninja.credits.fetch_credits()
    bigquery.sync_with_df(
        ninja_credits_df,
        reference_id="id",
        project=config.BIGQUERY_PROJECT,
        dataset=config.BIGQUERY_DATASET,
        table=config.BIGQUERY_TABLE_CREDIT,
    )


if __name__ == "__main__":
    # Sync clients and contacts data
    main()
