# Configuration settings for the cron job project
import os

from dotenv import load_dotenv

load_dotenv()

# Environment settings
ENV = os.getenv("ENV", "prod")
PROJECT = "finance-cron-invoice_ninja_pipeline"

# Set PROJECT_ROOT to the parent directory of the current file's directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Invoice Ninja API settings
PROD_INVOICE_NINJA_API_URL = "https://invoicing.co"
DEV_INVOICE_NINJA_API_URL = "https://demo.invoiceninja.com"
INVOICE_NINJA_API_URL = (
    PROD_INVOICE_NINJA_API_URL if ENV == "prod" else DEV_INVOICE_NINJA_API_URL
)
INVOICE_NINJA_API_KEY = os.getenv("INVOICE_NINJA_API_KEY") if ENV == "prod" else "TOKEN"

# BigQuery settings
SERVICE_ACCOUNT_FILE_PATH = os.path.join(
    PROJECT_ROOT, os.getenv("SERVICE_ACCOUNT_FILE_PATH")
)
PROD_BIGQUERY_PROJECT = "mag-datawarehouse"
DEV_BIGQUERY_PROJECT = "dev-mag-datawarehouse"
BIGQUERY_PROJECT = PROD_BIGQUERY_PROJECT if ENV == "prod" else DEV_BIGQUERY_PROJECT
BIGQUERY_DATASET = "invoice_ninja"
BIGQUERY_TABLE_CLIENT = "client"
BIGQUERY_TABLE_CONTACT = "contact"
BIGQUERY_TABLE_INVOICE = "invoice"
BIGQUERY_TABLE_PAYMENT = "payment"
BIGQUERY_TABLE_CREDIT = "credit"
BIGQUERY_TABLE_COUNTRY = "country"

# cspell: ignore dotenv
