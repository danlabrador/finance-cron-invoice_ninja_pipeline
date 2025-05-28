# Finance Cron - Invoice Ninja Pipeline

A Python-based ETL pipeline that synchronizes data from Invoice Ninja to BigQuery.

## Overview

This project provides a cron job that fetches data from Invoice Ninja's API and synchronizes it with BigQuery tables. It handles clients, contacts, invoices, payments, and credits data.

## Features

- Fetches data from Invoice Ninja API with pagination support
- Synchronizes data to BigQuery tables
- Handles data type conversions and schema updates
- Provides detailed logging
- Implements retry mechanisms for API calls and BigQuery operations

## Project Structure

```
finance-cron-invoice_ninja_pipeline/
├── job_daily/
│   ├── main.py                  # Main entry point for the cron job
│   ├── config.py                # Configuration settings
│   └── services/
│       ├── integrations/
│       │   ├── bigquery/        # BigQuery integration
│       │   │   ├── get.py       # Functions to query BigQuery
│       │   │   └── sync_with_df.py  # Functions to sync DataFrames with BigQuery
│       │   └── invoice_ninja/   # Invoice Ninja integration
│       │       ├── clients.py   # Client and contact data fetching
│       │       ├── invoices.py  # Invoice data fetching
│       │       ├── payments.py  # Payment data fetching
│       │       └── credits.py   # Credit data fetching
│       └── utils/
│           ├── logger.py        # Logging utilities
│           ├── throttler.py     # API rate limiting
│           └── iso_countries.py # Country code utilities
├── requirements.txt             # Python dependencies
└── .env                        # Environment variables (not in repo)
```

## Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Create a `.env` file with the following variables:
   ```
   INVOICE_NINJA_API_KEY=your_api_key
   SERVICE_ACCOUNT_FILE_PATH=path_to_service_account_file
   ```
4. Place your Google Cloud service account JSON file in the project directory

## Usage

Run the main script to synchronize all data:

```bash
python -m job_daily
```

## Configuration

The project uses environment variables and a configuration file (`config.py`) to manage settings:

- `INVOICE_NINJA_API_KEY`: API key for Invoice Ninja
- `SERVICE_ACCOUNT_FILE_PATH`: Path to Google Cloud service account JSON file
- `BIGQUERY_PROJECT`: BigQuery project ID
- `BIGQUERY_DATASET`: BigQuery dataset name
- `BIGQUERY_TABLE_*`: BigQuery table names for different entities

## Data Flow

1. Fetch data from Invoice Ninja API
2. Process and transform the data
3. Synchronize with BigQuery tables:
   - Insert new records
   - Update existing records
   - Update table schema if needed

## Error Handling

The project implements retry mechanisms for API calls and BigQuery operations to handle transient failures. Detailed logging is provided for troubleshooting.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a new branch for your feature:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes and commit them:
   ```bash
   git commit -m "Add your commit message"
   ```
4. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```
5. Open a Pull Request

Please ensure your code follows the existing style and includes appropriate tests and documentation.

### License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
