# DataOps Ingestion Service

A centralized traffic controller for bidirectional data flow between Snowflake and Salesforce, designed for efficient DataOps pipelines.

## Features
- **Bidirectional Sync**: Supports both Salesforce-to-Snowflake and Snowflake-to-Salesforce data transfers.
- **Change Data Capture (CDC)**: Implements timestamp-based and stream-based CDC to minimize API usage.
- **Secure Authentication**: Uses OAuth 2.0 (JWT Bearer) for Salesforce and Key-Pair for Snowflake.
- **Scalable Architecture**: Flask-based REST API for managing and monitoring ingestion jobs.

## Prerequisites
- Python 3.9+
- Snowflake Account with SELECT/INSERT/UPDATE privileges.
- Salesforce Developer Edition or Enterprise Account.
- Private/Public Key-Pair for Snowflake.
- Connected App with JWT Bearer Flow configured in Salesforce.

## Setup & Installation
1. Clone the repository.
2. Navigate to the `dataOps-service` folder.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Configure environment variables in a `.env` file (see `.env.example`).

## Configuration
Create a `.env` file with the following variables:
```env
# Salesforce Credentials
SF_CLIENT_ID=your_client_id
SF_USERNAME=your_username
SF_PRIVATE_KEY_PATH=path/to/sf_private.pem

# Snowflake Credentials
SNOW_ACCOUNT=your_account_url
SNOW_USER=your_username
SNOW_PRIVATE_KEY_PATH=path/to/snow_private.pem
SNOW_DATABASE=your_database
SNOW_SCHEMA=your_schema
SNOW_WAREHOUSE=your_warehouse

# Flask Configuration
FLASK_APP=app.py
FLASK_ENV=development
```

## Usage
Start the Data-Ingestion Service:
```bash
python app.py
```

## API Endpoints
| Endpoint | Method | Description |
| --- | --- | --- |
| `/ingest/salesforce` | POST | Trigger Salesforce-to-Snowflake ingestion. |
| `/ingest/snowflake` | POST | Trigger Snowflake-to-Salesforce synchronization. |
| `/status` | GET | Check the status of current and past ingestion jobs. |
| `/health` | GET | Service health check. |
