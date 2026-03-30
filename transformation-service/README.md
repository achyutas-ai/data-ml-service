# Transformation Service (DataOps-Pipeline)

The **Transformation Service** is a dedicated microservice that handles data cleaning, PII masking, deduplication, and schema mapping between Snowflake (`SNAKE_CASE`) and Salesforce (`PascalCase__c`).

## Features

- **PII Masking**: Redacts emails/phone numbers unless `Authorized` is true.
- **Deduplication**: Using composite keys (e.g., `Email + External_ID`).
- **Schema Mapping**: Facilitates mapping from Snowflake standard formatting to Salesforce specific API names.
- **Logic Filter**: Filter records based on specific business rules (e.g., `Stage = 'Closed Won'` and `Amount > 5000`).

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the service:
   ```bash
   python app.py
   ```

## Configuration

The transformation logic is defined in `config/schema.json`.

## API: /transform (POST)

### Request Payload:
```json
{
  "data": [
    {
      "FIRST_NAME": "John",
      "LAST_NAME": "Doe",
      "EMAIL": "john@example.com",
      "OPPORTUNITY_STAGE": "Closed Won",
      "AMOUNT": 10000,
      "Authorized": false
    }
  ]
}
```

### Response Payload:
```json
{
  "status": "success",
  "transformed_records": 1,
  "data": [
    {
      "FirstName__c": "John",
      "LastName__c": "Doe",
      "Email__c": "***MASKED***",
      "StageName": "Closed Won",
      "Amount": 10000,
      "Authorized": false
    }
  ]
}
```
