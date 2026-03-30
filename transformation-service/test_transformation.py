import json
import requests
import time
import subprocess
import os

def test_transformations():
    # 1. Start the Flask service
    print("Starting Transformation Service...")
    # Using a subprocess to start the service in background
    process = subprocess.Popen(["python", "app.py"], cwd="d:/codespace/data-ml-service/transformation-service")
    time.sleep(2) # Wait for it to start
    
    url = "http://127.0.0.1:5001/transform"
    
    # Sample data that hits all requirements:
    # 1. Logic Filter: Opportunity, Stage=Closed Won, Amount > 5000
    # 2. PII Masking: AUTHORIZED=False
    # 3. Deduplication: Same Email + External_ID
    # 4. Schema Mapping: SNAKE_CASE to PascalCase__c
    
    payload = {
        "data": [
            {
                "FIRST_NAME": "Alice",
                "LAST_NAME": "Smith",
                "EMAIL": "alice@company.com",
                "EXTERNAL_ID": "ID-001",
                "OPPORTUNITY_STAGE": "Closed Won",
                "AMOUNT": 10000,
                "Authorized": False # Should be masked
            },
            {
                "FIRST_NAME": "Alice", # DUPLICATE
                "LAST_NAME": "Smith",
                "EMAIL": "alice@company.com",
                "EXTERNAL_ID": "ID-001",
                "OPPORTUNITY_STAGE": "Closed Won",
                "AMOUNT": 10000,
                "Authorized": False 
            },
            {
                "FIRST_NAME": "Bob",
                "LAST_NAME": "Jones",
                "EMAIL": "bob@personal.com",
                "EXTERNAL_ID": "ID-002",
                "OPPORTUNITY_STAGE": "Prospecting", # Filtered Out: Incorrect Stage
                "AMOUNT": 20000,
                "Authorized": True
            },
            {
                "FIRST_NAME": "Charlie",
                "LAST_NAME": "Brown",
                "EMAIL": "charlie@hq.com",
                "EXTERNAL_ID": "ID-003",
                "OPPORTUNITY_STAGE": "Closed Won",
                "AMOUNT": 1000, # Filtered Out: Amount too low
                "Authorized": True
            },
            {
                "FIRST_NAME": "Diana",
                "LAST_NAME": "Prince",
                "EMAIL": "diana@themyscira.com",
                "EXTERNAL_ID": "ID-004",
                "OPPORTUNITY_STAGE": "Closed Won",
                "AMOUNT": 15000,
                "Authorized": True # Should NOT be masked
            }
        ]
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        
        print(f"Status: {data['status']}")
        print(f"Transformed Records: {data['transformed_records']}")
        
        # Validation checks
        transformed_data = data['data']
        
        # Expected:
        # 1. Alice (Masked, One record due to dedup)
        # 2. Diana (Not masked)
        # Total should be 2.
        
        if len(transformed_data) != 2:
            print(f"Error: Expected 2 records, got {len(transformed_data)}")
        else:
            print("Row count: OK")
            
        for record in transformed_data:
            if record['Email__c'] == "***MASKED***":
                print(f"Masked record: {record['FirstName__c']}")
            else:
                print(f"Authorized record: {record['FirstName__c']}")
                
        print("\nFull Result Sample:")
        print(json.dumps(transformed_data, indent=2))
        
    except Exception as e:
        print(f"Test Failed: {e}")
    finally:
        # Clean up: Kill the service
        process.terminate()

if __name__ == "__main__":
    test_transformations()
