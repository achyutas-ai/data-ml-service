import os
import sqlite3
import pandas as pd
import numpy as np
from flask import Flask, jsonify, request
from snowflake.connector import connect
from simple_salesforce import Salesforce
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

app = Flask(__name__)

# Mock database for watermark tracking (In production, use Snowflake or Postgres)
# Tracks last sync timestamp per object for CDC logic
def get_db_connection():
    conn = sqlite3.connect('watermarks.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    conn.execute('CREATE TABLE IF NOT EXISTS watermarks (object_name TEXT PRIMARY KEY, last_sync_timestamp TIMESTAMP)')
    conn.commit()
    conn.close()

# Initialize watermark DB
init_db()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "DataOps-Ingestion-Traffic-Controller"})

@app.route('/status', methods=['GET'])
def status():
    # Fetch status of past jobs from a real database in a real scenario
    return jsonify({"status": "All systems operational", "active_jobs": 0})

@app.route('/ingest/salesforce', methods=['POST'])
def ingest_salesforce():
    """
    Salesforce CDC Ingestion Logic:
    1. Fetch the last watermark (SystemModStamp) for the object.
    2. Query Salesforce for records modified > watermark.
    3. Push results to Snowflake Staging.
    4. Update watermark.
    """
    object_name = request.json.get('object_name', 'Account')
    
    # Logic explanation:
    # sf = Salesforce(username=SF_USER, password=SF_PWD, security_token=SF_TOKEN)
    # OR JWT Bearer Flow for Lead Data Architect's recommendation:
    # sf = Salesforce(instance_url=SF_INSTANCE, session_id=TOKEN)

    # Inbound CDC Implementation
    # last_watermark = get_watermark(object_name)
    # query = f"SELECT Id, Name, SystemModStamp FROM {object_name} WHERE SystemModStamp > {last_watermark}"
    # records = sf.query_all(query)
    
    return jsonify({
        "message": f"CDC Ingestion triggered for Salesforce object: {object_name}",
        "strategy": "SystemModStamp-based Incremental Inbound",
        "ingestion_id": "SF-INBOUND-001"
    })

@app.route('/ingest/snowflake', methods=['POST'])
def ingest_snowflake():
    """
    Snowflake CDC Inbound/Outbound Logic:
    1. Leverage Snowflake STREAMS on source tables.
    2. Fetch new/modified rows from the stream.
    3. Transform and sync back to Salesforce via Bulk API.
    """
    table_name = request.json.get('table_name', 'SF_STAGING_ACCOUNT')
    
    # Logic explanation:
    # conn = snowflake.connector.connect(..., private_key=pk_data)
    # cursor = conn.cursor()
    # cursor.execute(f"SELECT * FROM {table_name}_STREAM")
    # rows = cursor.fetchall()
    
    return jsonify({
        "message": f"CDC Synchronization triggered from Snowflake table: {table_name}",
        "strategy": "Snowflake-Stream-based Outbound",
        "sync_id": "SNOW-OUTBOUND-001"
    })

def get_private_key(key_path):
    """
    Helper function for Key-Pair Authentication for Snowflake
    """
    with open(key_path, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None, # Or pass password if key is encrypted
            backend=default_backend()
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
