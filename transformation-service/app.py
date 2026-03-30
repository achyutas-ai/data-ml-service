import os
import json
import pandas as pd
from flask import Flask, jsonify, request

app = Flask(__name__)

# Load transformation schema
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'config', 'schema.json')

def load_schema():
    with open(SCHEMA_PATH, 'r') as f:
        return json.load(f)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "Transformation-Service"})

@app.route('/transform', methods=['POST'])
def transform():
    """
    Main transformation endpoint.
    Accepts a list of JSON records and applies the transformation schema.
    """
    data = request.json.get('data', [])
    if not data:
        return jsonify({"error": "No data provided"}), 400

    df = pd.DataFrame(data)
    schema = load_schema()
    
    transformations = schema.get('transformations', [])
    
    # 1. Logic Filter (Pre-mapping)
    logic_filter_config = next((t for t in transformations if t['type'] == 'filter'), None)
    if logic_filter_config:
        rules = logic_filter_config.get('rules', [])
        for rule in rules:
            field = rule['field']
            op = rule['operator']
            val = rule['value']
            
            if field in df.columns:
                if op == '==':
                    df = df[df[field] == val]
                elif op == '>':
                    df = df[df[field] > val]
                elif op == '<':
                    df = df[df[field] < val]

    # 2. Deduplication
    dedup_config = next((t for t in transformations if t['type'] == 'deduplication'), None)
    if dedup_config:
        composite_key = dedup_config.get('composite_key', [])
        if all(key in df.columns for key in composite_key):
            df = df.drop_duplicates(subset=composite_key)

    # 3. PII Masking
    masking_config = next((t for t in transformations if t['type'] == 'masking'), None)
    if masking_config:
        fields = masking_config.get('fields', [])
        auth_flag = masking_config.get('authorized_flag', 'Authorized')
        mask_val = masking_config.get('mask_value', '***MASKED***')
        
        for field in fields:
            if field in df.columns:
                # Mask if Authorized is False or Column doesn't exist
                if auth_flag in df.columns:
                    df[field] = df.apply(lambda row: mask_val if not row[auth_flag] else row[field], axis=1)
                else:
                    df[field] = mask_val

    # 4. Schema Mapping
    mapping_config = next((t for t in transformations if t['type'] == 'mapping'), None)
    if mapping_config:
        mappings = mapping_config.get('mappings', {})
        # Rename columns based on mapping
        df = df.rename(columns=mappings)
        
    # Return transformed data
    result = df.to_dict(orient='records')
    
    return jsonify({
        "status": "success",
        "transformed_records": len(result),
        "data": result
    })

if __name__ == '__main__':
    # Default to 5001 to avoid conflict with dataOps-service on 5000
    app.run(host='0.0.0.0', port=5001, debug=True)
