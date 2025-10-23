import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
# Corrected PyMongo Import
from pymongo.errors import ConnectionFailure as PyMongoConnectionError 

# --- 1. Setting Up the Connector Environment ---

# Load environment variables from .env file
load_dotenv()

# API Configuration (GreyNoise Community API)
GREYNOISE_BASE_URL = "https://api.greynoise.io/v3/community/"

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# List of IPs to check - For demonstration purposes
IPS_TO_CHECK = [
    "1.2.3.4",         
    "8.8.8.8",         
    "192.0.2.1",       
    "195.72.230.190"   
]

# ----------------------------------------------------------------------
## ETL Functions
# ----------------------------------------------------------------------

def extract_data(ip: str) -> dict:
    """
    E: Connects to the GreyNoise Community API WITHOUT an API key (unauthenticated).
    Note: This is subject to very low rate limits (e.g., 50 searches per week).
    """
    print(f"-> Extracting data for IP: {ip}...")
    
    # Headers only include Accept type - NO 'key' HEADER IS USED
    headers = {
        "Accept": "application/json",
    }

    url = f"{GREYNOISE_BASE_URL}{ip}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        # Iterative Testing & Validation: Handle non-200 responses
        if response.status_code == 429:
            print(f"   [RATE LIMIT ERROR] Rate limit hit for IP {ip}. You have exceeded the unauthenticated limit.")
            return {"error": "Rate Limit Exceeded (Unauthenticated)", "ip": ip, "status_code": 429}
        
        if response.status_code == 404:
            print(f"   [NOT FOUND] IP {ip} not found in GreyNoise Community data.")
            return {"error": "IP Not Found", "ip": ip, "status_code": 404}

        if response.status_code != 200:
            print(f"   [API ERROR] Failed for IP {ip} with status code {response.status_code}.")
            return {"error": f"API Error: Status {response.status_code}", "ip": ip, "status_code": response.status_code}

        # Successful extraction
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"   [CONNECTIVITY ERROR] Connection error for IP {ip}: {e}")
        return {"error": f"Connectivity Error: {e}", "ip": ip, "status_code": 0}

def transform_data(raw_record: dict) -> dict | None:
    """
    T: Cleans and reformats the data for MongoDB compatibility, adding audit fields.
    """
    ingestion_time = datetime.utcnow()
    
    if 'error' in raw_record:
        # If extraction failed, return a minimal record for audit
        return {
            "ip": raw_record.get('ip', 'N/A'),
            "ingestion_timestamp": ingestion_time,
            "status": "Extraction Failed",
            "error_details": raw_record.get('error'),
            "status_code": raw_record.get('status_code', 0)
        }
    
    # Transformation: Add the audit field to the successful record
    transformed = {
        **raw_record, 
        "ingestion_timestamp": ingestion_time
    }
    
    if 'ip' not in transformed:
        print(f"   [TRANSFORM WARNING] Record missing 'ip' field. Skipping or marking for audit.")
        return None 

    return transformed

def load_data(data_to_load: list[dict], mongo_uri: str, db_name: str, collection_name: str):
    """
    L: Stores the transformed data into the specified MongoDB collection.
    """
    if not data_to_load:
        print("-> No valid data to load. Exiting load phase.")
        return

    print(f"\n-> Attempting to connect to MongoDB...")
    client = None
    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        print("   Connection to MongoDB successful.")

        db = client[db_name]
        collection = db[collection_name]

        insert_result = collection.insert_many(data_to_load, ordered=False)
        
        print(f"   Successfully loaded {len(insert_result.inserted_ids)} records into '{collection_name}'.")

    except PyMongoConnectionError as e:
        print(f"   [MONGO ERROR] MongoDB Connection Error: {e}")
        print("   Data not loaded due to connection failure. Check MONGO_URI and IP whitelist.")
    except Exception as e:
        print(f"   [MONGO ERROR] An unexpected error occurred during data loading: {e}")
    finally:
        if client:
            client.close()
            print("   MongoDB connection closed.")


def run_etl_pipeline():
    """
    Orchestrates the complete ETL pipeline.
    """
    print("--- Starting GreyNoise Community ETL Pipeline (Unauthenticated) ---")
    
    transformed_records = []
    
    for ip in IPS_TO_CHECK:
        # 1. Extract
        raw_data = extract_data(ip)
        
        # 2. Transform
        transformed_record = transform_data(raw_data)
        
        if transformed_record:
            transformed_records.append(transformed_record)

    # 3. Load
    load_data(transformed_records, MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME)
    
    print("\n--- ETL Pipeline Finished ---")


if __name__ == "__main__":
    if not all([MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME]):
        print("FATAL ERROR: Missing one or more critical MongoDB environment variables in .env.")
    else:
        run_etl_pipeline()