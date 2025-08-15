#!/usr/bin/env python3
"""
etl_connector.py
Generic Python ETL connector template.

Usage:
  - Create a .env file with MONGO_URI, MONGO_DB, API_KEY, etc.
  - pip install -r requirements.txt
  - python etl_connector.py
"""

import os
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

# Load environment variables
load_dotenv()

# Configuration (set in .env)
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.example.com")
API_ENDPOINT = os.getenv("API_ENDPOINT", "/v1/items")
API_KEY = os.getenv("API_KEY", "")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "1.0"))  # seconds between requests
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "etl_db")
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "example_connector")  # used for collection name
UNIQUE_ID_FIELD = os.getenv("UNIQUE_ID_FIELD", "id")  # field in payload used as unique key

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_mongo_client(uri: str) -> MongoClient:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Trigger a server selection to validate connection
        client.admin.command("ping")
        logger.info("Connected to MongoDB")
        return client
    except PyMongoError as e:
        logger.exception("Could not connect to MongoDB: %s", e)
        raise


def fetch_page(session: requests.Session, page: int) -> Optional[Dict[str, Any]]:
    """
    Fetch one page from the API and return parsed JSON or None on permanent failure.
    Adjust query params or pagination style to match the API.
    """
    url = f"{API_BASE_URL.rstrip('/')}{API_ENDPOINT}"
    params = {
        "page": page,
        "per_page": PAGE_SIZE,
        "api_key": API_KEY  # or use headers; depends on provider
    }
    headers = {
        "Accept": "application/json"
    }

    # Make request with basic retry/backoff
    max_retries = 3
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                # Rate limited: sleep and retry; respect Retry-After if present
                retry_after = resp.headers.get("Retry-After")
                sleep_for = float(retry_after) if retry_after else RATE_LIMIT_SLEEP * backoff
                logger.warning("Rate limited (429). Sleeping for %s seconds.", sleep_for)
                time.sleep(sleep_for)
            elif 500 <= resp.status_code < 600:
                logger.warning("Server error %s. Attempt %s/%s", resp.status_code, attempt, max_retries)
                time.sleep(RATE_LIMIT_SLEEP * backoff)
            else:
                logger.error("Request failed: %s %s", resp.status_code, resp.text[:500])
                return None
        except requests.RequestException as e:
            logger.warning("Request exception: %s. Attempt %s/%s", e, attempt, max_retries)
            time.sleep(RATE_LIMIT_SLEEP * backoff)
        backoff *= 2
    logger.error("Failed to fetch page %s after %s attempts", page, max_retries)
    return None


def transform_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform raw API record into Mongo-ready document.
    Minimal example:
      - ensure types are consistent
      - remove nested fields not required
      - compute ingestion metadata
    Customize as per your API schema.
    """
    # Example transformations. Replace with real mapping logic.
    doc = dict(rec)  # shallow copy
    # Normalise timestamp fields to ISODate strings
    if "timestamp" in doc:
        try:
            dt = datetime.fromisoformat(doc["timestamp"])
            doc["timestamp"] = dt.isoformat()
        except Exception:
            # keep original if parsing fails
            pass

    # Add ingestion metadata
    doc["_ingested_at"] = datetime.utcnow()
    doc["_source_connector"] = CONNECTOR_NAME
    return doc


def upsert_documents(collection, docs: List[Dict[str, Any]]) -> int:
    """
    Perform bulk upsert (idempotent loads). Returns number inserted/updated count.
    Uses UNIQUE_ID_FIELD to match existing docs.
    """
    if not docs:
        return 0
    operations = []
    for d in docs:
        if UNIQUE_ID_FIELD not in d:
            # skip or log; we expect a unique key
            logger.warning("Record missing unique id field '%s': %s", UNIQUE_ID_FIELD, d)
            continue
        filter_q = {UNIQUE_ID_FIELD: d[UNIQUE_ID_FIELD]}
        update_doc = {"$set": d}
        operations.append(UpdateOne(filter_q, update_doc, upsert=True))

    if not operations:
        return 0

    try:
        result = collection.bulk_write(operations, ordered=False)
        upserted = (result.upserted_count if hasattr(result, "upserted_count") else 0)
        modified = result.modified_count if hasattr(result, "modified_count") else 0
        logger.info("Bulk write result: upserted=%s, modified=%s", upserted, modified)
        return upserted + modified
    except PyMongoError as e:
        logger.exception("Bulk write failed: %s", e)
        return 0


def run_etl():
    # Initialize HTTP session and Mongo
    session = requests.Session()
    client = get_mongo_client(MONGO_URI)
    db = client[MONGO_DB]
    collection_name = f"{CONNECTOR_NAME}_raw"
    collection = db[collection_name]
    logger.info("Using collection: %s.%s", MONGO_DB, collection_name)

    page = 1
    total_processed = 0
    consecutive_empty_pages = 0
    max_empty_pages = 3  # stop if many empty pages in a row (depends on API)

    while True:
        payload = fetch_page(session, page)
        if payload is None:
            logger.error("Stopping ETL due to fetch failure for page %s", page)
            break

        # Adjust access path depending on response schema
        # e.g., {'data': [...], 'meta': {...}}
        records = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
        if isinstance(records, dict):
            # if API returns an object with items under a key
            # try common keys:
            for k in ("items", "results", "data"):
                if k in records:
                    records = records[k]
                    break

        if not records:
            logger.info("No records on page %s", page)
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_empty_pages:
                logger.info("Stopping: %s consecutive empty pages", consecutive_empty_pages)
                break
            page += 1
            continue

        consecutive_empty_pages = 0

        # Validate and transform
        transformed = []
        for rec in records:
            # Basic validation: ensure dict-like structure
            if not isinstance(rec, dict):
                logger.warning("Skipping record not a dict: %s", rec)
                continue
            transformed.append(transform_record(rec))

        # Load into MongoDB with upserts for idempotency
        processed = upsert_documents(collection, transformed)
        total_processed += processed
        logger.info("Page %s processed; %s docs upserted/modified. Total so far: %s", page, processed, total_processed)

        # Pagination break conditions - adapt to API: next link in payload, or page size < PAGE_SIZE
        # Example: if results less than page size, we've reached the end
        if isinstance(records, list) and len(records) < PAGE_SIZE:
            logger.info("Last page detected (len < PAGE_SIZE). Stopping.")
            break

        page += 1
        time.sleep(RATE_LIMIT_SLEEP)  # polite sleep to avoid hitting rate limits

    logger.info("ETL finished. Total documents processed: %s", total_processed)
    client.close()


if __name__ == "__main__":
    try:
        start = datetime.utcnow()
        logger.info("ETL run started at %s", start.isoformat())
        run_etl()
        logger.info("ETL run completed at %s", datetime.utcnow().isoformat())
    except Exception as e:
        logger.exception("Unhandled exception in ETL: %s", e)
        raise
