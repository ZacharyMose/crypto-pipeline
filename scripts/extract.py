import requests
import json
import boto3
import os
import logging
from datetime import datetime

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_and_upload():
    # 1. Configuration
    base_url = "https://api.coingecko.com/api/v3"
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    # Check if bucket is defined
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable is not set.")

    # 2. Fetch Data (Top 50 Coins)
    logger.info("Fetching data from CoinGecko API...")
    endpoint = f"{base_url}/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 50,
        'page': 1,
        'sparkline': 'false'
    }
    
    try:
        response = requests.get(endpoint, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} records.")
    except requests.exceptions.RequestException as e:
        logger.error(f"API Request failed: {e}")
        raise

    # 3. Generate File Name (Partitioned by Date)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_name = f"raw/crypto_markets/{date_str}/data.json"

    # 4. Upload to S3
    logger.info(f"Uploading to S3 bucket: {bucket_name}...")
    s3_client = boto3.client('s3')
    
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        logger.info(f"✅ Success! Data uploaded to s3://{bucket_name}/{file_name}")
    except Exception as e:
        logger.error(f"S3 Upload failed: {e}")
        raise

if __name__ == "__main__":
    fetch_and_upload()