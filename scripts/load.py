import boto3
import json
import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_to_postgres():
    # 2. Get Config
    bucket_name = os.getenv('S3_BUCKET_NAME')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT')

    # 3. Download from S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_key = f"raw/crypto_markets/{date_str}/data.json"

    logger.info(f"Downloading {file_key} from S3...")
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        data = json.loads(obj['Body'].read())
        logger.info(f"Downloaded {len(data)} records.")
    except Exception as e:
        logger.error(f"Failed to download from S3: {e}")
        raise

    # 4. Prepare Data for Insertion
    # We define specific columns we want to keep to ensure the table structure is clean
    records_to_insert = []
    current_time = datetime.now()

    for row in data:
        records_to_insert.append((
            row.get('id'),
            row.get('symbol'),
            row.get('name'),
            row.get('current_price'),
            row.get('market_cap'),
            row.get('total_volume'),
            row.get('high_24h'),
            row.get('low_24h'),
            row.get('last_updated'),
            current_time
        ))

    # 5. Connect to Postgres (Raw Driver)
    logger.info("Connecting to Postgres...")
    conn = None  # Initialize conn to None
    try:
        conn = psycopg2.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port,
            dbname=db_name
        )
        cur = conn.cursor()

        # 6. Create Table (DDL)
        # We drop and recreate it to ensure it matches our raw data exactly
        create_table_query = """
        DROP TABLE IF EXISTS raw_markets;
        CREATE TABLE raw_markets (
            id TEXT,
            symbol TEXT,
            name TEXT,
            current_price NUMERIC,
            market_cap NUMERIC,
            total_volume NUMERIC,
            high_24h NUMERIC,
            low_24h NUMERIC,
            last_updated TEXT,
            _loaded_at TIMESTAMP
        );
        """
        cur.execute(create_table_query)

        # 7. Bulk Insert
        insert_query = """
        INSERT INTO raw_markets 
        (id, symbol, name, current_price, market_cap, total_volume, high_24h, low_24h, last_updated, _loaded_at)
        VALUES %s
        """
        
        execute_values(cur, insert_query, records_to_insert)
        
        conn.commit()
        logger.info(f"Success! Loaded {len(records_to_insert)} rows into raw_markets.")

    except Exception as e:
        logger.error(f"Database Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    load_to_postgres()