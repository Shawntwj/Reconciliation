import pandas as pd
import logging
import os
import pytz
import sys
from datetime import datetime
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
# Load chunk size from .env or default to 1000
CHUNK_SIZE = int(os.getenv("INGEST_CHUNK_SIZE", 1000))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@db:5432/in_commodities")

# Initialize Engine globally for connection pooling
engine = create_engine(DATABASE_URL)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("IngestPipeline")

def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Handles timezone conversion and derived calculations."""
    aest = pytz.timezone('Australia/Sydney')
    
    def to_utc(d_str):
        try:
            dt = datetime.strptime(d_str, "%d/%m/%Y")
            return aest.localize(dt).astimezone(pytz.UTC).replace(tzinfo=None)
        except (ValueError, TypeError):
            return None

    # Vectorized validation (faster than iterrows)
    df['is_complete'] = ~(df['price'].isna() | df['quantity'].isna())
    
    # Apply transformations
    df['trade_date_utc'] = df['trade_date_aest'].apply(to_utc)
    df['total_value'] = df['price'] * df['quantity']
    
    return df

def load_chunk(df: pd.DataFrame):
    """Performs UPSERT operation on a single chunk of data."""
    upsert_sql = text("""
        INSERT INTO stg.clearing_trades (
            trade_number, fill_sequence, product, market, direction, 
            quantity, price, counterparty, fee, trade_date_aest, 
            trade_date_utc, is_complete, total_value
        ) VALUES (
            :trade_number, :fill_sequence, :product, :market, :direction, 
            :quantity, :price, :counterparty, :fee, TO_DATE(:trade_date_aest, 'DD/MM/YYYY'), 
            :trade_date_utc, :is_complete, :total_value
        )
        ON CONFLICT (trade_number, fill_sequence) 
        DO UPDATE SET
            price = EXCLUDED.price,
            quantity = EXCLUDED.quantity,
            total_value = EXCLUDED.total_value,
            is_complete = EXCLUDED.is_complete,
            updated_at = NOW();
    """)
    
    try:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                # Convert NaNs to None for Postgres compatibility
                params = row.where(pd.notnull(row), None).to_dict()
                conn.execute(upsert_sql, params)
    except Exception as e:
        logger.error(f"Database error during load: {e}")
        raise

def run_pipeline(file_path: str):
    """Main entry point: reads CSV in chunks and processes them."""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Starting ingest for {file_path} (Chunk Size: {CHUNK_SIZE})")
    
    try:
        # read_csv with chunksize returns an iterator
        reader = pd.read_csv(
            file_path, 
            sep=';', 
            decimal=',', 
            chunksize=CHUNK_SIZE,
            dtype={'trade_number': str, 'fill_sequence': int}
        )
        
        total_rows = 0
        for i, chunk in enumerate(reader):
            logger.info(f"Processing chunk {i+1}...")
            
            # Validate & Transform
            processed_chunk = transform_chunk(chunk)
            
            # Check for incomplete records in log
            incomplete_trades = processed_chunk[~processed_chunk['is_complete']]
            for _, row in incomplete_trades.iterrows():
                logger.warning(f"ALERT: Incomplete trade {row['trade_number']}-{row['fill_sequence']} missing price/qty")
            
            # Load
            load_chunk(processed_chunk)
            
            total_rows += len(chunk)
            
        logger.info(f"Ingestion complete. Total rows processed: {total_rows}")

    except Exception as e:
        logger.critical(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Example usage
    run_pipeline('trades.csv')