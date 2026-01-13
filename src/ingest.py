import pandas as pd
import logging
import sys
import os
import pytz
from datetime import datetime
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@db:5432/in_commodities")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IngestPipeline")

def read_and_validate(file_path: str) -> pd.DataFrame:
    logger.info(f"Reading {file_path}...")
    # [cite_start]Handle European decimal ',' and semicolon separator [cite: 1]
    df = pd.read_csv(file_path, sep=';', decimal=',')
    
    df['is_complete'] = True
    for idx, row in df.iterrows():
        if pd.isna(row['price']) or pd.isna(row['quantity']):
            df.at[idx, 'is_complete'] = False
            logger.warning(f"ALERT: Incomplete trade {row['trade_number']}-{row['fill_sequence']} missing price/qty")
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Converting Timezones (AEST -> UTC)...")
    aest = pytz.timezone('Australia/Sydney')
    
    def to_utc(d_str):
        dt = datetime.strptime(d_str, "%d/%m/%Y")
        return aest.localize(dt).astimezone(pytz.UTC).replace(tzinfo=None)

    df['trade_date_utc'] = df['trade_date_aest'].apply(to_utc)
    df['total_value'] = df['price'] * df['quantity']
    return df

def load(df: pd.DataFrame):
    logger.info("Loading to Postgres with UPSERT...")
    engine = create_engine(DATABASE_URL)
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
            updated_at = NOW(); -- Handles reruns
    """)
    
    with engine.connect() as conn:
        for _, row in df.iterrows():
            params = row.where(pd.notnull(row), None).to_dict()
            conn.execute(upsert_sql, params)
        conn.commit()
    logger.info(f"Successfully processed {len(df)} rows.")

if __name__ == "__main__":
    data = read_and_validate('trades.csv')
    transformed = transform(data)
    load(transformed)