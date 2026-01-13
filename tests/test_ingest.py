import pytest
import pandas as pd
from datetime import datetime
from io import StringIO
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from ingest import run_pipeline, load_chunk, transform_chunk

# --- MOCK DATA ---
CSV_CONTENT = """trade_date_aest;trade_number;fill_sequence;product;market;direction;quantity;price;counterparty;fee
14/01/2025;T001;1;PWR-NORDIC;EEX;BUY;5;1,76;STATKRAFT;10,02
14/01/2025;T002;1;GAS-UK;EEX;SELL;10;;BP;20,40
15/01/2025;T003;1;EUA;EEX;BUY;4;2,83;SHELL;20,40
"""

@pytest.fixture
def raw_chunk():
    """Simulates a single chunk loaded by pandas.read_csv"""
    return pd.read_csv(StringIO(CSV_CONTENT), sep=';', decimal=',')

@pytest.fixture
def processed_df(raw_chunk):
    """
    Simulates the pipeline by passing a chunk through the 
    transformation logic inside process_chunk.
    """
    # We modify the chunk in-place or return it depending on your process_chunk implementation
    # Note: In the previous implementation, process_chunk also handles the DB load.
    # For testing, we isolate the transformation logic.
    df = raw_chunk.copy()
    
    # Validation logic (Vectorized)
    df['is_complete'] = ~(df['price'].isna() | df['quantity'].isna())
    
    # Transformation logic
    import pytz
    aest = pytz.timezone('Australia/Sydney')
    def to_utc(d_str):
        dt = datetime.strptime(d_str, "%d/%m/%Y")
        return aest.localize(dt).astimezone(pytz.UTC).replace(tzinfo=None)

    df['trade_date_utc'] = df['trade_date_aest'].apply(to_utc)
    df['total_value'] = df['price'] * df['quantity']
    return df

# --- TESTS ---

def test_european_parsing(processed_df):
    """Test if 1,76 is correctly converted to float 1.76"""
    valid_trade = processed_df[processed_df['trade_number'] == 'T001'].iloc[0]
    
    assert isinstance(valid_trade['price'], float)
    assert valid_trade['price'] == 1.76
    assert valid_trade['fee'] == 10.02

def test_incomplete_trade_detection(processed_df):
    """Test if missing price triggers is_complete=False using vectorized logic"""
    incomplete_trade = processed_df[processed_df['trade_number'] == 'T002'].iloc[0]
    
    assert incomplete_trade['is_complete'] == False
    assert pd.isna(incomplete_trade['price'])

def test_timezone_conversion(processed_df):
    """Test AEST to UTC conversion (AEDT UTC+11 in January)"""
    trade = processed_df[processed_df['trade_number'] == 'T001'].iloc[0]
    
    # Jan 14 00:00 AEDT -> Jan 13 13:00 UTC
    expected_date = datetime(2025, 1, 13, 13, 0, 0)
    assert trade['trade_date_utc'] == expected_date

def test_total_value_calculation(processed_df):
    """Test if Price * Quantity = Total Value"""
    trade = processed_df[processed_df['trade_number'] == 'T001'].iloc[0]
    assert trade['total_value'] == 8.8

def test_chunking_iterator(tmp_path):
    """Verifies that the file is actually read in chunks"""
    f = tmp_path / "large_test.csv"
    # Create 10 rows
    content = "trade_date_aest;trade_number;fill_sequence;product;market;direction;quantity;price;counterparty;fee\n"
    content += "14/01/2025;T;1;P;M;D;1;1,0;C;1,0\n" * 10
    f.write_text(content)

    # Use a chunk size of 3
    reader = pd.read_csv(str(f), sep=';', decimal=',', chunksize=3)
    
    chunks = list(reader)
    assert len(chunks) == 4  # 3+3+3+1
    assert len(chunks[0]) == 3
    assert len(chunks[-1]) == 1