import pytest
import pandas as pd
import io
from datetime import datetime
from src.ingest import read_and_validate, transform

# --- MOCK DATA ---
# We simulate a CSV with 3 rows:
# 1. Valid Trade
# 2. Incomplete Trade (Missing Price)
# 3. Trade with different date for Timezone check
CSV_CONTENT = """trade_date_aest;trade_number;fill_sequence;product;market;direction;quantity;price;counterparty;fee
14/01/2025;T001;1;PWR-NORDIC;EEX;BUY;5;1,76;STATKRAFT;10,02
14/01/2025;T002;1;GAS-UK;EEX;SELL;10;;BP;20,40
15/01/2025;T003;1;EUA;EEX;BUY;4;2,83;SHELL;20,40
"""

@pytest.fixture
def mock_csv_file(tmp_path):
    """Creates a temporary CSV file for testing"""
    f = tmp_path / "test_trades.csv"
    f.write_text(CSV_CONTENT)
    return str(f)

@pytest.fixture
def processed_df(mock_csv_file):
    """Runs the ingestion pipeline up to transformation"""
    df = read_and_validate(mock_csv_file)
    return transform(df)

# --- TESTS ---

def test_european_parsing(processed_df):
    """Test if 1,76 is correctly converted to float 1.76"""
    valid_trade = processed_df[processed_df['trade_number'] == 'T001'].iloc[0]
    
    assert isinstance(valid_trade['price'], float), "Price should be a float"
    assert valid_trade['price'] == 1.76, f"Expected 1.76, got {valid_trade['price']}"
    assert valid_trade['fee'] == 10.02, "Fee decimal parsing failed"

def test_incomplete_trade_detection(processed_df):
    """Test if missing price triggers is_complete=False"""
    incomplete_trade = processed_df[processed_df['trade_number'] == 'T002'].iloc[0]
    
    assert incomplete_trade['is_complete'] == False, "Row with missing price should be incomplete"
    
    # Verify the alert logic captured it
    # We can inspect the DataFrame directly since our logic modifies it
    assert pd.isna(incomplete_trade['price']), "Price should be NaN"

def test_timezone_conversion(processed_df):
    """Test AEST to UTC conversion"""
    # Case 1: 14/01/2025 (AEST) -> 13/01/2025 13:00:00 UTC (assuming UTC+10) day light savings
    # Note: Python's datetime comparison requires care with types
    trade = processed_df[processed_df['trade_number'] == 'T001'].iloc[0]

    # January is AEDT (UTC+11), so 00:00 local is 13:00 previous day UTC
    expected_date = datetime(2025, 1, 13, 13, 0, 0)
    assert trade['trade_date_utc'] == expected_date, \
        f"Timezone conversion failed. Expected {expected_date}, got {trade['trade_date_utc']}"

def test_total_value_calculation(processed_df):
    """Test if Price * Quantity = Total Value"""
    trade = processed_df[processed_df['trade_number'] == 'T001'].iloc[0]
    
    expected_value = 5 * 1.76  # 8.8
    assert trade['total_value'] == expected_value, "Total value calculation is wrong"

def test_alert_system_capture(caplog, mock_csv_file):
    """Test if the Alert System actually logs warnings"""
    # We need to re-run read_and_validate to capture logs
    read_and_validate(mock_csv_file)
    
    # Check if "INCOMPLETE TRADE" appeared in logs
    assert "ALERT: Incomplete trade" in caplog.text
    assert "T002-1" in caplog.text